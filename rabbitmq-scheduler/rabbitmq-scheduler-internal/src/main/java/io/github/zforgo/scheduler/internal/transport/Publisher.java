package io.github.zforgo.scheduler.internal.transport;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Return;

import io.quarkus.logging.Log;

import io.github.zforgo.scheduler.SchedulerException;

public final class Publisher {

    private static final String KEY_PUBLISH_ID = "scheduler-publish-id";
    private static final Duration LOCK_TIMEOUT = Duration.ofSeconds(1);
    private final Channel channel;

    private final ReentrantLock emitLock = new ReentrantLock();
    private final BooleanSupplier isBlocked;
    private volatile boolean closing;

    private final ConcurrentNavigableMap<Long, Pending> pending = new ConcurrentSkipListMap<>();

    private final Map<String, Long> seqByPublishId = new ConcurrentHashMap<>();
    private final Map<Long, UnroutableMessageException> returned = new ConcurrentHashMap<>();

    private record Pending(String publishId, CompletableFuture<Void> result) {}

    public Publisher(Channel channel, BooleanSupplier isBlocked) {
        try {
            channel.confirmSelect();
            this.channel = channel;
            this.channel.addConfirmListener(
                    (tag, multiple) -> onConfirm(tag, multiple, null),
                    (tag, multiple) -> onConfirm(tag, multiple, new SchedulerException("Nacked publishing: " + tag))
            );
            this.isBlocked = isBlocked;
            this.channel.addReturnListener(this::onReturn);
            this.channel.addShutdownListener(this::failPending);
        } catch (IOException e) {
            throw new SchedulerException(e);
        }
    }

    private void failPending(Throwable cause) {
        pending.values().forEach(p -> p.result().completeExceptionally(cause));
        pending.clear();
        seqByPublishId.clear();
        returned.clear();
    }

    public CompletableFuture<Void> publish(String exchange, String routingKey, AMQP.BasicProperties props, byte[] body) {
        var rejection = checkAcceptance();
        if (rejection.isPresent()) {
            return rejection.get();
        }
        if (!acquireLock()) {
            return failed("Timed out waiting for the publish channel");
        }
        try {
            return checkAcceptance()
                    .orElseGet(() -> doPublish(exchange, routingKey, props, body));
        } finally {
            emitLock.unlock();
        }
    }

    private CompletableFuture<Void> doPublish(String exchange, String routingKey, AMQP.BasicProperties props, byte[] body) {
        var result = new CompletableFuture<Void>();
        long seq = channel.getNextPublishSeqNo();

        var publishId = UUID.randomUUID().toString();
        seqByPublishId.put(publishId, seq);
        pending.put(seq, new Pending(publishId, result));
        var headersWithId = mergeHeaders(props, Map.of(KEY_PUBLISH_ID, publishId));
        var propsWithId = props
                .builder()
                .headers(headersWithId)
                .build();
        try {
            channel.basicPublish(exchange, routingKey, true, propsWithId, body);
        } catch (Exception e) {
            pending.remove(seq);
            seqByPublishId.remove(publishId);
            result.completeExceptionally(e);
        }

        return result;
    }

    private void onConfirm(long tag, boolean multiple, Throwable error) {
        var done = multiple
                ? pending.headMap(tag, true)
                : pending.subMap(tag, true, tag, true);
        done.forEach((seq, p) -> {
            seqByPublishId.remove(p.publishId());
            var unroutable = returned.remove(seq);
            Optional.ofNullable(error).or(() -> Optional.ofNullable(unroutable))
                    .ifPresentOrElse(
                            t -> p.result().completeExceptionally(t),
                            () -> p.result().complete(null)
                    );
        });
        done.clear();
    }

    private void onReturn(Return r) {
        Optional.ofNullable(r.getProperties())
                .map(AMQP.BasicProperties::getHeaders)
                .map(h -> h.get(KEY_PUBLISH_ID))
                .map(Object::toString)
                .map(seqByPublishId::get)
                .ifPresent(seq -> returned.put(seq,
                        new UnroutableMessageException(r.getReplyCode(), r.getReplyText(), r.getExchange())));
    }

    private static Map<String, Object> mergeHeaders(AMQP.BasicProperties props, Map<String, Object> modification) {
        return Optional.ofNullable(props)
                .map(AMQP.BasicProperties::getHeaders)
                .map(h -> (Map<String, Object>) new HashMap<>(h))
                .map(h -> {
                    h.putAll(modification);
                    return h;
                })
                .orElse(modification);
    }

    private boolean acquireLock() {

        try {
            return emitLock.tryLock(LOCK_TIMEOUT.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private Optional<CompletableFuture<Void>> checkAcceptance() {
        if (closing) {
            return Optional.of(failed("Publisher is closed"));
        }
        if (isBlocked.getAsBoolean()) {
            return Optional.of(failed("Broker is blocking publishers"));
        }
        return Optional.empty();
    }

    private static CompletableFuture<Void> failed(Throwable t) {
        return CompletableFuture.failedFuture(t);
    }

    private static CompletableFuture<Void> failed(String message) {
        return failed(new SchedulerException(message));
    }

    public void shutdown(Duration grace) {
        closing = true;
        awaitPendingConfirms(grace);
        if (!pending.isEmpty()) {
            Log.warnf("%d publish(es) unconfirmed after grace period", pending.size());
        }
        failAllPending(new SchedulerException("Publisher closed before broker confirmation"));
    }

    private void awaitPendingConfirms(Duration grace) {
        long deadline = System.nanoTime() + grace.toNanos();
        while (!pending.isEmpty() && System.nanoTime() < deadline) {
            LockSupport.parkNanos(Duration.ofMillis(20).toNanos());
        }
    }

    private void failAllPending(Throwable cause) {
        pending.forEach((_, p) -> p.result().completeExceptionally(cause));
        pending.clear();
        seqByPublishId.clear();
        returned.clear();
    }
}
