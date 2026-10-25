package io.github.zforgo.scheduler.internal.transport;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Return;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.MultiEmitter;

import io.github.zforgo.scheduler.SchedulerException;

public final class Publisher {

    private static final String KEY_PUBLISH_ID = "scheduler-publish-id";

    private final Channel channel;

    private final ConcurrentNavigableMap<Long, Pending> pending = new ConcurrentSkipListMap<>();

    private final Map<String, Long> seqByPublishId = new ConcurrentHashMap<>();
    private final Map<Long, UnroutableMessageException> returned = new ConcurrentHashMap<>();

    private MultiEmitter<? super Outgoing> emitter;

    private record Outgoing(String exchange, String routingKey, AMQP.BasicProperties props, byte[] body,
            CompletableFuture<Void> result) {}

    private record Pending(String publishId, CompletableFuture<Void> result) {}

    public Publisher(Channel channel) {
        try {
            channel.confirmSelect();
            this.channel = channel;
            this.channel.addConfirmListener(
                    (tag, multiple) -> onConfirm(tag, multiple, null),
                    (tag, multiple) -> onConfirm(tag, multiple, new SchedulerException("Nacked publishing: " + tag))
            );
            this.channel.addReturnListener(this::onReturn);
            //TODO attach shutdownListener
        } catch (IOException e) {
            throw new SchedulerException(e);
        }
        Multi.createFrom().<Outgoing> emitter(e -> emitter = e)
                .emitOn(task -> Thread.ofVirtual().name("scheduler-publisher").start(task))
                .subscribe().with(this::publish);
    }

    public Uni<Void> publish(String exchange, String routingKey, AMQP.BasicProperties props, byte[] body) {
        return Uni.createFrom().completionStage(() -> {
            var f = new CompletableFuture<Void>();
            emitter.emit(new Outgoing(exchange, routingKey, props, body, f));
            return f;
        }).emitOn(Infrastructure.getDefaultWorkerPool());
    }

    private void publish(Outgoing outgoing) {
        long seq = channel.getNextPublishSeqNo();

        var publishId = UUID.randomUUID().toString();
        seqByPublishId.put(publishId, seq);
        pending.put(seq, new Pending(publishId, outgoing.result()));
        var headersWithId = headers(outgoing.props(), Map.of(KEY_PUBLISH_ID, publishId));
        var propsWithId = outgoing.props()
                .builder()
                .headers(headersWithId)
                .build();
        try {
            channel.basicPublish(outgoing.exchange(), outgoing.routingKey(), true, propsWithId, outgoing.body());
        } catch (Exception e) {
            pending.remove(seq);
            seqByPublishId.remove(publishId);
            outgoing.result().completeExceptionally(e);
        }
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
                .ifPresent(seq -> returned.put(seq, new UnroutableMessageException("szeva")));
    }

    private static Map<String, Object> headers(AMQP.BasicProperties props, Map<String, Object> modification) {
        return Optional.ofNullable(props)
                .map(AMQP.BasicProperties::getHeaders)
                .map(h -> (Map<String, Object>) new HashMap<>(h))
                .map(h -> {
                    h.putAll(modification);
                    return h;
                })
                .orElse(modification);
    }
}
