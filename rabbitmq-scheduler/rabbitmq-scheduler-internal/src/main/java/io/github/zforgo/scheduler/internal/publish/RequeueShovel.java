package io.github.zforgo.scheduler.internal.publish;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import io.quarkus.logging.Log;

import io.github.zforgo.scheduler.SchedulerException;
import io.github.zforgo.scheduler.internal.topology.Tier;
import io.github.zforgo.scheduler.internal.topology.TierSelector;
import io.github.zforgo.scheduler.internal.transport.Publisher;
import io.github.zforgo.scheduler.internal.transport.QueuedItemCodec;
import io.github.zforgo.scheduler.internal.transport.UnroutableMessageException;

public class RequeueShovel extends DefaultConsumer {

    private final Publisher publisher;
    private final String queueName;
    private final String exchangeName;
    private final TierSelector tierSelector;

    private final Object ackLock = new Object();
    private volatile boolean closing;

    public RequeueShovel(
            Channel channel, Publisher publisher, String queueName, String exchangeName, TierSelector tierSelector
    ) {
        super(channel);
        this.publisher = publisher;
        this.queueName = queueName;
        this.exchangeName = exchangeName;
        this.tierSelector = tierSelector;
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        var deliveryTag = envelope.getDeliveryTag();
        if (closing) {
            nack(deliveryTag, false);
        }
        Tier nextTier;
        try {
            var payload = QueuedItemCodec.decode(body);
            var remaining = ChronoUnit.MILLIS.between(Instant.now(), Instant.ofEpochMilli(payload.fireTime()));
            nextTier = tierSelector.select(remaining);
        } catch (RuntimeException e) {
            nack(deliveryTag, false);
            return;
        }

        var props = properties.builder()
                .headers(Map.of(
                        "job-tier", nextTier.tier()
                ))
                .deliveryMode(2) //durable
                .build();

        publisher.publish(exchangeName, "", props, body)
                .whenComplete((_, failure) -> {
                    if (failure == null) {
                        ack(deliveryTag);
                    } else {
                        handleFailure(deliveryTag, unwrap(failure));
                    }
                });
    }

    private void ack(long tag) {
        synchronized (ackLock) {
            try {
                getChannel().basicAck(tag, false);
            } catch (IOException e) { /* channel gone → broker redelivers */ } //TODO
        }
    }

    private void nack(long tag, boolean requeue) {
        synchronized (ackLock) {
            try {
                getChannel().basicNack(tag, false, requeue);
            } catch (IOException e) { /* channel gone → broker redelivers */ } //TODO
        }
    }

    private void handleFailure(long tag, Throwable failure) {
        if (failure instanceof UnroutableMessageException) {
            nack(tag, false);
        } else {
            requeue(tag); //TODO short delay
        }
        Log.error("Requeue error", failure);
        nack(tag, true);
    }

    private void requeue(long tag) {
        nack(tag, true);
    }

    private static Throwable unwrap(Throwable t) {
        while ((t instanceof CompletionException || t instanceof ExecutionException) && t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }

    void start() {
        try {
            getChannel().basicQos(1);
            getChannel().basicConsume(queueName, false, this);
        } catch (IOException e) {
            throw new SchedulerException(e);
        }
    }

    void shutdown(Duration grace) throws IOException {
        closing = true;
        var tag = getConsumerTag();
        if (tag != null && getChannel().isOpen()) {
            try {
                getChannel().basicCancel(tag);
            } catch (AlreadyClosedException _) {
                Log.debugf("Channel of consumer [%s] already closed", tag);
            }
        }
    }
}
