package io.github.zforgo.scheduler.internal.consume;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import io.quarkus.logging.Log;

import io.github.zforgo.scheduler.SchedulerConfig.WorkerConfig.WorkerGroup;
import io.github.zforgo.scheduler.SchedulerException;
import io.github.zforgo.scheduler.internal.queue.QueuedItem;
import io.github.zforgo.scheduler.internal.transport.QueuedItemCodec;

final class GroupConsumer extends DefaultConsumer {

    private final WorkerGroup group;
    private final Executor executor;
    private final Object ackLock = new Object();
    private final JobExecutionHandler executionHandler;
    private volatile String consumerTag;

    GroupConsumer(Channel channel, WorkerGroup group, JobExecutionHandler executionHandler) {
        super(channel);
        this.group = group;
        this.executor = new BoundedExecutor(group.name(), group.concurrency());
        this.executionHandler = executionHandler;
    }

    void start() {
        try {
            getChannel().basicQos(group.concurrency());
            consumerTag = getChannel().basicConsume(group.queueName(), false, this);
        } catch (IOException e) {
            throw new SchedulerException(e);
        }
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        var deliveryTag = envelope.getDeliveryTag();
        try {
            executor.execute(() -> {
                QueuedItem queuedItem;
                try {
                    queuedItem = QueuedItemCodec.decode(body);
                } catch (RuntimeException e) {
                    Log.errorf(e, "Malformed message on group %s", group.name());
                    nack(deliveryTag);
                    return;
                }

                final var id = queuedItem.id();
                try {
                    executionHandler.createRequest(id).ifPresentOrElse(
                            request -> {
                                ack(deliveryTag);
                                executionHandler.proceed(request);

                            },
                            () -> {
                                Log.debugf("Job execution skipped silently for id: %s", id);
                                ack(deliveryTag);
                            });

                } catch (JobExecutionPreconditionException e) {
                    Log.errorf(e, "Job precondition failed for id: %s", id);
                    nack(deliveryTag);
                } catch (RuntimeException e) {
                    Log.warnf(e, "Cannot process job %s, requeueing", id);
                    requeue(deliveryTag); // claim not taken: safe to retry
                }
            });
        } catch (RejectedExecutionException e) {
            Log.warnf("Concurrency limit exceeded on group: %s ", group.name());
            requeue(deliveryTag);
        }

    }

    private void requeue(long tag) {
        nack(tag, true);
    }

    private void nack(long tag) {
        nack(tag, false);
    }

    private void nack(long tag, boolean requeue) {
        synchronized (ackLock) {
            try {
                getChannel().basicNack(tag, false, requeue);
            } catch (IOException e) { /* channel gone → broker redelivers */ } //TODO
        }

    }

    private void ack(long tag) {
        synchronized (ackLock) {
            try {
                getChannel().basicAck(tag, false);
            } catch (IOException e) { /* channel gone → broker redelivers */ } //TODO
        }
    }
}
