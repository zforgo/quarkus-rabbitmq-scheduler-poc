package io.github.zforgo.scheduler.internal.consume;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import com.rabbitmq.client.Connection;

import io.quarkus.logging.Log;

import io.github.zforgo.scheduler.SchedulerConfig;
import io.github.zforgo.scheduler.SchedulerException;
import io.github.zforgo.scheduler.internal.Closer;

public final class ConsumerWorker {

    private final List<GroupConsumer> consumers = new ArrayList<>();
    private final SchedulerConfig.WorkerConfig config;
    private final JobExecutionHandler handler;

    public ConsumerWorker(SchedulerConfig.WorkerConfig config, JobExecutionHandler handler) {
        this.config = config;
        this.handler = handler;
    }

    public void start(Connection connection) {
        if (!config.enabled()) {
            return;
        }
        if (config.groups().isEmpty()) {
            throw new SchedulerException("Job consumer enabled but no groups configured, nothing will be consumed");
        }
        for (var group : config.groups()) {
            try {
                var consumer = new GroupConsumer(connection.createChannel(), group, handler);
                consumer.start();
                consumers.add(consumer);
            } catch (IOException e) {
                throw new SchedulerException(e);
            }
        }
    }

    public void shutdown(Duration grace, Closer closer) {
        for (var consumer : consumers) {
            closer.close("Stop consumer[%s]".formatted(consumer.groupName()), consumer::hangup);
        }

        long deadline = System.nanoTime() + grace.toNanos();
        for (var consumer : consumers) {
            closer.close("Shutdown consumer[%s]".formatted(consumer.groupName()), () -> {
                var remaining = Duration.ofNanos(Math.max(0, deadline - System.nanoTime()));
                if (!consumer.shutdown(remaining)) {
                    Log.warnf("Consumer[%s]: jobs still running after grace period, interrupting", consumer.groupName());
                    if (!consumer.shutdownNow(Duration.ofSeconds(5))) {
                        Log.warnf("Consumer[%s]: jobs did not stop; lease expiry will recover them", consumer.groupName());
                    }
                }
            });
        }
    }
}
