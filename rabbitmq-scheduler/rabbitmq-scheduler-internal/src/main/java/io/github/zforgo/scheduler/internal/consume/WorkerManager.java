package io.github.zforgo.scheduler.internal.consume;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.rabbitmq.client.Connection;

import io.github.zforgo.scheduler.SchedulerConfig;
import io.github.zforgo.scheduler.SchedulerException;

public final class WorkerManager {

    private final List<GroupConsumer> consumers = new ArrayList<>();
    private final SchedulerConfig.WorkerConfig config;
    private final JobExecutionHandler handler;

    public WorkerManager(SchedulerConfig.WorkerConfig config, JobExecutionHandler handler) {
        this.config = config;
        this.handler = handler;
    }

    //TODO implement
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
}
