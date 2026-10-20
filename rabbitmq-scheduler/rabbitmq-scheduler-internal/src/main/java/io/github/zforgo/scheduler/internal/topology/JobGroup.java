package io.github.zforgo.scheduler.internal.topology;

import io.github.zforgo.scheduler.SchedulerConfig;

public record JobGroup(String group, String queue) {

    public JobGroup(SchedulerConfig.WorkerConfig.WorkerGroup workerGroup) {
        this(workerGroup.name(), workerGroup.queueName());
    }
}
