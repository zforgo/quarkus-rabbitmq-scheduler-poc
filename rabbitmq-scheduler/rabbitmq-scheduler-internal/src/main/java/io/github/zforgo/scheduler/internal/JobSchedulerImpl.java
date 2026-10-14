package io.github.zforgo.scheduler.internal;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import io.github.zforgo.scheduler.JobScheduler;

final class JobSchedulerImpl implements JobScheduler {

    private final SchedulerEngine engine;

    JobSchedulerImpl(SchedulerEngine engine) {
        this.engine = engine;
    }

    @Override
    public UUID schedule(String jobId, String jobGroup, Object payload) {
        return scheduleJob(jobId, jobGroup, payload, Instant.now());
    }

    @Override
    public UUID schedule(String jobId, String jobGroup, Object payload, Duration delay) {
        var fireTime = Instant.now().plus(delay);
        return scheduleJob(jobId, jobGroup, payload, fireTime);
    }

    private UUID scheduleJob(String jobId, String jobGroup, Object payload, Instant fireTime) {
        return engine.schedule(jobId, jobGroup, payload, fireTime).await().indefinitely();

    }
}
