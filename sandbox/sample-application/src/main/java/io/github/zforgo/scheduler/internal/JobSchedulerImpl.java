package io.github.zforgo.scheduler.internal;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.github.zforgo.scheduler.JobScheduler;
import io.github.zforgo.scheduler.SchedulerException;
import io.github.zforgo.scheduler.store.JobStoreHandler;

import static io.github.zforgo.scheduler.Job.DEFAULT_GROUP;

@ApplicationScoped
public class JobSchedulerImpl implements JobScheduler {

    @Inject
    JobStoreHandler storeHandler;

    @Inject
    JobRegistry registry;

    @Override
    public UUID schedule(String jobId, Object payload) {
        var fireTime = Instant.now().toEpochMilli();
        return storeJob(jobId, DEFAULT_GROUP, payload, fireTime);
    }

    @Override
    public UUID schedule(String jobId, Object payload, Duration delay) {
        var fireTime = Instant.now().plus(delay).toEpochMilli();
        return storeJob(jobId, DEFAULT_GROUP, payload, fireTime);
    }

    private UUID storeJob(String jobId, String jobGroup, Object payload, long fireTime) {
        var def = registry.resolve(jobGroup, jobId)
                .orElseThrow(
                        () -> new SchedulerException(
                                "Unable to find job with id. '%s' in group: '%s'".formatted(jobId, jobGroup)
                        )
                );
        var scheduledTime = Instant.now().toEpochMilli();
        return storeHandler.store(jobId, def.group(), payload, scheduledTime, fireTime);
    }
}
