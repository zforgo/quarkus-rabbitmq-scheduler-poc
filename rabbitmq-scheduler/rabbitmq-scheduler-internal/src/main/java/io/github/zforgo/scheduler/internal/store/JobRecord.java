package io.github.zforgo.scheduler.internal.store;

import java.util.UUID;

import io.github.zforgo.scheduler.JobState;

public record JobRecord(UUID id, String jobId, String jobGroup, byte[] payload,
        long scheduledTime, long fireTime, JobState state) {

}
