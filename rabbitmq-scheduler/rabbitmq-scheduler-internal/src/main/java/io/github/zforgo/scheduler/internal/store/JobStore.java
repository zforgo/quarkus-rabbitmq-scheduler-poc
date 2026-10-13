package io.github.zforgo.scheduler.internal.store;

import java.util.Optional;
import java.util.UUID;

import io.github.zforgo.scheduler.JobState;

public interface JobStore {

    void insert(JobRecord job);

    Optional<JobRecord> find(UUID id);

    boolean transition(UUID id, JobState expected, JobState target);
}
