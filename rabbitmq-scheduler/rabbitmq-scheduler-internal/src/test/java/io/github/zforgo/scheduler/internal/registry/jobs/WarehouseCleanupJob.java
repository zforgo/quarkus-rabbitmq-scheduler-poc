package io.github.zforgo.scheduler.internal.registry.jobs;

import jakarta.inject.Singleton;

import io.github.zforgo.scheduler.Job;
import io.github.zforgo.scheduler.SelfContainedJob;

@Singleton
@Job(id = "cleanup", groups = "warehouse")
public class WarehouseCleanupJob implements SelfContainedJob {

    public void process() {
    }
}
