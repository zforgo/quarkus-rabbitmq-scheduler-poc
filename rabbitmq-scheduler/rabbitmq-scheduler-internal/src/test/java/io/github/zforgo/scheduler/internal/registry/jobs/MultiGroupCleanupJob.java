package io.github.zforgo.scheduler.internal.registry.jobs;

import jakarta.inject.Singleton;

import io.github.zforgo.scheduler.Job;
import io.github.zforgo.scheduler.SelfContainedJob;

@Singleton
@Job(id = "clean", groups = { "payment", "warehouse" })
public class MultiGroupCleanupJob implements SelfContainedJob {

    public void process() {
    }
}
