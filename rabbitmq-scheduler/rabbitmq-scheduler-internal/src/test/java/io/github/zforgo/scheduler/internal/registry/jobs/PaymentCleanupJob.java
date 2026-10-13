package io.github.zforgo.scheduler.internal.registry.jobs;

import jakarta.inject.Singleton;

import io.github.zforgo.scheduler.Job;
import io.github.zforgo.scheduler.SelfContainedJob;

@Singleton
@Job(id = "cleanup", groups = "payment")
public class PaymentCleanupJob implements SelfContainedJob {

    public void process() {
    }
}
