package io.github.zforgo.scheduler.internal.registry.jobs;

import io.github.zforgo.scheduler.Job;
import io.github.zforgo.scheduler.SelfContainedJob;

@Job
public class NonRegisteredJob implements SelfContainedJob {

    public void process() {
    }
}
