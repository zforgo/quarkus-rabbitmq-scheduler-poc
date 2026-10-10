package io.github.zforgo.application;

import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import io.quarkus.logging.Log;
import io.quarkus.runtime.Startup;

import io.github.zforgo.scheduler.JobScheduler;

@Singleton
@Startup
public class SampleScheduler {

    @Inject
    JobScheduler scheduler;

    @PostConstruct
    public void init() {
        var id = scheduler.schedule("sample", 1);
        Log.info("Job scheduled: " + id.toString());
    }
}
