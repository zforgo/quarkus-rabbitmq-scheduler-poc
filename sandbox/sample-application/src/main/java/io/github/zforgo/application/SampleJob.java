package io.github.zforgo.application;

import jakarta.inject.Singleton;

import io.quarkus.logging.Log;

import io.github.zforgo.scheduler.Job;
import io.github.zforgo.scheduler.ScheduledJob;

@Job(id = "sample")
@Singleton
public class SampleJob implements ScheduledJob<Integer> {

    @Override
    public void process(Integer argument) {
        Log.infof("SampleJob process argument: %s%n", argument);
    }
}
