package io.github.zforgo.scheduler;

import java.time.Duration;

public interface ScheduledJob<T> {

    void process(T argument) throws Exception;

    default Duration getWaitIntervalAfterFail(int failCount, T argument, Throwable failCause) {
        return null;
    }

}
