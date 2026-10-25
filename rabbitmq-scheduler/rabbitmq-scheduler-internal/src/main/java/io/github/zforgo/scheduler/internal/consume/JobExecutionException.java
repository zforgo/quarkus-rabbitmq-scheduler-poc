package io.github.zforgo.scheduler.internal.consume;

import io.github.zforgo.scheduler.SchedulerException;

public class JobExecutionException extends SchedulerException {

    public JobExecutionException(String message) {
        super(message);
    }

    public JobExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public JobExecutionException(Throwable cause) {
        super(cause);
    }
}
