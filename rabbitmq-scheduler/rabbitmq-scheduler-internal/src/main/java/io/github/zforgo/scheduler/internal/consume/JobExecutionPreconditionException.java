package io.github.zforgo.scheduler.internal.consume;

public class JobExecutionPreconditionException extends JobExecutionException {

    public JobExecutionPreconditionException(String message) {
        super(message);
    }

    public JobExecutionPreconditionException(String message, Throwable cause) {
        super(message, cause);
    }

    public JobExecutionPreconditionException(Throwable cause) {
        super(cause);
    }
}
