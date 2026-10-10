package io.github.zforgo.scheduler.store;

import io.github.zforgo.scheduler.SchedulerException;

public class PayloadCodecException extends SchedulerException {

    public PayloadCodecException(String message) {
        super(message);
    }

    public PayloadCodecException(Throwable cause) {
        super(cause);
    }

    public PayloadCodecException(String message, Throwable cause) {
        super(message, cause);
    }
}
