package io.github.zforgo.scheduler.internal.transport;

import io.github.zforgo.scheduler.SchedulerException;

public class UnroutableMessageException extends SchedulerException {
    //TODO implement
    //    private final int replyCode;
    //    private final String id;
    //	private final long seq;

    public UnroutableMessageException(String message) {
        super(message);
    }
}
