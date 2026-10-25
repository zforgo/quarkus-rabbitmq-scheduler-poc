package io.github.zforgo.scheduler.internal.transport;

import io.github.zforgo.scheduler.SchedulerException;

public class UnroutableMessageException extends SchedulerException {

    private final int replyCode;
    private final String exchange;

    public UnroutableMessageException(int replyCode, String replyText, String exchange) {
        this.replyCode = replyCode;
        this.exchange = exchange;
        super("Message unroutable: %d - %s on exchange: %s".formatted(replyCode, replyText, exchange));
    }

    public int replyCode() {
        return replyCode;
    }

    public String exchange() {
        return exchange;
    }

}
