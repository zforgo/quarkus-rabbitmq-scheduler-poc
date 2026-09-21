package io.github.zforgo.scheduler;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;

public record QueuedItem(UUID jobId, long fireTime) {

    QueuedItem(UUID jobId, Duration delay) {
        this(jobId, LocalDateTime.now().plus(delay).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
    }

    QueuedItem(UUID jobId, LocalDateTime fireTime) {
        this(jobId, fireTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
    }
}
