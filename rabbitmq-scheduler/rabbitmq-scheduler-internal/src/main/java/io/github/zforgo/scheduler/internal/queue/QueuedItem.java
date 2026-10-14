package io.github.zforgo.scheduler.internal.queue;

import java.util.UUID;

public record QueuedItem(UUID id, long fireTime) {}
