package io.github.zforgo.scheduler.internal.transport;

import java.util.UUID;

public record QueuedItem(UUID id, long fireTime) {}
