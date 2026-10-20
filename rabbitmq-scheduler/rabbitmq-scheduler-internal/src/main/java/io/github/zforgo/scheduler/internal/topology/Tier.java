package io.github.zforgo.scheduler.internal.topology;

import java.time.Duration;

import io.quarkus.runtime.configuration.DurationConverter;

public record Tier(long millis, String tier) {

    Tier(Duration duration, String tier) {
        this(duration.toMillis(), tier);
    }

    Tier(String tier) {
        this(DurationConverter.parseDuration(tier), tier);
    }
}
