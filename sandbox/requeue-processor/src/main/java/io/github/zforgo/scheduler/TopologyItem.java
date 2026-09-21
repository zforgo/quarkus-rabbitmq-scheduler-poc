package io.github.zforgo.scheduler;

import java.time.Duration;

import io.quarkus.runtime.configuration.DurationConverter;

public record TopologyItem(long millis, String tier) {

    TopologyItem(Duration duration, String tier) {
        this(duration.toMillis(), tier);
    }

    public TopologyItem(String tier) {
        this(DurationConverter.parseDuration(tier), tier);
    }
}
