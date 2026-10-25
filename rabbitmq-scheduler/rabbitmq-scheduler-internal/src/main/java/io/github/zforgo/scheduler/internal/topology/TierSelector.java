package io.github.zforgo.scheduler.internal.topology;

import java.util.LinkedList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class TierSelector {

    private static final Tier immediate = new Tier(-1L, "dispatch");
    private final LinkedList<Tier> selectors;

    public TierSelector() {
        selectors = Stream.of(
                new Tier("15s"),
                new Tier("2m"),
                new Tier("30m"),
                new Tier("1h"),
                new Tier("2h"),
                new Tier("5h"),
                new Tier("1d"),
                new Tier("7d")
        ).distinct()
                //descendant by ttl
                .sorted((a, b) -> Math.toIntExact(b.millis() - a.millis()))
                .collect(Collectors.toCollection(LinkedList::new));

    }

    public Tier select(long scheduledTime, long fireTime) {
        return select(fireTime - scheduledTime);
    }

    public Tier select(long remaining) {
        return selectors.stream()
                .filter(item -> item.millis() < remaining)
                .findAny().orElse(immediate);
    }

}
