package io.github.zforgo.scheduler;

import java.util.LinkedList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TierSelector {

    private static final LinkedList<TopologyItem> selectors;
    private static final TopologyItem immediate = new TopologyItem(-1L, "dispatch");

    static {
        selectors = Stream.of(
                new TopologyItem("15s"),
                new TopologyItem("2m"),
                new TopologyItem("30m"),
                new TopologyItem("1h"),
                new TopologyItem("2h"),
                new TopologyItem("5h"),
                new TopologyItem("1d"),
                new TopologyItem("7d")
        ).distinct()
                //descendant by ttl
                .sorted((a, b) -> Math.toIntExact(b.millis() - a.millis()))
                .collect(Collectors.toCollection(LinkedList::new));
    }

    public static TopologyItem select(long remaining) {
        return selectors.stream()
                .filter(item -> item.millis() < remaining)
                .findAny().orElse(immediate);
    }
}
