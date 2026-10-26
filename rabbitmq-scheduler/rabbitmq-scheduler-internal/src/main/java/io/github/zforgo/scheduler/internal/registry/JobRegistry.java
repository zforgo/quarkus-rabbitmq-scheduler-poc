package io.github.zforgo.scheduler.internal.registry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import io.github.zforgo.scheduler.ScheduledJob;
import io.github.zforgo.scheduler.SchedulerConfig;
import io.github.zforgo.scheduler.SchedulerConfig.DiscoverStrategy;
import io.github.zforgo.scheduler.internal.registry.JobDefinition.Key;

import static io.github.zforgo.scheduler.Job.DEFAULT_GROUP;

public final class JobRegistry {

    private final Map<Key, JobDefinition> definitions;

    private JobRegistry(Map<Key, JobDefinition> definitions) {
        this.definitions = definitions;
    }

    public static JobRegistry from(SchedulerConfig config, List<JobDefinition> discoveredJobs) {
        var configured = configuredJobs(config).stream()
                .map(def -> complete(def, discoveredJobs))
                .toList();

        var allJobs = new ArrayList<>(configured);
        if (config.discovery().strategy() == DiscoverStrategy.MERGE) {
            for (var candidate : discoveredJobs) {
                if (allJobs.stream().noneMatch(candidate::matches)) {
                    allJobs.add(candidate);
                }
            }
        }
        var index = new HashMap<Key, JobDefinition>();
        var duplicates = new LinkedHashSet<Key>();
        for (var def : allJobs) {
            def.key().ifPresent(key -> {
                if (index.putIfAbsent(key, def) != null) {
                    duplicates.add(key);
                }
            });
        }

        if (!duplicates.isEmpty()) {
            throw new JobRegistryException(
                    "Non-unique job ids found:%n - %s".formatted(
                            duplicates.stream().map(Key::describe).collect(Collectors.joining("%n - ".formatted()))
                    )
            );
        }
        return new JobRegistry(Map.copyOf(index));
    }

    private static List<JobDefinition> configuredJobs(SchedulerConfig config) {
        return config.jobs().entrySet().stream()
                .flatMap(e -> e.getValue().stream().map(coordinate -> JobDefinition.of(coordinate, e.getKey())))
                .toList();

    }

    private static JobDefinition complete(JobDefinition def, List<JobDefinition> discovered) {
        if (def.isComplete()) {
            return def;
        }
        for (var candidate : discovered) {
            if (def.matches(candidate)) {
                return def.fillFrom(candidate);
            }
        }
        return def;
    }

    public boolean isEmpty() {
        return definitions.isEmpty();
    }

    public boolean hasJobs() {
        return !isEmpty();
    }

    public Optional<JobDefinition> resolve(String group, Class<? extends ScheduledJob<?>> jobClass) {
        Objects.requireNonNull(group);
        Objects.requireNonNull(jobClass);
        var matches = definitions.values().stream()
                .filter(def -> group.equals(def.group()))
                .filter(def -> jobClass.getName().equals(def.className()))
                .toList();
        if (matches.size() > 1) {
            throw new JobRegistryException(
                    "Job class '%s' is registered with multiple ids in group '%s'. Specify id instead."
                            .formatted(jobClass.getName(), group)
            );
        }
        return matches.isEmpty() ? Optional.empty() : Optional.of(matches.getFirst());
    }

    public Optional<JobDefinition> resolve(Class<? extends ScheduledJob<?>> jobClass) {
        return resolve(DEFAULT_GROUP, jobClass);
    }

    public Optional<JobDefinition> resolve(String group, String id) {
        return Optional.ofNullable(definitions.get(new Key(group, id)));
    }

    public Optional<JobDefinition> resolve(String id) {
        return resolve(DEFAULT_GROUP, id);
    }

}
