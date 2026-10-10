package io.github.zforgo.scheduler.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import io.quarkus.runtime.Startup;

import io.github.zforgo.scheduler.ScheduledJob;
import io.github.zforgo.scheduler.SchedulerConfig;
import io.github.zforgo.scheduler.SchedulerConfig.DiscoverStrategy;
import io.github.zforgo.scheduler.internal.JobDefinition.Key;

import static io.github.zforgo.scheduler.Job.DEFAULT_GROUP;

@Singleton
@Startup
public class JobRegistry {

    @Inject
    SchedulerConfig config;

    @Any
    @Inject
    Instance<ScheduledJob<?>> instances;

    private List<JobDefinition> definitions;
    private Map<Key, JobDefinition> definitionsByKey;

    @PostConstruct
    void init() {
        var discovered = discoverJobs();
        var configured = configuredJobs().stream()
                .map(def -> complete(def, discovered))
                .toList();
        var allJobs = new ArrayList<>(configured);
        if (config.discovery().strategy() == DiscoverStrategy.MERGE) {
            for (var candidate : discovered) {
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

        definitions = List.copyOf(allJobs);
        definitionsByKey = Map.copyOf(index);
    }

    private List<JobDefinition> configuredJobs() {
        return config.jobs().entrySet().stream()
                .flatMap(e -> e.getValue().stream().map(coordinate -> JobDefinition.of(coordinate, e.getKey())))
                .toList();

    }

    private List<JobDefinition> discoverJobs() {
        if (config.discovery().enabled()) {
            return instances.handlesStream()
                    .flatMap(handle -> JobDefinition.of(handle.getBean().getBeanClass()))
                    .toList();
        }
        return Collections.emptyList();
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

    Optional<JobDefinition> resolve(String group, Class<? extends ScheduledJob<?>> jobClass) {
        Objects.requireNonNull(group);
        Objects.requireNonNull(jobClass);
        var matches = definitions.stream()
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

    Optional<JobDefinition> resolve(Class<? extends ScheduledJob<?>> jobClass) {
        return resolve(DEFAULT_GROUP, jobClass);
    }

    Optional<JobDefinition> resolve(String group, String id) {
        return Optional.ofNullable(definitionsByKey.get(new Key(group, id)));
    }

    Optional<JobDefinition> resolve(String id) {
        return resolve(DEFAULT_GROUP, id);
    }

}
