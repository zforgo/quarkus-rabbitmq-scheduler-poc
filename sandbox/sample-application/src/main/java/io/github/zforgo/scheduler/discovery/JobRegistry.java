package io.github.zforgo.scheduler.discovery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import io.quarkus.runtime.StartupEvent;

import io.github.zforgo.scheduler.ScheduledJob;
import io.github.zforgo.scheduler.SchedulerConfig;
import io.github.zforgo.scheduler.SchedulerConfig.DiscoverStrategy;

import static io.github.zforgo.scheduler.Job.DEFAULT_GROUP;

@Singleton
public class JobRegistry {

    @Inject
    SchedulerConfig config;

    @Any
    @Inject
    Instance<ScheduledJob<?>> instances;

    private List<JobDefinition> definitions;

    void init(@Observes StartupEvent event) {
        var discovered = discoverJobs();
        var configured = configuredJobs().stream()
                .map(
                        def -> def.isComplete()
                                ? def
                                : discovered.stream()
                                        .filter(def::matches)
                                        .findFirst()
                                        .map(def::fillFrom)
                                        .orElse(def)
                ).toList();
        var allJobs = new ArrayList<>(configured);
        if (config.discovery().strategy() == DiscoverStrategy.MERGE) {
            discovered.stream()
                    .filter(
                            def -> allJobs.stream()
                                    .filter(Objects::nonNull)
                                    .noneMatch(def::matches)
                    )
                    .forEach(allJobs::add);
        }
        var duplicates = allJobs.stream()
                .filter(Objects::nonNull)
                .filter(def -> def.id() != null)
                .collect(
                        Collectors.groupingBy(
                                JobDefinition::group, LinkedHashMap::new,
                                Collectors.groupingBy(JobDefinition::id, LinkedHashMap::new, Collectors.counting())
                        )
                )
                .entrySet().stream()
                .flatMap(
                        group -> group.getValue().entrySet().stream()
                                .filter(id -> id.getValue() > 1)
                                .map(id -> "%n'%s' in group '%s'".formatted(id.getKey(), group.getKey()))
                )
                .toList();
        if (!duplicates.isEmpty()) {
            throw new IllegalStateException("Non-unique job ids found: " + String.join(", ", duplicates));
        }

        definitions = List.copyOf(allJobs);
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

    JobDefinition resolve(String group, Class<? extends ScheduledJob<?>> jobClass) {
        Objects.requireNonNull(group);
        Objects.requireNonNull(jobClass);
        var matches = definitions.stream()
                .filter(def -> group.equals(def.group()))
                .filter(def -> jobClass.getName().equals(def.className()))
                .toList();
        if (matches.size() > 1) {
            throw new IllegalStateException("Job class '%s' is registered with multiple ids in group '%s'. Specify id instead."
                    .formatted(jobClass.getName(), group));
        }
        return matches.isEmpty() ? null : matches.getFirst();
    }

    JobDefinition resolve(Class<? extends ScheduledJob<?>> jobClass) {
        return resolve(DEFAULT_GROUP, jobClass);
    }

    JobDefinition resolve(String group, String id) {
        Objects.requireNonNull(group);
        Objects.requireNonNull(id);
        return definitions.stream()
                .filter(def -> group.equals(def.group()))
                .filter(def -> id.equals(def.id()))
                .findFirst()
                .orElse(null);
    }

    JobDefinition resolve(String id) {
        return resolve(DEFAULT_GROUP, id);
    }

}
