package io.github.zforgo.scheduler.internal;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import io.github.zforgo.scheduler.Job;
import io.github.zforgo.scheduler.SchedulerConfig.JobCoordinate;

public record JobDefinition(String group, String id, String className) {

    public JobDefinition {
        group = Objects.requireNonNullElse(normalize(group), Job.DEFAULT_GROUP);
        id = normalize(id);
        className = normalize(className);
        if (id == null && className == null) {
            throw new IllegalArgumentException("Either id or class must be provided for job in group '" + group + "'");
        }
    }

    record Key(String group, String id) {

        Key {
            Objects.requireNonNull(group);
            Objects.requireNonNull(id);
        }

        String describe() {
            return "'%s' in group '%s'".formatted(id, group);
        }
    }

    Optional<Key> key() {
        return id == null ? Optional.empty() : Optional.of(new Key(group, id));
    }

    static Stream<JobDefinition> of(Class<?> jobClass) {
        var ann = jobClass.getDeclaredAnnotation(Job.class);
        if (ann == null) {
            return Stream.of(new JobDefinition(null, null, jobClass.getName()));
        }
        var groups = Arrays.stream(ann.groups())
                .map(JobDefinition::normalize)
                .filter(Objects::nonNull)
                .distinct()
                .toList();
        if (groups.isEmpty()) {
            return Stream.of(new JobDefinition(null, ann.id(), jobClass.getName()));
        }
        return groups.stream().map(group -> new JobDefinition(group, ann.id(), jobClass.getName()));
    }

    static JobDefinition of(JobCoordinate coordinate, String group) {
        return new JobDefinition(group, coordinate.id().orElse(null), coordinate.className().orElse(null));
    }

    boolean isComplete() {
        return id != null && className != null;
    }

    boolean matches(JobDefinition other) {
        return group.equals(other.group)
                && ((id != null && id.equals(other.id)) || (className != null && className.equals(other.className)));
    }

    JobDefinition fillFrom(JobDefinition other) {
        return new JobDefinition(group, id != null ? id : other.id, className != null ? className : other.className);
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
