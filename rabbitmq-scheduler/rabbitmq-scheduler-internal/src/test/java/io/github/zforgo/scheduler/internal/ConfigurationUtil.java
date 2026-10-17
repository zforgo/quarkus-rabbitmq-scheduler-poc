package io.github.zforgo.scheduler.internal;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.github.zforgo.scheduler.ScheduledJob;
import io.github.zforgo.scheduler.SchedulerConfig;
import io.github.zforgo.scheduler.internal.registry.JobDefinition;

import static io.github.zforgo.scheduler.SchedulerConfig.DiscoverStrategy.MERGE;

public class ConfigurationUtil {

    private ConfigurationUtil() {
        //avoid direct instantiation
    }

    public static SchedulerConfig.JobCoordinate coordinate(String id, Class<? extends ScheduledJob<?>> clazz) {
        return new SchedulerConfig.JobCoordinate() {

            @Override
            public Optional<String> id() {
                return Optional.ofNullable(id);
            }

            @Override
            public Optional<String> className() {
                return Optional.ofNullable(clazz)
                        .map(Class::getName);
            }
        };
    }

    public static List<JobDefinition> definitions(Collection<Class<? extends ScheduledJob<?>>> classes) {
        return classes.stream().flatMap(JobDefinition::of).toList();
    }

    public static SchedulerConfig emptyConfig() {
        return config(Collections.emptyMap(), MERGE);
    }

    public static SchedulerConfig config(
            List<SchedulerConfig.JobCoordinate> jobs, SchedulerConfig.DiscoverStrategy discoverStrategy
    ) {
        return config(Map.of("default", jobs), discoverStrategy);
    }

    public static SchedulerConfig config(
            Map<String, List<SchedulerConfig.JobCoordinate>> jobs, SchedulerConfig.DiscoverStrategy discoverStrategy
    ) {
        return new SchedulerConfig() {

            @Override
            public Map<String, List<JobCoordinate>> jobs() {
                return jobs;
            }

            @Override
            public StoreConfig store() {
                return null;
            }

            @Override
            public DiscoveryConfig discovery() {
                return new DiscoveryConfig() {

                    @Override
                    public boolean enabled() {
                        return true;
                    }

                    @Override
                    public DiscoverStrategy strategy() {
                        return discoverStrategy;
                    }
                };
            }

            @Override
            public TopologyConfig topology() {
                return null;
            }

        };
    }
}
