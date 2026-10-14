package io.github.zforgo.scheduler;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import io.smallrye.config.WithUnnamedKey;

import static io.github.zforgo.scheduler.Job.DEFAULT_GROUP;

@ConfigMapping(prefix = "scheduler")
public interface SchedulerConfig {

    @WithUnnamedKey(DEFAULT_GROUP)
    Map<String, List<JobCoordinate>> jobs();

    StoreConfig store();

    DiscoveryConfig discovery();

    TopologyConfig topology();

    interface DiscoveryConfig {

        @WithDefault("true")
        boolean enabled();

        @WithDefault("MERGE")
        DiscoverStrategy strategy();
    }

    interface StoreConfig {

        @WithDefault("8")
        @WithName("pool-size")
        int poolSize();
    }

    interface JobCoordinate {

        Optional<String> id();

        @WithName("class")
        Optional<String> className();
    }

    enum DiscoverStrategy {
        MERGE,
        IGNORE //TODO probably FULFILL is better
    }

    interface TopologyConfig {

        @WithDefault("true")
        @WithName("requeue.enabled")
        boolean requeue();
    }
}
