package io.github.zforgo.scheduler;

import java.time.Duration;
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

    BrokerConfig broker();

    WorkerConfig worker();

    StoreConfig store();

    DiscoveryConfig discovery();

    TopologyConfig topology();

    interface BrokerConfig {

        @WithDefault("${rabbitmq-host:localhost}")
        String host();

        @WithDefault("${rabbitmq-port:5672}")
        int port();

        @WithDefault("${rabbitmq-virtual-host:/}")
        String virtualHost();

        @WithDefault("${rabbitmq-username:}")
        Optional<String> username();

        @WithDefault("${rabbitmq-password:}")
        Optional<String> password();

        @WithDefault("${rabbitmq-ssl:false}")
        boolean ssl();

        Optional<String> tlsConfigurationName();

        // No global alias in the connector → only your key, connector defaults
        @WithDefault("60s")
        Duration connectionTimeout();

        @WithDefault("10s")
        Duration handshakeTimeout();

        @WithDefault("60s")
        Duration requestedHeartbeat();

        @WithDefault("${rabbitmq-reconnect-interval:5}") // plain number = seconds
        Duration networkRecoveryInterval();
    }

    interface WorkerConfig {

        @WithDefault("true")
        boolean enabled();

        @WithDefault("1")
        int concurrency();

        List<WorkerGroup> groups();

        interface WorkerGroup {

            String name();

            @WithName("queue-name")
            Optional<String> queueNameValue();

            @WithDefault("${scheduler.worker.concurrency}")
            int concurrency();

            default String queueName() {
                return queueNameValue().orElse("queued-jobs-" + name());
            }
        }
    }

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
