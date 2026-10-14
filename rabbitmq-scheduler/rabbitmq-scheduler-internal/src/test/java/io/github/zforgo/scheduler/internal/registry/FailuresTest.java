package io.github.zforgo.scheduler.internal.registry;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.github.zforgo.scheduler.Job;
import io.github.zforgo.scheduler.ScheduledJob;
import io.github.zforgo.scheduler.SchedulerConfig;
import io.github.zforgo.scheduler.internal.registry.jobs.DefaultNoArgJob;
import io.github.zforgo.scheduler.internal.registry.jobs.DefaultNoArgJobWithId;
import io.github.zforgo.scheduler.internal.registry.jobs.MultiGroupCleanupJob;
import io.github.zforgo.scheduler.internal.registry.jobs.NonRegisteredJob;
import io.github.zforgo.scheduler.internal.registry.jobs.PaymentCleanupJob;
import io.github.zforgo.scheduler.internal.registry.jobs.PaymentProcessor;
import io.github.zforgo.scheduler.internal.registry.jobs.WarehouseCleanupJob;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("Checks JobRegistry failures")
class FailuresTest {

    private static SchedulerConfig.JobCoordinate coordinate(String id, Class<? extends ScheduledJob<?>> clazz) {
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

    private static JobRegistry newRegistry(List<SchedulerConfig.JobCoordinate> jobs) {
        var config = new SchedulerConfig() {

            @Override
            public Map<String, List<JobCoordinate>> jobs() {
                return Map.of("default", jobs);
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
                        return false;
                    }

                    @Override
                    public DiscoverStrategy strategy() {
                        return DiscoverStrategy.IGNORE;
                    }
                };
            }

	        @Override
			public TopologyConfig topology() {
				return null;
			}
		};
        return JobRegistry.from(config, discoveredJobs());
    }

    private static List<JobDefinition> discoveredJobs() {
        return Stream.of(
                JobDefinition.of(DefaultNoArgJob.class),
                JobDefinition.of(DefaultNoArgJobWithId.class),
                JobDefinition.of(NonRegisteredJob.class),
                JobDefinition.of(PaymentProcessor.class),
                JobDefinition.of(PaymentCleanupJob.class),
                JobDefinition.of(WarehouseCleanupJob.class),
                JobDefinition.of(MultiGroupCleanupJob.class)

        ).flatMap(s -> s).toList();
    }

    @Test
    void multipleId() {
        var config = List.of(
                coordinate("given-id", PaymentProcessor.class),
                coordinate("given-id", MultiGroupCleanupJob.class)
        );
        var ex = assertThrows(JobRegistryException.class, () -> newRegistry(config));
        assertThat(ex.getMessage(), endsWith("'given-id' in group 'default'"));
    }

    @Test
    void ambiguousJobClass() {
        var config = List.of(
                coordinate("given-id", PaymentProcessor.class),
                coordinate("other-id", PaymentProcessor.class)
        );
        var registry = newRegistry(config);
        var ex = assertThrows(JobRegistryException.class, () -> registry.resolve(PaymentProcessor.class));
        assertEquals(
                "Job class '%s' is registered with multiple ids in group '%s'. Specify id instead."
                        .formatted(PaymentProcessor.class.getName(), Job.DEFAULT_GROUP),
                ex.getMessage()
        );
    }
}
