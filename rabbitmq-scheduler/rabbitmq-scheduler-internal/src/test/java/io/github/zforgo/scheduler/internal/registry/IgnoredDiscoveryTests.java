package io.github.zforgo.scheduler.internal.registry;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

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

import static io.github.zforgo.scheduler.internal.Assertions.assertEmpty;
import static io.github.zforgo.scheduler.internal.Assertions.assertNotEmpty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@DisplayName("Discovered jobs ignored with config")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IgnoredDiscoveryTests {

    private JobRegistry registry;

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

    @BeforeAll
    void setUp() {
        var config = new SchedulerConfig() {

            @Override
            public Map<String, List<JobCoordinate>> jobs() {
                return Map.of("payment", List.of(
                        coordinate("process", null),
                        coordinate("clean", null)),
                        "default", List.of(coordinate("given-id", WarehouseCleanupJob.class))
                );
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
        };

        var discoveredJobs = Stream.of(
                JobDefinition.of(DefaultNoArgJob.class),
                JobDefinition.of(DefaultNoArgJobWithId.class),
                JobDefinition.of(NonRegisteredJob.class),
                JobDefinition.of(PaymentProcessor.class),
                JobDefinition.of(PaymentCleanupJob.class),
                JobDefinition.of(WarehouseCleanupJob.class),
                JobDefinition.of(MultiGroupCleanupJob.class)

        ).flatMap(s -> s).toList();
        registry = JobRegistry.from(config, discoveredJobs);
    }

    @Test
    void fulfilledClass() {
        var def = assertNotEmpty(registry.resolve("payment", "process"));
        assertNotNull(def);
        assertEquals("process", def.id());
        assertEquals(PaymentProcessor.class.getName(), def.className());
    }

    @Test
    void fulfilledOnlyNeeded() {
        var def = assertNotEmpty(registry.resolve("payment", "clean"));
        assertEquals("clean", def.id());
        assertEquals(MultiGroupCleanupJob.class.getName(), def.className());
        assertEmpty(registry.resolve("warehouse", "clean"));
    }

    @Test
    void registeredToAnotherGroup() {
        {
            var def = assertNotEmpty(registry.resolve("given-id"));
            assertEquals("given-id", def.id());
            assertEquals(WarehouseCleanupJob.class.getName(), def.className());
        }
        {
            var def = assertNotEmpty(registry.resolve(WarehouseCleanupJob.class));
            assertEquals("given-id", def.id());
            assertEquals(Job.DEFAULT_GROUP, def.group());
            assertEquals(WarehouseCleanupJob.class.getName(), def.className());
        }
        assertEmpty(registry.resolve("warehouse", WarehouseCleanupJob.class));
    }

}
