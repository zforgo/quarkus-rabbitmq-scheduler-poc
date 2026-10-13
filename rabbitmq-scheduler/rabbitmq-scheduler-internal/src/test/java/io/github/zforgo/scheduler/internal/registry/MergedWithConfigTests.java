package io.github.zforgo.scheduler.internal.registry;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import io.github.zforgo.scheduler.ScheduledJob;
import io.github.zforgo.scheduler.SchedulerConfig;
import io.github.zforgo.scheduler.internal.registry.jobs.DefaultNoArgJob;
import io.github.zforgo.scheduler.internal.registry.jobs.DefaultNoArgJobWithId;
import io.github.zforgo.scheduler.internal.registry.jobs.MultiGroupCleanupJob;
import io.github.zforgo.scheduler.internal.registry.jobs.NonRegisteredJob;
import io.github.zforgo.scheduler.internal.registry.jobs.PaymentCleanupJob;
import io.github.zforgo.scheduler.internal.registry.jobs.PaymentProcessor;
import io.github.zforgo.scheduler.internal.registry.jobs.WarehouseCleanupJob;

import static io.github.zforgo.scheduler.internal.Assertions.assertNotEmpty;
import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("Discovered jobs merged with config")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MergedWithConfigTests {

    private JobRegistry registry;

    private static SchedulerConfig.JobCoordinate coordinate(String id, Class<? extends ScheduledJob<?>> clazz) {
        return new SchedulerConfig.JobCoordinate() {

            @Override
            public Optional<String> id() {
                return Optional.ofNullable(id);
            }

            @Override
            public Optional<String> className() {
                return Optional.of(clazz.getName());
            }
        };
    }

    @BeforeAll
    void setUp() {
        var config = new SchedulerConfig() {

            @Override
            public Map<String, List<JobCoordinate>> jobs() {
                return Map.of("payment", List.of(
                        coordinate("processor", PaymentProcessor.class),
                        coordinate("process", NonRegisteredJob.class),
                        coordinate("clean", NonRegisteredJob.class)
                ));
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
                        return DiscoverStrategy.MERGE;
                    }
                };
            }
        };

        var discoveredJobs = Stream.of(
                JobDefinition.of(DefaultNoArgJob.class),
                JobDefinition.of(DefaultNoArgJobWithId.class),
                JobDefinition.of(NonRegisteredJob.class),
                JobDefinition.of(PaymentCleanupJob.class),
                JobDefinition.of(WarehouseCleanupJob.class),
                JobDefinition.of(MultiGroupCleanupJob.class)

        ).flatMap(s -> s).toList();
        registry = JobRegistry.from(config, discoveredJobs);
    }

    @Test
    void overriddenId() {
        var def = assertNotEmpty(registry.resolve("payment", PaymentProcessor.class));
        assertEquals("processor", def.id());
        assertEquals(PaymentProcessor.class.getName(), def.className());
    }

    @Test
    void overriddenClass() {
        {
            var def = assertNotEmpty(registry.resolve("payment", "process"));
            assertEquals("process", def.id());
            assertEquals(NonRegisteredJob.class.getName(), def.className());
        }
    }

    @Test
    void overrideWhereNeeded() {
        {
            var def = assertNotEmpty(registry.resolve("payment", "clean"));
            assertEquals("clean", def.id());
            assertEquals(NonRegisteredJob.class.getName(), def.className());
        }
        {
            var def = assertNotEmpty(registry.resolve("warehouse", MultiGroupCleanupJob.class));
            assertEquals("clean", def.id());
            assertEquals(MultiGroupCleanupJob.class.getName(), def.className());
        }
    }
}
