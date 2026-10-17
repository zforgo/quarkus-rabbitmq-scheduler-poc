package io.github.zforgo.scheduler.internal.registry;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import io.github.zforgo.scheduler.ScheduledJob;
import io.github.zforgo.scheduler.internal.registry.jobs.DefaultNoArgJob;
import io.github.zforgo.scheduler.internal.registry.jobs.DefaultNoArgJobWithId;
import io.github.zforgo.scheduler.internal.registry.jobs.MultiGroupCleanupJob;
import io.github.zforgo.scheduler.internal.registry.jobs.NonRegisteredJob;
import io.github.zforgo.scheduler.internal.registry.jobs.PaymentCleanupJob;
import io.github.zforgo.scheduler.internal.registry.jobs.PaymentProcessor;
import io.github.zforgo.scheduler.internal.registry.jobs.WarehouseCleanupJob;

import static io.github.zforgo.scheduler.SchedulerConfig.DiscoverStrategy.MERGE;
import static io.github.zforgo.scheduler.internal.Assertions.assertNotEmpty;
import static io.github.zforgo.scheduler.internal.ConfigurationUtil.config;
import static io.github.zforgo.scheduler.internal.ConfigurationUtil.coordinate;
import static io.github.zforgo.scheduler.internal.ConfigurationUtil.definitions;
import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("Discovered jobs merged with config")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MergedWithConfigTests {

    private static final List<Class<? extends ScheduledJob<?>>> discoveredJobs = List.of(
            DefaultNoArgJob.class,
            DefaultNoArgJobWithId.class,
            NonRegisteredJob.class,
            PaymentCleanupJob.class,
            WarehouseCleanupJob.class,
            MultiGroupCleanupJob.class
    );

    private JobRegistry registry;

    @BeforeAll
    void setUp() {
        var jobs = Map.of("payment", List.of(
                coordinate("processor", PaymentProcessor.class),
                coordinate("process", NonRegisteredJob.class),
                coordinate("clean", NonRegisteredJob.class)
        ));

        registry = JobRegistry.from(config(jobs, MERGE), definitions(discoveredJobs));
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
