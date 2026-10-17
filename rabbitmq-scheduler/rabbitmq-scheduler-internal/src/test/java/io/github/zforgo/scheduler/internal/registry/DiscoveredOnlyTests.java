package io.github.zforgo.scheduler.internal.registry;

import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import io.github.zforgo.scheduler.Job;
import io.github.zforgo.scheduler.ScheduledJob;
import io.github.zforgo.scheduler.internal.registry.jobs.DefaultNoArgJob;
import io.github.zforgo.scheduler.internal.registry.jobs.DefaultNoArgJobWithId;
import io.github.zforgo.scheduler.internal.registry.jobs.NonRegisteredJob;
import io.github.zforgo.scheduler.internal.registry.jobs.PaymentCleanupJob;
import io.github.zforgo.scheduler.internal.registry.jobs.WarehouseCleanupJob;

import static io.github.zforgo.scheduler.internal.Assertions.assertEmpty;
import static io.github.zforgo.scheduler.internal.Assertions.assertNotEmpty;
import static io.github.zforgo.scheduler.internal.ConfigurationUtil.definitions;
import static io.github.zforgo.scheduler.internal.ConfigurationUtil.emptyConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("Discovered jobs without config")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DiscoveredOnlyTests {

    private static final List<Class<? extends ScheduledJob<?>>> discoveredJobs = List.of(
            DefaultNoArgJob.class,
            DefaultNoArgJobWithId.class,
            NonRegisteredJob.class,
            PaymentCleanupJob.class,
            WarehouseCleanupJob.class
    );

    private JobRegistry registry;

    @BeforeAll
    void setUp() {
        registry = JobRegistry.from(emptyConfig(), definitions(discoveredJobs));
    }

    @Nested
    @DisplayName("Resolve jobs by class")
    class ResolveByClass {

        @Test
        void noAnnotationIdAttribute() {
            assertEmpty(registry.resolve(DefaultNoArgJob.class));
        }

        @Test
        void jobNotFoundInGroup() {
            assertEmpty(registry.resolve("non-existing", PaymentCleanupJob.class));
        }

        @Test
        void nonRegisteredJob() {
            assertEmpty(registry.resolve(NonRegisteredJob.class));
        }

        @Test
        void jobInDefaultGroup() {
            var def = assertNotEmpty(registry.resolve(DefaultNoArgJobWithId.class));
            assertEquals("sample", def.id());
            assertEquals(Job.DEFAULT_GROUP, def.group());
        }

        @Test
        void jobWithAnnotatedGroup() {
            var def = assertNotEmpty(registry.resolve("payment", PaymentCleanupJob.class));
            assertEquals("cleanup", def.id());
            assertEquals("payment", def.group());
        }

        @Test
        void jobWithGroupInDefault() {
            assertEmpty(registry.resolve(PaymentCleanupJob.class));
        }

        @Test
        void jobNotInGroup() {
            assertEmpty(registry.resolve("warehouse", PaymentCleanupJob.class));
        }

    }

    @Nested
    @DisplayName("Resolve jobs by id")
    class ResolveById {

        @Test
        void idNotInDefault() {
            assertEmpty(registry.resolve("cleanup"));
        }

        @Test
        void multipleGroups() {
            {
                var def = assertNotEmpty(registry.resolve("payment", "cleanup"));
                assertEquals("payment", def.group());
                assertEquals("cleanup", def.id());
                assertEquals(PaymentCleanupJob.class.getName(), def.className());
            }
            {
                var def = assertNotEmpty(registry.resolve("warehouse", "cleanup"));
                assertEquals("warehouse", def.group());
                assertEquals("cleanup", def.id());
                assertEquals(WarehouseCleanupJob.class.getName(), def.className());
            }
        }

        @Test
        void idOnlyJob() {
            var def = assertNotEmpty(registry.resolve("sample"));
            assertEquals(Job.DEFAULT_GROUP, def.group());
            assertEquals("sample", def.id());
            assertEquals(DefaultNoArgJobWithId.class.getName(), def.className());
        }
    }

}
