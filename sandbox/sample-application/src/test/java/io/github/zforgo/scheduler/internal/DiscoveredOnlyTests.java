package io.github.zforgo.scheduler.internal;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.quarkus.test.component.QuarkusComponentTest;
import io.quarkus.test.component.TestConfigProperty;

import io.github.zforgo.scheduler.Job;
import io.github.zforgo.scheduler.SelfContainedJob;

import static io.github.zforgo.scheduler.Assertions.assertEmpty;
import static io.github.zforgo.scheduler.Assertions.assertNotEmpty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@QuarkusComponentTest(JobRegistry.class)
@TestConfigProperty(key = "scheduler.discovery.strategy", value = "MERGE")
@DisplayName("Discovered jobs without config")
public class DiscoveredOnlyTests {

    @Inject
    JobRegistry registry;

    @Job
    static class NonRegisteredJob implements SelfContainedJob {

        public void process() {
        }
    }

    @Singleton
    @Job
    static class DefaultNoArgJob implements SelfContainedJob {

        public void process() {
        }
    }

    @Singleton
    @Job(id = "sample")
    static class DefaultNoArgJobWithId implements SelfContainedJob {

        public void process() {
        }
    }

    @Singleton
    @Job(id = "cleanup", groups = "payment")
    static class PaymentCleanupJob implements SelfContainedJob {

        public void process() {
        }
    }

    @Singleton
    @Job(id = "cleanup", groups = "warehouse")
    static class WarehouseCleanupJob implements SelfContainedJob {

        public void process() {
        }
    }

    @Nested
    @DisplayName("Resolve jobs by class")
    class ResolveByClass {

        @Test
        void noAnnotationAttributes() {
            var def = assertNotEmpty(registry.resolve(DiscoveredOnlyTests.DefaultNoArgJob.class));
            assertNull(def.id());
            assertEquals(Job.DEFAULT_GROUP, def.group());
        }

        @Test
        void noAnnotationAttributesWithDefaultGroup() {
            var def = assertNotEmpty(registry.resolve(Job.DEFAULT_GROUP, DiscoveredOnlyTests.DefaultNoArgJob.class));
            assertNull(def.id());
            assertEquals(Job.DEFAULT_GROUP, def.group());
        }

        @Test
        void jobNotFoundInGroup() {
            assertEmpty(registry.resolve("non-existing", DiscoveredOnlyTests.DefaultNoArgJob.class));
        }

        @Test
        void nonRegisteredJob() {
            assertEmpty(registry.resolve(DiscoveredOnlyTests.NonRegisteredJob.class));
        }

        @Test
        void jobWithAnnotatedGroup() {
            var def = assertNotEmpty(registry.resolve("payment", DiscoveredOnlyTests.PaymentCleanupJob.class));
            assertEquals("cleanup", def.id());
            assertEquals("payment", def.group());
        }

        @Test
        void jobWithGroupInDefault() {
            assertEmpty(registry.resolve(DiscoveredOnlyTests.PaymentCleanupJob.class));
        }

        @Test
        void jobNotInGroup() {
            assertEmpty(registry.resolve("warehouse", DiscoveredOnlyTests.PaymentCleanupJob.class));
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
                assertEquals(DiscoveredOnlyTests.PaymentCleanupJob.class.getName(), def.className());
            }
            {
                var def = assertNotEmpty(registry.resolve("warehouse", "cleanup"));
                assertEquals("warehouse", def.group());
                assertEquals("cleanup", def.id());
                assertEquals(DiscoveredOnlyTests.WarehouseCleanupJob.class.getName(), def.className());
            }
        }

        @Test
        void idOnlyJob() {
            var def = assertNotEmpty(registry.resolve("sample"));
            assertEquals(Job.DEFAULT_GROUP, def.group());
            assertEquals("sample", def.id());
            assertEquals(DiscoveredOnlyTests.DefaultNoArgJobWithId.class.getName(), def.className());
        }
    }
}
