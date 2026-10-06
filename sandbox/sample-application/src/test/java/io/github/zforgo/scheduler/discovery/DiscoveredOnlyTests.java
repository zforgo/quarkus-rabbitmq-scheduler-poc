package io.github.zforgo.scheduler.discovery;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.quarkus.test.component.QuarkusComponentTest;
import io.quarkus.test.component.TestConfigProperty;

import io.github.zforgo.scheduler.Job;
import io.github.zforgo.scheduler.SelfContainedJob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@QuarkusComponentTest(JobRegistry.class)
@TestConfigProperty(key = "scheduler.discovery.strategy", value = "MERGE")
@DisplayName("Discovered jobs without config")
public class DiscoveredOnlyTests {

    @Inject
    JobRegistry registry;

    @BeforeEach
    void setup() {
        registry.init(null);
    }

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
            var def = registry.resolve(DiscoveredOnlyTests.DefaultNoArgJob.class);
            assertNotNull(def);
            assertNull(def.id());
            assertEquals(Job.DEFAULT_GROUP, def.group());
        }

        @Test
        void noAnnotationAttributesWithDefaultGroup() {
            var def = registry.resolve(Job.DEFAULT_GROUP, DiscoveredOnlyTests.DefaultNoArgJob.class);
            assertNotNull(def);
            assertNull(def.id());
            assertEquals(Job.DEFAULT_GROUP, def.group());
        }

        @Test
        void jobNotFoundInGroup() {
            var def = registry.resolve("non-existing", DiscoveredOnlyTests.DefaultNoArgJob.class);
            assertNull(def);

        }

        @Test
        void nonRegisteredJob() {
            var def = registry.resolve(DiscoveredOnlyTests.NonRegisteredJob.class);
            assertNull(def);
        }

        @Test
        void jobWithAnnotatedGroup() {
            var def = registry.resolve("payment", DiscoveredOnlyTests.PaymentCleanupJob.class);
            assertNotNull(def);
            assertEquals("cleanup", def.id());
            assertEquals("payment", def.group());
        }

        @Test
        void jobWithGroupInDefault() {
            var def = registry.resolve(DiscoveredOnlyTests.PaymentCleanupJob.class);
            assertNull(def);
        }

        @Test
        void jobNotInGroup() {
            var def = registry.resolve("warehouse", DiscoveredOnlyTests.PaymentCleanupJob.class);
            assertNull(def);
        }
    }

    @Nested
    @DisplayName("Resolve jobs by id")
    class ResolveById {

        @Test
        void idNotInDefault() {
            var def = registry.resolve("cleanup");
            assertNull(def);
        }

        @Test
        void multipleGroups() {
            {
                var def = registry.resolve("payment", "cleanup");
                assertNotNull(def);
                assertEquals("payment", def.group());
                assertEquals("cleanup", def.id());
                assertEquals(DiscoveredOnlyTests.PaymentCleanupJob.class.getName(), def.className());
            }
            {
                var def = registry.resolve("warehouse", "cleanup");
                assertNotNull(def);
                assertEquals("warehouse", def.group());
                assertEquals("cleanup", def.id());
                assertEquals(DiscoveredOnlyTests.WarehouseCleanupJob.class.getName(), def.className());
            }
        }

        @Test
        void idOnlyJob() {
            var def = registry.resolve("sample");
            assertNotNull(def);
            assertEquals(Job.DEFAULT_GROUP, def.group());
            assertEquals("sample", def.id());
            assertEquals(DiscoveredOnlyTests.DefaultNoArgJobWithId.class.getName(), def.className());
        }
    }
}
