package io.github.zforgo.scheduler.discovery;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
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
@DisplayName("Discovered jobs merged with config")
public class MergedWithConfigTests {

    @Inject
    JobRegistry registry;

    @BeforeEach
    void setup() {
        registry.init(null);
    }

    static class NonRegisteredJob implements SelfContainedJob {

        public void process() {
        }
    }

    @Singleton
    @Job(id = "process", groups = "payment")
    static class PaymentProcessor implements SelfContainedJob {

        public void process() {
        }
    }

    @Singleton
    @Job(id = "cleanup", groups = { "payment", "warehouse" })
    static class CleanupJob implements SelfContainedJob {

        public void process() {
        }
    }

    @Test
    @TestConfigProperty(key = "scheduler.jobs.payment[0].id", value = "processor")
    @TestConfigProperty(key = "scheduler.jobs.payment[0].class",
            value = "io.github.zforgo.scheduler.discovery.MergedWithConfigTests$PaymentProcessor")
    void overriddenId() {
        {
            var def = registry.resolve("payment", MergedWithConfigTests.PaymentProcessor.class);
            assertNotNull(def);
            assertEquals("processor", def.id());
            assertEquals(MergedWithConfigTests.PaymentProcessor.class.getName(), def.className());
        }
        {
            var def = registry.resolve("payment", "process");
            assertNull(def);
        }
    }

    @Test
    @TestConfigProperty(key = "scheduler.jobs.payment[0].id", value = "process")
    @TestConfigProperty(key = "scheduler.jobs.payment[0].class",
            value = "io.github.zforgo.scheduler.discovery.MergedWithConfigTests$NonRegisteredJob")
    void overriddenClass() {
        {
            var def = registry.resolve("payment", "process");
            assertNotNull(def);
            assertEquals("process", def.id());
            assertEquals(MergedWithConfigTests.NonRegisteredJob.class.getName(), def.className());
        }
        {
            var def = registry.resolve("payment", MergedWithConfigTests.PaymentProcessor.class);
            assertNull(def);
        }
    }

    @Test
    @TestConfigProperty(key = "scheduler.jobs.payment[0].id", value = "cleanup")
    @TestConfigProperty(key = "scheduler.jobs.payment[0].class",
            value = "io.github.zforgo.scheduler.discovery.MergedWithConfigTests$NonRegisteredJob")
    void overrideWhereNeeded() {
        {
            var def = registry.resolve("payment", "cleanup");
            assertNotNull(def);
            assertEquals("cleanup", def.id());
            assertEquals(MergedWithConfigTests.NonRegisteredJob.class.getName(), def.className());
        }
        {
            var def = registry.resolve("warehouse", MergedWithConfigTests.CleanupJob.class);
            assertNotNull(def);
            assertEquals("cleanup", def.id());
            assertEquals(MergedWithConfigTests.CleanupJob.class.getName(), def.className());
        }
    }
}
