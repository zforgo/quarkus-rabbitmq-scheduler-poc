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
@TestConfigProperty(key = "scheduler.discovery.strategy", value = "IGNORE")
@DisplayName("Discovered jobs ignored with config")
public class IgnoredDiscoveryTests {

    @Inject
    JobRegistry registry;

    @BeforeEach
    void setup() {
        registry.init(null);
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
    @TestConfigProperty(key = "scheduler.jobs.payment[0].id", value = "process")
    void fulfilledClass() {
        var def = registry.resolve("payment", "process");
        assertNotNull(def);
        assertEquals("process", def.id());
        assertEquals(PaymentProcessor.class.getName(), def.className());
    }

    @Test
    @TestConfigProperty(key = "scheduler.jobs.payment[0].id", value = "cleanup")
    void fulfilledOnlyNeeded() {
        var def = registry.resolve("payment", "cleanup");
        assertNotNull(def);
        assertEquals("cleanup", def.id());
        assertEquals(CleanupJob.class.getName(), def.className());
        assertNull(registry.resolve("warehouse", "cleanup"));
    }

    @Test
    @TestConfigProperty(key = "scheduler.jobs[0].id", value = "given-id")
    @TestConfigProperty(key = "scheduler.jobs[0].class",
            value = "io.github.zforgo.scheduler.discovery.IgnoredDiscoveryTests$PaymentProcessor")
    void registeredToAnotherGroup() {
        {
            var def = registry.resolve("given-id");
            assertNotNull(def);
            assertEquals("given-id", def.id());
            assertEquals(PaymentProcessor.class.getName(), def.className());
        }
        {
            var def = registry.resolve(PaymentProcessor.class);
            assertNotNull(def);
            assertEquals("given-id", def.id());
            assertEquals(Job.DEFAULT_GROUP, def.group());
            assertEquals(PaymentProcessor.class.getName(), def.className());
        }
        assertNull(registry.resolve("payment", PaymentProcessor.class));
    }
}
