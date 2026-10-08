package io.github.zforgo.scheduler.internal;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.quarkus.test.component.QuarkusComponentTest;
import io.quarkus.test.component.TestConfigProperty;

import io.github.zforgo.scheduler.Job;
import io.github.zforgo.scheduler.SelfContainedJob;

import static io.github.zforgo.scheduler.Assertions.assertEmpty;
import static io.github.zforgo.scheduler.Assertions.assertNotEmpty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusComponentTest(JobRegistry.class)
@TestConfigProperty(key = "scheduler.discovery.strategy", value = "IGNORE")
@DisplayName("Discovered jobs ignored with config")
public class IgnoredDiscoveryTests {

    @Inject
    JobRegistry registry;

    //    @BeforeEach
    //    void setup() {
    //        registry.init(null);
    //    }

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
        var def = assertNotEmpty(registry.resolve("payment", "process"));
        assertNotNull(def);
        assertEquals("process", def.id());
        assertEquals(PaymentProcessor.class.getName(), def.className());
    }

    @Test
    @TestConfigProperty(key = "scheduler.jobs.payment[0].id", value = "cleanup")
    void fulfilledOnlyNeeded() {
        var def = assertNotEmpty(registry.resolve("payment", "cleanup"));
        assertEquals("cleanup", def.id());
        assertEquals(CleanupJob.class.getName(), def.className());
        assertEmpty(registry.resolve("warehouse", "cleanup"));
    }

    @Test
    @TestConfigProperty(key = "scheduler.jobs[0].id", value = "given-id")
    @TestConfigProperty(key = "scheduler.jobs[0].class",
            value = "io.github.zforgo.scheduler.internal.IgnoredDiscoveryTests$PaymentProcessor")
    void registeredToAnotherGroup() {
        {
            var def = assertNotEmpty(registry.resolve("given-id"));
            assertEquals("given-id", def.id());
            assertEquals(PaymentProcessor.class.getName(), def.className());
        }
        {
            var def = assertNotEmpty(registry.resolve(PaymentProcessor.class));
            assertEquals("given-id", def.id());
            assertEquals(Job.DEFAULT_GROUP, def.group());
            assertEquals(PaymentProcessor.class.getName(), def.className());
        }
        assertEmpty(registry.resolve("payment", PaymentProcessor.class));
    }
}
