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

@QuarkusComponentTest(JobRegistry.class)
@TestConfigProperty(key = "scheduler.discovery.strategy", value = "MERGE")
@DisplayName("Discovered jobs merged with config")
public class MergedWithConfigTests {

    @Inject
    JobRegistry registry;

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
            value = "io.github.zforgo.scheduler.internal.MergedWithConfigTests$PaymentProcessor")
    void overriddenId() {
        {
            var def = assertNotEmpty(registry.resolve("payment", MergedWithConfigTests.PaymentProcessor.class));
            assertEquals("processor", def.id());
            assertEquals(MergedWithConfigTests.PaymentProcessor.class.getName(), def.className());
        }
        {
            assertEmpty(registry.resolve("payment", "process"));
        }
    }

    @Test
    @TestConfigProperty(key = "scheduler.jobs.payment[0].id", value = "process")
    @TestConfigProperty(key = "scheduler.jobs.payment[0].class",
            value = "io.github.zforgo.scheduler.internal.MergedWithConfigTests$NonRegisteredJob")
    void overriddenClass() {
        {
            var def = assertNotEmpty(registry.resolve("payment", "process"));
            assertEquals("process", def.id());
            assertEquals(MergedWithConfigTests.NonRegisteredJob.class.getName(), def.className());
        }
        assertEmpty(registry.resolve("payment", MergedWithConfigTests.PaymentProcessor.class));
    }

    @Test
    @TestConfigProperty(key = "scheduler.jobs.payment[0].id", value = "cleanup")
    @TestConfigProperty(key = "scheduler.jobs.payment[0].class",
            value = "io.github.zforgo.scheduler.internal.MergedWithConfigTests$NonRegisteredJob")
    void overrideWhereNeeded() {
        {
            var def = assertNotEmpty(registry.resolve("payment", "cleanup"));
            assertEquals("cleanup", def.id());
            assertEquals(MergedWithConfigTests.NonRegisteredJob.class.getName(), def.className());
        }
        {
            var def = assertNotEmpty(registry.resolve("warehouse", MergedWithConfigTests.CleanupJob.class));
            assertEquals("cleanup", def.id());
            assertEquals(MergedWithConfigTests.CleanupJob.class.getName(), def.className());
        }
    }
}
