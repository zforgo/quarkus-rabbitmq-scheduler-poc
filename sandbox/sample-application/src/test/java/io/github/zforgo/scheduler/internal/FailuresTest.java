package io.github.zforgo.scheduler.internal;

import jakarta.inject.Singleton;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.quarkus.arc.Arc;
import io.quarkus.test.component.QuarkusComponentTest;
import io.quarkus.test.component.TestConfigProperty;

import io.github.zforgo.scheduler.Job;
import io.github.zforgo.scheduler.SelfContainedJob;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusComponentTest(JobRegistry.class)
@TestConfigProperty(key = "scheduler.discovery.strategy", value = "IGNORE")
@DisplayName("Checks JobRegistry failures")
public class FailuresTest {

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
    @TestConfigProperty(key = "scheduler.jobs[0].id", value = "given-id")
    @TestConfigProperty(key = "scheduler.jobs[0].class",
            value = "io.github.zforgo.scheduler.internal.FailuresTest$PaymentProcessor")
    @TestConfigProperty(key = "scheduler.jobs[1].id", value = "given-id")
    @TestConfigProperty(key = "scheduler.jobs[1].class",
            value = "io.github.zforgo.scheduler.internal.FailuresTest$CleanupJob")
    void multipleId() {
        var ex = assertThrows(JobRegistryException.class, () -> Arc.container().select(JobRegistry.class).get());
        assertThat(ex.getMessage(), endsWith("'given-id' in group 'default'"));
    }

    @Test
    @TestConfigProperty(key = "scheduler.jobs[0].id", value = "given-id")
    @TestConfigProperty(key = "scheduler.jobs[0].class",
            value = "io.github.zforgo.scheduler.internal.FailuresTest$CleanupJob")
    @TestConfigProperty(key = "scheduler.jobs[1].id", value = "other-id")
    @TestConfigProperty(key = "scheduler.jobs[1].class",
            value = "io.github.zforgo.scheduler.internal.FailuresTest$CleanupJob")
    void ambiguousJobClass(JobRegistry registry) {
        var ex = assertThrows(JobRegistryException.class, () -> registry.resolve(CleanupJob.class));
        assertEquals(
                "Job class '%s' is registered with multiple ids in group '%s'. Specify id instead."
                        .formatted(CleanupJob.class.getName(), Job.DEFAULT_GROUP),
                ex.getMessage()
        );
    }
}
