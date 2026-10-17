package io.github.zforgo.scheduler.internal.registry;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.github.zforgo.scheduler.Job;
import io.github.zforgo.scheduler.ScheduledJob;
import io.github.zforgo.scheduler.SchedulerConfig;
import io.github.zforgo.scheduler.internal.registry.jobs.DefaultNoArgJob;
import io.github.zforgo.scheduler.internal.registry.jobs.DefaultNoArgJobWithId;
import io.github.zforgo.scheduler.internal.registry.jobs.MultiGroupCleanupJob;
import io.github.zforgo.scheduler.internal.registry.jobs.NonRegisteredJob;
import io.github.zforgo.scheduler.internal.registry.jobs.PaymentCleanupJob;
import io.github.zforgo.scheduler.internal.registry.jobs.PaymentProcessor;
import io.github.zforgo.scheduler.internal.registry.jobs.WarehouseCleanupJob;

import static io.github.zforgo.scheduler.SchedulerConfig.DiscoverStrategy.IGNORE;
import static io.github.zforgo.scheduler.internal.ConfigurationUtil.config;
import static io.github.zforgo.scheduler.internal.ConfigurationUtil.coordinate;
import static io.github.zforgo.scheduler.internal.ConfigurationUtil.definitions;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("Checks JobRegistry failures")
class FailuresTest {

    private static final List<Class<? extends ScheduledJob<?>>> discoveredJobs = List.of(
            DefaultNoArgJob.class,
            DefaultNoArgJobWithId.class,
            NonRegisteredJob.class,
            PaymentProcessor.class,
            PaymentCleanupJob.class,
            WarehouseCleanupJob.class,
            MultiGroupCleanupJob.class
    );

    private static JobRegistry newRegistry(List<SchedulerConfig.JobCoordinate> jobs) {
        return JobRegistry.from(config(jobs, IGNORE), definitions(discoveredJobs));
    }

    @Test
    void multipleId() {
        var coordinates = List.of(
                coordinate("given-id", PaymentProcessor.class),
                coordinate("given-id", MultiGroupCleanupJob.class)
        );
        var ex = assertThrows(JobRegistryException.class, () -> newRegistry(coordinates));
        assertThat(ex.getMessage(), endsWith("'given-id' in group 'default'"));
    }

    @Test
    void ambiguousJobClass() {
        var coordinates = List.of(
                coordinate("given-id", PaymentProcessor.class),
                coordinate("other-id", PaymentProcessor.class)
        );
        var registry = newRegistry(coordinates);
        var ex = assertThrows(JobRegistryException.class, () -> registry.resolve(PaymentProcessor.class));
        assertEquals(
                "Job class '%s' is registered with multiple ids in group '%s'. Specify id instead."
                        .formatted(PaymentProcessor.class.getName(), Job.DEFAULT_GROUP),
                ex.getMessage()
        );
    }
}
