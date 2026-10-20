package io.github.zforgo.scheduler.internal;

import java.util.Collections;
import java.util.List;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.interceptor.Interceptor;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.reactive.messaging.MutinyEmitter;

import io.github.zforgo.scheduler.JobScheduler;
import io.github.zforgo.scheduler.ScheduledJob;
import io.github.zforgo.scheduler.SchedulerConfig;
import io.github.zforgo.scheduler.internal.queue.QueueHandler;
import io.github.zforgo.scheduler.internal.queue.TierSelector;
import io.github.zforgo.scheduler.internal.registry.JobDefinition;
import io.github.zforgo.scheduler.internal.registry.JobRegistry;
import io.github.zforgo.scheduler.internal.store.JobStore;
import io.github.zforgo.scheduler.internal.store.PayloadCodec;

@Singleton
class SchedulerWiring {

    @Inject
    @Channel("scheduled-jobs")
    MutinyEmitter<byte[]> emitter;

    @Produces
    @Singleton
    SchedulerEngine engine(SchedulerConfig config, JobStore store, @Any Instance<ScheduledJob<?>> jobs) {
        var codec = new PayloadCodec(config.store().poolSize());
        var discovered = discoveredJobs(config, jobs);
        var registry = JobRegistry.from(config, discovered);
        var queueHandler = new QueueHandler(emitter, new TierSelector());
        return new SchedulerEngine(registry, codec, store, queueHandler);
    }

    @Produces
    @ApplicationScoped
    JobScheduler scheduler(SchedulerEngine engine) {
        return new JobSchedulerImpl(engine);
    }

    private List<JobDefinition> discoveredJobs(SchedulerConfig config, Instance<ScheduledJob<?>> instances) {
        if (config.discovery().enabled()) {
            return instances.handlesStream()
                    .flatMap(handle -> JobDefinition.of(handle.getBean().getBeanClass()))
                    .toList();

        }
        return Collections.emptyList();
    }

    void onStart(@Observes @Priority(Interceptor.Priority.LIBRARY_BEFORE + 10) StartupEvent event, SchedulerEngine engine) {
        // do nothing for now just injecting engine (eager init)
    }

    void close(@Disposes SchedulerEngine engine) {
        engine.close();
    }

}
