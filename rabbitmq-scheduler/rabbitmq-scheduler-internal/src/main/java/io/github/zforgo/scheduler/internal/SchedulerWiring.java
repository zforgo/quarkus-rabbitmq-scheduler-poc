package io.github.zforgo.scheduler.internal;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.interceptor.Interceptor;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.tls.TlsConfigurationRegistry;
import io.smallrye.reactive.messaging.MutinyEmitter;

import io.github.zforgo.scheduler.JobScheduler;
import io.github.zforgo.scheduler.ScheduledJob;
import io.github.zforgo.scheduler.SchedulerConfig;
import io.github.zforgo.scheduler.internal.consume.JobExecutionHandler;
import io.github.zforgo.scheduler.internal.consume.WorkerManager;
import io.github.zforgo.scheduler.internal.queue.QueueHandler;
import io.github.zforgo.scheduler.internal.queue.QueuedItem;
import io.github.zforgo.scheduler.internal.queue.TierSelector;
import io.github.zforgo.scheduler.internal.registry.JobDefinition;
import io.github.zforgo.scheduler.internal.registry.JobRegistry;
import io.github.zforgo.scheduler.internal.store.JobStore;
import io.github.zforgo.scheduler.internal.store.PayloadCodec;
import io.github.zforgo.scheduler.internal.transport.BrokerConnections;
import io.github.zforgo.scheduler.internal.transport.SchedulerConnectionFactory;

@Singleton
class SchedulerWiring {

    @Inject
    @Channel("scheduled-jobs")
    MutinyEmitter<QueuedItem> emitter;

    @Produces
    @Singleton
    ConsumerEngine consumerEngine(
            SchedulerConfig config, Instance<TlsConfigurationRegistry> tls, @Any Instance<ScheduledJob<?>> jobs, JobStore store
    ) {
        var tlsRegistry = tls.isResolvable() ? Optional.of(tls.get()) : Optional.<TlsConfigurationRegistry> empty();
        var connections = new BrokerConnections(SchedulerConnectionFactory.create(config.broker(), tlsRegistry));

        var discovered = discoveredJobs(config, jobs);
        var registry = JobRegistry.from(config, discovered);

		var codec = new PayloadCodec(config.store().poolSize());

        var handler = new JobExecutionHandler(registry, store, codec);
        var workerManager = new WorkerManager(config.worker(), handler);
        return new ConsumerEngine(connections, workerManager);
    }

    void onConsumerStart(
            @Observes @Priority(Interceptor.Priority.LIBRARY_BEFORE + 10) StartupEvent event, ConsumerEngine engine
    ) {
        engine.start();
    }

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

    void close(@Observes ShutdownEvent event) {
    }
}
