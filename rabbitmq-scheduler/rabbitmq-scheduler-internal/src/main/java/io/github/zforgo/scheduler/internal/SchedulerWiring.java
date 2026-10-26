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
import jakarta.inject.Singleton;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.tls.TlsConfigurationRegistry;

import io.github.zforgo.scheduler.JobScheduler;
import io.github.zforgo.scheduler.ScheduledJob;
import io.github.zforgo.scheduler.SchedulerConfig;
import io.github.zforgo.scheduler.internal.consume.ConsumerWorker;
import io.github.zforgo.scheduler.internal.consume.JobExecutionHandler;
import io.github.zforgo.scheduler.internal.publish.PublisherWorker;
import io.github.zforgo.scheduler.internal.registry.JobDefinition;
import io.github.zforgo.scheduler.internal.registry.JobRegistry;
import io.github.zforgo.scheduler.internal.store.JobStore;
import io.github.zforgo.scheduler.internal.store.PayloadCodec;
import io.github.zforgo.scheduler.internal.topology.TierSelector;
import io.github.zforgo.scheduler.internal.transport.BrokerConnections;
import io.github.zforgo.scheduler.internal.transport.SchedulerConnectionFactory;

import static jakarta.interceptor.Interceptor.Priority.LIBRARY_BEFORE;

@Singleton
class SchedulerWiring {

    @Produces
    @Singleton
    SchedulerEngine engine(
            SchedulerConfig config, Instance<TlsConfigurationRegistry> tls,
            JobStore store, @Any Instance<ScheduledJob<?>> jobs
    ) {
        var registry = buildRegistry(config, jobs);
        //TODO check that enabled worker has empty registry => throw ex
        var connections = connectionComponent(config, tls, registry.hasJobs());

        //TODO check and log that neither consuming nor publishing functions were enabled

        var codec = new PayloadCodec(config.store().poolSize());

        var handler = new JobExecutionHandler(registry, store, codec);
        var workerManager = new ConsumerWorker(config.worker(), handler);
        var publisherWorker = new PublisherWorker(config.topology(), new TierSelector());
        return new SchedulerEngine(connections, publisherWorker, workerManager, registry, codec, store);
    }

    private JobRegistry buildRegistry(SchedulerConfig config, @Any Instance<ScheduledJob<?>> jobs) {
        var discovered = discoveredJobs(config, jobs);
        return JobRegistry.from(config, discovered);
    }

    //TODO do not forget topology declaration role
    private BrokerConnections connectionComponent(
            SchedulerConfig config, Instance<TlsConfigurationRegistry> tls, boolean hasJobs
    ) {
        var tlsRegistry = tls.isResolvable() ? Optional.of(tls.get()) : Optional.<TlsConfigurationRegistry> empty();
        var connectionFactory = SchedulerConnectionFactory.create(config.broker(), tlsRegistry);
        var consumingEnabled = config.topology().requeue().enabled() || (config.worker().enabled() && hasJobs);
        var publishingEnabled = consumingEnabled || config.schedulingEnabled();
        return new BrokerConnections(connectionFactory, consumingEnabled, publishingEnabled);
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

    void onStart(@Observes @Priority(LIBRARY_BEFORE + 10) StartupEvent event, SchedulerEngine engine) {
        engine.start();
    }

    void close(@Observes ShutdownEvent event, SchedulerEngine engine) throws Exception {
        engine.shutdown();
    }
}
