package io.github.zforgo.scheduler.internal;

import java.time.Instant;
import java.util.UUID;

import io.smallrye.mutiny.Uni;

import io.github.zforgo.scheduler.JobState;
import io.github.zforgo.scheduler.ScheduledJob;
import io.github.zforgo.scheduler.SchedulerException;
import io.github.zforgo.scheduler.internal.consume.ConsumerWorker;
import io.github.zforgo.scheduler.internal.publish.PublisherWorker;
import io.github.zforgo.scheduler.internal.registry.JobDefinition;
import io.github.zforgo.scheduler.internal.registry.JobRegistry;
import io.github.zforgo.scheduler.internal.store.JobRecord;
import io.github.zforgo.scheduler.internal.store.JobStore;
import io.github.zforgo.scheduler.internal.store.PayloadCodec;
import io.github.zforgo.scheduler.internal.transport.BrokerConnections;

final class SchedulerEngine implements AutoCloseable {

    private final BrokerConnections connections;
    private final JobRegistry registry;
    private final PayloadCodec codec;
    private final JobStore store;
    private final PublisherWorker publisherWorker;
    private final ConsumerWorker consumerWorker;

    public SchedulerEngine(
            BrokerConnections connections,
            PublisherWorker publisher,
            ConsumerWorker workerManager,
            JobRegistry registry, PayloadCodec codec, JobStore store
    ) {
        this.connections = connections;
        this.registry = registry;
        this.codec = codec;
        this.store = store;
        this.publisherWorker = publisher;
        this.consumerWorker = workerManager;
    }

    Uni<UUID> schedule(Class<? extends ScheduledJob<?>> jobClass, String jobGroup, Object payload, Instant fireTime) {
        var coordinates = registry.resolve(jobGroup, jobClass)
                .orElseThrow(() -> new SchedulerException(
                        "Unable to resolve job by class '%s' in group %s ".formatted(jobClass, jobGroup) + jobGroup));
        return schedule(coordinates, payload, fireTime);
    }

    Uni<UUID> schedule(String jobId, String jobGroup, Object payload, Instant fireTime) {
        var coordinates = registry.resolve(jobGroup, jobId)
                .orElseThrow(() -> new SchedulerException(
                        "Unable to resolve job by id '%s' in group %s ".formatted(jobId, jobGroup) + jobGroup));
        return schedule(coordinates, payload, fireTime);
    }

    private Uni<UUID> schedule(JobDefinition coordinates, Object payload, Instant fireTime) {
        var encoded = payload == null ? null : codec.encode(payload);
        var now = Instant.now();
        final var id = UuidV7.of(now);
        var record = new JobRecord(id, coordinates.id(), coordinates.group(), encoded, now.toEpochMilli(),
                fireTime.toEpochMilli(), JobState.SCHEDULED);
        store.insert(record);
        return publisherWorker.emit(record).replaceWith(record.id());
    }

    public void start() {
        connections.open();
        publisherWorker.start(connections.publishing());
        consumerWorker.start(connections.consuming());
    }

    @Override
    public void close() {
        codec.close();
    }
}
