package io.github.zforgo.scheduler.internal;

import java.time.Instant;
import java.util.UUID;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Uni;

import io.github.zforgo.scheduler.JobState;
import io.github.zforgo.scheduler.ScheduledJob;
import io.github.zforgo.scheduler.SchedulerException;
import io.github.zforgo.scheduler.internal.queue.QueueHandler;
import io.github.zforgo.scheduler.internal.registry.JobDefinition;
import io.github.zforgo.scheduler.internal.registry.JobRegistry;
import io.github.zforgo.scheduler.internal.store.JobRecord;
import io.github.zforgo.scheduler.internal.store.JobStore;
import io.github.zforgo.scheduler.internal.store.PayloadCodec;

final class SchedulerEngine implements AutoCloseable {

    private final JobRegistry registry;
    private final PayloadCodec codec;
    private final JobStore store;
    private final QueueHandler queueHandler;

    public SchedulerEngine(JobRegistry registry, PayloadCodec codec, JobStore store, QueueHandler queueHandler) {
        this.registry = registry;
        this.codec = codec;
        this.store = store;
        this.queueHandler = queueHandler;
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
        return queueHandler.emit(record).replaceWith(record.id());
    }

    Uni<Message<byte[]>> nextTier(Message<byte[]> message) {
        //TODO handle cancellation
        return queueHandler.nextTier(message);
    }

    @Override
    public void close() {
        codec.close();
    }
}
