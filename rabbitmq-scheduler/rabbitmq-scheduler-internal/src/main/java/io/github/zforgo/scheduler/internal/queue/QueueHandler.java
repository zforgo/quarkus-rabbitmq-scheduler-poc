package io.github.zforgo.scheduler.internal.queue;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata;

import io.github.zforgo.scheduler.internal.store.JobRecord;

public final class QueueHandler {

    private final MutinyEmitter<QueuedItem> emitter;
    private final TierSelector tierSelector;

    public QueueHandler(MutinyEmitter<QueuedItem> emitter, TierSelector tierSelector) {
        this.emitter = emitter;
        this.tierSelector = tierSelector;
    }

    public Uni<Void> emit(JobRecord jobRecord) {
        var tier = tierSelector.select(jobRecord.scheduledTime(), jobRecord.fireTime());
        var meta = new OutgoingRabbitMQMetadata.Builder()
                .withContentType("application/json")
                .withHeader("job-tier", tier.tier())
                .withHeader("job-group", jobRecord.jobGroup())
                .build();
        return emitter.sendMessage(Message
                .of(new QueuedItem(jobRecord.id(), jobRecord.fireTime()))
                .addMetadata(meta));

    }
}
