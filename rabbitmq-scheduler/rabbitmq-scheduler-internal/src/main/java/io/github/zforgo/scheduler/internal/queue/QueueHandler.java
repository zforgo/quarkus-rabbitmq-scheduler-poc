package io.github.zforgo.scheduler.internal.queue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMetadata;
import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata;

import io.github.zforgo.scheduler.internal.store.JobRecord;
import io.github.zforgo.scheduler.internal.transport.QueuedItemCodec;

public final class QueueHandler {

    private final MutinyEmitter<byte[]> emitter;
    private final TierSelector tierSelector;

    public QueueHandler(MutinyEmitter<byte[]> emitter, TierSelector tierSelector) {
        this.emitter = emitter;
        this.tierSelector = tierSelector;
    }

    public Uni<Void> emit(JobRecord jobRecord) {
        var tier = tierSelector.select(jobRecord.scheduledTime(), jobRecord.fireTime());
        var meta = new OutgoingRabbitMQMetadata.Builder()
                .withHeader("job-tier", tier.tier())
                .withHeader("job-group", jobRecord.jobGroup())
                .build();
        return emitter.sendMessage(Message
                .of(QueuedItemCodec.encode(new QueuedItem(jobRecord.id(), jobRecord.fireTime())))
                .addMetadata(meta));
    }

    public Uni<Message<byte[]>> nextTier(Message<byte[]> message) {
        var payload = QueuedItemCodec.decode(message.getPayload());
        var remaining = ChronoUnit.MILLIS.between(Instant.now(), Instant.ofEpochMilli(payload.fireTime()));
        var nextTier = tierSelector.select(remaining);
        return Uni.createFrom()
                .item(
                        () -> message.getMetadata(IncomingRabbitMQMetadata.class)
                                .map(IncomingRabbitMQMetadata::getHeaders)
                                .map(HashMap::new)
                                .orElseGet(HashMap::new)
                )
                .map(
                        headers -> new OutgoingRabbitMQMetadata.Builder()
                                .withHeaders(headers)
                                .withHeader("job-tier", nextTier.tier())
                                .build()
                )
                .map(message::addMetadata);

    }
}
