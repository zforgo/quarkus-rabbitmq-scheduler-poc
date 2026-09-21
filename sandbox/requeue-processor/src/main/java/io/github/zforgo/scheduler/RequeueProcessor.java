package io.github.zforgo.scheduler;

import java.util.HashMap;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMetadata;
import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata;

@ApplicationScoped
public class RequeueProcessor {

    @Incoming("requeue-requests")
    @Outgoing("requeued-jobs")
    public Uni<Message<QueuedItem>> process(Message<QueuedItem> message) {
        var payload = message.getPayload();
        var remaining = payload.fireTime() - System.currentTimeMillis();
        var nextTier = TierSelector.select(remaining);
        return Uni.createFrom()
                .item(
                        () -> message.getMetadata(IncomingRabbitMQMetadata.class)
                                .map(IncomingRabbitMQMetadata::getHeaders)
                                .map(HashMap::new)
                                .orElseGet(HashMap::new)
                )
                .map(
                        headers -> new OutgoingRabbitMQMetadata.Builder()
                                .withContentType("application/json")
                                .withHeaders(headers)
                                .withHeader("job-tier", nextTier.tier())
                                .build()
                )
                .map(message::addMetadata);
        //                .map(meta -> message.withMetadata(Metadata.of(meta)));
    }
}
