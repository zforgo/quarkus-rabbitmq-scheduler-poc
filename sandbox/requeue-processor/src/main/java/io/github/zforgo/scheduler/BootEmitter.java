package io.github.zforgo.scheduler;

import java.time.Duration;
import java.util.UUID;

import jakarta.enterprise.event.Observes;
import jakarta.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata;

@Singleton
public class BootEmitter {

    @Channel("scheduled-jobs")
    Emitter<QueuedItem> emitter;

    void init(@Observes StartupEvent event) {
        //        final var delay = Duration.ofSeconds(35L);
        final var delay = Duration.ofSeconds(153L);
        var tier = TierSelector.select(delay.toMillis());
        var meta = new OutgoingRabbitMQMetadata.Builder()
                .withContentType("application/json")
                .withHeader("job-tier", tier.tier())
                .withHeader("job-group", "payment")
                .build();
        var i = new QueuedItem(UUID.randomUUID(), System.currentTimeMillis() + delay.toMillis());
        emitter.send(Message.of(i).withMetadata(Metadata.of(meta)));
    }
}
