package io.github.zforgo.scheduler.internal;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.mutiny.Uni;

import io.github.zforgo.scheduler.internal.queue.QueuedItem;

@ApplicationScoped
@IfBuildProperty(name = "scheduler.topology.requeue.enabled", stringValue = "true", enableIfMissing = true)
class RequeueProcessor {

    @Inject
    SchedulerEngine engine;

    @Incoming("requeue-requests")
    @Outgoing("requeued-jobs")
    public Uni<Message<QueuedItem>> process(Message<QueuedItem> message) {
        return engine.nextTier(message);
    }
}
