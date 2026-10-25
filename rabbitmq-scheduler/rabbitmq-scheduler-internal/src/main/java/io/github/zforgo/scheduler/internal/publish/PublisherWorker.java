package io.github.zforgo.scheduler.internal.publish;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Connection;

import io.smallrye.mutiny.Uni;

import io.github.zforgo.scheduler.SchedulerConfig;
import io.github.zforgo.scheduler.SchedulerException;
import io.github.zforgo.scheduler.internal.store.JobRecord;
import io.github.zforgo.scheduler.internal.topology.TierSelector;
import io.github.zforgo.scheduler.internal.transport.Publisher;
import io.github.zforgo.scheduler.internal.transport.QueuedItem;
import io.github.zforgo.scheduler.internal.transport.QueuedItemCodec;

//TODO shutdown
public final class PublisherWorker {

    private final AtomicReference<Publisher> publisher = new AtomicReference<>();
    private final AtomicReference<RequeueShovel> requeueShovel = new AtomicReference<>();

    private final TierSelector tierSelector;
    private final SchedulerConfig.TopologyConfig config;

    public PublisherWorker(SchedulerConfig.TopologyConfig config, TierSelector tierSelector) {
        this.config = config;
        this.tierSelector = tierSelector;
    }

    public void start(Connection connection) {
        try {
            //TODO check config is enabled
            var publisher = new Publisher(connection.createChannel());
            this.publisher.set(publisher);

            //TODO check config is requeue enabled
            var shovel = new RequeueShovel(connection.createChannel(), publisher,
                    config.requeue().incomingQueueName(), config.outgoingExchangeName(), tierSelector);
            this.requeueShovel.set(shovel);
            this.requeueShovel.getAcquire().start();
        } catch (IOException e) {
            throw new SchedulerException(e);
        }
    }

    public Uni<Void> emit(JobRecord jobRecord) {
        var tier = tierSelector.select(jobRecord.scheduledTime(), jobRecord.fireTime());
        var props = new AMQP.BasicProperties.Builder()
                .deliveryMode(2) //durable
                .headers(Map.of(
                        "job-tier", tier.tier(),
                        "job-group", jobRecord.jobGroup()
                )).build();
        var body = QueuedItemCodec.encode(new QueuedItem(jobRecord.id(), jobRecord.fireTime()));
        return publisher.getAcquire().publish(config.outgoingExchangeName(), "", props, body);
    }

}
