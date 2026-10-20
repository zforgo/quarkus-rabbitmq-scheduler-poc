package io.github.zforgo.scheduler.internal.consume;

import java.util.UUID;

import io.quarkus.logging.Log;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.util.TypeLiteral;

import io.quarkus.arc.Arc;
import io.quarkus.arc.InstanceHandle;

import io.github.zforgo.scheduler.JobState;
import io.github.zforgo.scheduler.ScheduledJob;
import io.github.zforgo.scheduler.SchedulerException;
import io.github.zforgo.scheduler.internal.registry.JobDefinition;
import io.github.zforgo.scheduler.internal.registry.JobRegistry;
import io.github.zforgo.scheduler.internal.store.JobStore;
import io.github.zforgo.scheduler.internal.store.PayloadCodec;

public final class JobExecutionHandler {

    private final JobRegistry registry;
    private final JobStore store;
    private final PayloadCodec codec;

    public JobExecutionHandler(JobRegistry registry, JobStore store, PayloadCodec codec) {
        this.registry = registry;
        this.store = store;
        this.codec = codec;
    }

    public boolean canProceed(UUID id) {
        var state = store.state(id).orElseThrow(() -> new SchedulerException("No such job id " + id));
        var result = switch (state) {
            case SCHEDULED -> true;
            case BEING_CANCELLED, CANCELLED -> false;
            case FINISHED -> throw new SchedulerException("Job execution [%s] has already been finished".formatted(id));
        };
        if (state == JobState.BEING_CANCELLED) {
            store.transition(id, JobState.BEING_CANCELLED, JobState.CANCELLED);
        }
        return result;
    }

    void proceed(UUID id) {
        var record = store.find(id).orElseThrow(() -> new JobExecutionPreconditionException("No such job by id " + id));
        var instance = select(record.jobId(), record.jobGroup());
        var payload = decodePayload(record.payload());
        try {
            invoke(instance, payload);
        } catch (Exception e) {
			Log.error("Job execution [{}] has been failed", id, e);
			//TODO redelivery if necessary
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void invoke(ScheduledJob<T> instance, Object payload) throws Exception {
        instance.process((T) payload);
    }

    private Object decodePayload(byte[] payload) {
        try {
            return codec.decode(payload);
        } catch (Exception e) {
            throw new JobExecutionPreconditionException(e);
        }
    }

    private ScheduledJob<?> select(String jobId, String group) {
        var className = registry.resolve(group, jobId)
                .or(() -> {
                    throw new JobExecutionPreconditionException(
                            "Unable to resolve job by id '%s' in group %s ".formatted(jobId, group));
                })
                .map(JobDefinition::className)
                .orElseThrow(
                        () -> new JobExecutionPreconditionException(
                                "Unable to resolve job class by id '%s' in group %s ".formatted(jobId, group))
                );
        return Arc.container()
                .listAll(new TypeLiteral<ScheduledJob<?>>() {}, Any.Literal.INSTANCE).stream()
                .filter(handle -> handle.getBean().getImplementationClass().getName().equals(className))
                .map(InstanceHandle::get)
                .findFirst()
                .orElseThrow(
                        () -> new JobExecutionPreconditionException("No ScheduledJob bean found: '%s' ".formatted(className)));

    }
}
