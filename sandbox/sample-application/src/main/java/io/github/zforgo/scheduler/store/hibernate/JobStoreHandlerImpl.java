package io.github.zforgo.scheduler.store.hibernate;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;

import io.github.zforgo.scheduler.store.JobStoreHandler;
import io.github.zforgo.scheduler.store.PayloadCodec;

@ApplicationScoped
public class JobStoreHandlerImpl implements JobStoreHandler {

    @Inject
    PayloadCodec codec;

    @Inject
    EntityManager em;

    @Override
    @Transactional(TxType.REQUIRES_NEW)
    public UUID store(String jobId, String jobGroup, Object payload, long fireTime, long scheduledTime) {
        Objects.requireNonNull(jobId);
        final var encodedPayload = Optional.ofNullable(payload)
                .map(p -> codec.encode(p))
                .orElse(null);
        var entity = new JobStore();
        entity.setJobId(jobId);
        entity.setJobGroup(jobGroup);
        entity.setPayload(encodedPayload);
        entity.setFireTime(fireTime);
        entity.setScheduledTime(scheduledTime);
        entity.setState(JobState.SCHEDULED);
        em.persist(entity);
        return entity.getId();
    }
}
