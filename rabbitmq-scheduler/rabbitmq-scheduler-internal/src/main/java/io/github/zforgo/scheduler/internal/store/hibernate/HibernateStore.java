package io.github.zforgo.scheduler.internal.store.hibernate;

import java.util.Optional;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;

import io.github.zforgo.scheduler.JobState;
import io.github.zforgo.scheduler.internal.store.JobRecord;
import io.github.zforgo.scheduler.internal.store.JobStore;

@ApplicationScoped
@Typed(JobStore.class)
class HibernateStore implements JobStore {

    @Inject
    EntityManager em;

    @Override
    @Transactional(TxType.REQUIRES_NEW)
    public void insert(JobRecord job) {
        var entity = JobEntity.from(job);
        em.persist(entity);
    }

    @Override
    @Transactional
    public Optional<JobRecord> find(UUID id) {
        try {
            var entity = em.find(JobEntity.class, id);
            return Optional.of(new JobRecord(entity.getId(), entity.getJobId(), entity.getJobGroup(), entity.getPayload(),
                    entity.getScheduledTime(), entity.getFireTime(), entity.getState()));
        } catch (NoResultException e) {
            return Optional.empty();
        }

    }

    @Override
    @Transactional
    public Optional<JobState> state(UUID id) {
        return Optional.ofNullable(
                em.createQuery("select e.state from JobEntity e where e.id = :id", JobState.class)
                        .setParameter("id", id)
                        .getSingleResultOrNull()
        );
    }

    @Override
    @Transactional(TxType.REQUIRES_NEW)
    public boolean transition(UUID id, JobState expected, JobState target) {
        return em.createQuery("update JobEntity j set j.state = :target where j.id = :id and j.state = :expected")
                .setParameter("id", id)
                .setParameter("expected", expected)
                .setParameter("target", target)
                .executeUpdate() == 1;
    }
}
