package io.github.zforgo.scheduler.store.hibernate;

import java.util.Objects;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "SCHEDULED_JOBS")
public class JobStore {

    @Id
    @GeneratedValue
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "job_id", nullable = false, updatable = false)
    private String jobId;

    @Column(name = "job_group", nullable = false, updatable = false)
    private String jobGroup;

    @Column(name = "payload")
    private byte[] payload;

    @Column(name = "state", nullable = false)
    @Enumerated(EnumType.STRING)
    private JobState state;

    private long fireTime;
    private long scheduledTime;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJobGroup() {
        return jobGroup;
    }

    public void setJobGroup(String jobGroup) {
        this.jobGroup = jobGroup;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public JobState getState() {
        return state;
    }

    public void setState(JobState state) {
        this.state = state;
    }

    public long getFireTime() {
        return fireTime;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof JobStore jobStore))
            return false;
        return Objects.equals(id, jobStore.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    public void setFireTime(long fireTime) {
        this.fireTime = fireTime;
    }

    public long getScheduledTime() {
        return scheduledTime;
    }

    public void setScheduledTime(long scheduledTime) {
        this.scheduledTime = scheduledTime;
    }
}
