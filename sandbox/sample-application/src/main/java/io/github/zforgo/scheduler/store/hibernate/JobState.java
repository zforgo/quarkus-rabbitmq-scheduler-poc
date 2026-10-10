package io.github.zforgo.scheduler.store.hibernate;

public enum JobState {
    SCHEDULED,
    BEING_CANCELLED,
    CANCELLED,
    FINISHED,
}
