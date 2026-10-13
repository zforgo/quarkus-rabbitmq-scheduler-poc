package io.github.zforgo.scheduler.internal;

import io.github.zforgo.scheduler.internal.registry.JobRegistry;
import io.github.zforgo.scheduler.internal.store.JobStore;
import io.github.zforgo.scheduler.internal.store.PayloadCodec;

final class SchedulerEngine implements AutoCloseable {

    private final JobRegistry registry;
    private final PayloadCodec codec;
    private final JobStore store;

    public SchedulerEngine(JobRegistry registry, PayloadCodec codec, JobStore store) {
        this.registry = registry;
        this.codec = codec;
        this.store = store;
    }

    @Override
    public void close() {
        codec.close();
    }
}
