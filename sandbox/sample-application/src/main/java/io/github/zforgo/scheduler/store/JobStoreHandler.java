package io.github.zforgo.scheduler.store;

import java.util.UUID;

public interface JobStoreHandler {

    UUID store(String jobId, String jobGroup, Object payload, long fireTime, long scheduledTime);
}
