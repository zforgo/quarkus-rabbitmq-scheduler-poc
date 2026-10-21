package io.github.zforgo.scheduler.internal.consume;

import java.util.UUID;

import io.github.zforgo.scheduler.ScheduledJob;

record JobExecutionRequest(UUID id, String jobId, String jobGroup, ScheduledJob<?> instance, Object payload) {}
