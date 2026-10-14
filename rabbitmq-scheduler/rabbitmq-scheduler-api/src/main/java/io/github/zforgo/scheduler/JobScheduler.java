package io.github.zforgo.scheduler;

import java.time.Duration;
import java.util.UUID;

public interface JobScheduler {

    default UUID schedule(String jobId) {
        return schedule(jobId, null);
    }

    default UUID schedule(String jobId, Object payload) {
        return schedule(jobId, Job.DEFAULT_GROUP, payload);
    }

    UUID schedule(String jobId, String group, Object payload);

    default UUID schedule(String jobId, Object payload, Duration delay) {
        return schedule(jobId, Job.DEFAULT_GROUP, payload, delay);
    }

    UUID schedule(String jobId, String jobGroup, Object payload, Duration delay);
}
/*
 * schedule
 * - immediately to default group
 * - delayed (duration) to default group
 * - timed (zonedDateTime) to default group
 * - timed (localDateTime) to default group
 *
 * - same as above with group
 */

/*
 * params
 * - payload (simple object)
 * - duration now@UTC + millis
 * - zonedDateTime -> convert to UTC
 * - localDateTime -> zonedDateTime@LOCAL -> convert to UTC
 */

/*
 * job coordinates
 * id - @Identifier value can be overridden by config
 * class<? extends ScheduledJob>
 */
