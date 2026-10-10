package io.github.zforgo.scheduler;

import java.time.Duration;
import java.util.UUID;

public interface JobScheduler {

    default UUID schedule(String jobId) {
        return schedule(jobId, null);
    }

    UUID schedule(String jobId, Object payload);

    UUID schedule(String jobId, Object payload, Duration delay);
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
 * - duration now@GMT + millis
 * - zonedDateTime -> convert to UTC
 * - localDateTime -> zonedDateTime@LOCAL -> convert to UTC
 */

/*
 * job coordinates
 * id - @Identifier value can be overridden by config
 * class<? extends ScheduledJob>
 */
