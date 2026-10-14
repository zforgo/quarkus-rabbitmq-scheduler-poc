package io.github.zforgo.application;

import java.time.Duration;
import java.util.UUID;

import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import io.github.zforgo.scheduler.JobScheduler;

@Path("/")
@Produces(MediaType.TEXT_PLAIN)
public class TriggerResource {

    @Inject
    JobScheduler scheduler;

    @POST
    @Path("/now")
    public UUID immediately(Integer payload) {
        return scheduler.schedule("sample", payload);
    }

    @POST
    @Path("/3min")
    public UUID threeMins(Integer payload) {
        return scheduler.schedule("sample", payload, Duration.ofMinutes(3));
    }
}
