package io.github.zforgo.scheduler.internal.transport;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import io.quarkus.logging.Log;

import io.github.zforgo.scheduler.SchedulerException;
import io.github.zforgo.scheduler.internal.Closer;

public final class BrokerConnections {

    private final ConnectionFactory factory;
    private final boolean consumingEnabled, publishingEnabled;

    private volatile Connection publishing;
    private volatile Connection consuming;

    private volatile boolean publishingBlocked;

    public BrokerConnections(ConnectionFactory factory, boolean consumingEnabled, boolean publishingEnabled) {
        this.factory = factory;
        this.consumingEnabled = consumingEnabled;
        this.publishingEnabled = publishingEnabled;
    }

    public void open() {
        try {
            publishing = publishingEnabled ? factory.newConnection("scheduler-publisher") : null;
            consuming = consumingEnabled ? factory.newConnection("scheduler-consumer") : null;
            publishing.addBlockedListener(
                    reason -> {
                        publishingBlocked = true;
                        Log.warnf("Broker blocked publishing: %s", reason);
                    },
                    () -> {
                        publishingBlocked = false;
                        Log.info("Broker unblocked publishing");
                    });
        } catch (IOException | TimeoutException e) {
            throw new SchedulerException(e);
        }
    }

    public Connection publishing() {
        return requireOpen(publishing, "publishing");
    }

    public Connection consuming() {
        return requireOpen(consuming, "consuming");
    }

    private static Connection requireOpen(Connection connection, String name) {
        if (connection == null) {
            throw new SchedulerException("The connection for " + name + " was not opened "
                    + "(scheduler not started, or the function using it is disabled)");
        }
        if (!connection.isOpen()) {
            throw new SchedulerException("The connection for " + name + " is closed", connection.getCloseReason());
        }
        return connection;
    }

    public BooleanSupplier publishingBlocked() {
        return () -> publishingBlocked;
    }

    public void shutdown(Closer closer) {
        closer.close("Closing publisher connection", () -> closeConnection(publishing));
        closer.close("Closing consumer connection", () -> closeConnection(consuming));
    }

    private static void closeConnection(Connection connection) throws IOException {
        if (connection != null) {
            try {
                connection.close();
            } catch (AlreadyClosedException e) {
                Log.warn("Connection already closed", e);
            }
        }
    }
}
