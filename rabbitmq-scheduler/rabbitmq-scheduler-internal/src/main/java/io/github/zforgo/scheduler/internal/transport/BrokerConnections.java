package io.github.zforgo.scheduler.internal.transport;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import io.github.zforgo.scheduler.SchedulerException;

public final class BrokerConnections implements AutoCloseable {

    private final ConnectionFactory factory;
    private volatile Connection publishing;
    private volatile Connection consuming;

    public BrokerConnections(ConnectionFactory factory) {
        this.factory = factory;

    }

    //TODO open necessary connection(s) only
    public void open() {
        try {
            publishing = factory.newConnection("scheduler-publisher");
            consuming = factory.newConnection("scheduler-consumer");
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

    @Override
    public void close() throws Exception {
        //TODO better implementation w/safeClose and throwingCollector
        publishing.close();
        consuming.close();
    }
}
