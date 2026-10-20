package io.github.zforgo.scheduler.internal;

import io.github.zforgo.scheduler.internal.consume.WorkerManager;
import io.github.zforgo.scheduler.internal.transport.BrokerConnections;

//Temporary class to separate consuming functions
final class ConsumerEngine {

    private final BrokerConnections connections;
    private final WorkerManager workerManager;

    ConsumerEngine(BrokerConnections connections, WorkerManager workerManager) {
        this.connections = connections;
        this.workerManager = workerManager;
    }

    void start() {
        connections.open();
        workerManager.start(connections.consuming()); // last

    }
}
