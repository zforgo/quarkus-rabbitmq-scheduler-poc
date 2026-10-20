package io.github.zforgo.scheduler.internal.consume;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;

public class BoundedExecutor implements Executor, AutoCloseable {

    private final ExecutorService delegate;
    private final Semaphore permits;
    private final String name;

    BoundedExecutor(String name, int limit) {
        this.name = name;
        this.permits = new Semaphore(limit,  true);
        this.delegate = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("job-" + name + "-", 0).factory());
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }

    @Override
    public void execute(Runnable task) {
        if (!permits.tryAcquire()) {
            throw new RejectedExecutionException("Job group '" + name + "' overloaded");
        }
        try {
            delegate.execute(() -> {
                try {
                    task.run();
                } finally {
                    permits.release();
                }
            });
        } catch (RejectedExecutionException e) { // delegate shut down
            permits.release();
            throw e;
        }
    }
}
