package io.github.zforgo.scheduler.internal.consume;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;

public class BoundedExecutorService extends AbstractExecutorService implements ExecutorService {

    private final ExecutorService delegate;
    private final Semaphore permits;
    private final String name;

    BoundedExecutorService(String name, int limit) {
        this.name = name;
        this.permits = new Semaphore(limit, true);
        this.delegate = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("job-" + name).factory());
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, @NonNull TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

    @Override
    public void execute(@NonNull Runnable task) {
        Objects.requireNonNull(task);
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
