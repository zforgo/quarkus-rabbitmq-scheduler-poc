package io.github.zforgo.scheduler.internal;

import java.io.IOException;

public final class Closer {

    private Exception failure;

    public void close(String what, ThrowingRunnable action) {
        try {
            action.run();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            var wrapped = new IOException("Failed to close " + what, e);
            if (failure == null) {
                failure = wrapped;
            } else {
                failure.addSuppressed(wrapped);
            }
        }
    }

    void throwIfFailed() throws Exception {
        if (failure != null) {
            throw failure;
        }
    }

    @FunctionalInterface
    public interface ThrowingRunnable {

        void run() throws Exception;
    }
}
