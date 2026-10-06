package io.github.zforgo.scheduler;

public interface SelfContainedJob extends ScheduledJob<Void> {

    void process();

    @Override
    default void process(Void argument) {
        process();
    }

}
