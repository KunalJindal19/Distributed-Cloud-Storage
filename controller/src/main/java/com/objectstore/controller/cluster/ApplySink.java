package com.objectstore.controller.cluster;

/**
 * Where the receipt step puts work for the block map: the {@link ApplyQueue}. {@link #offer} is
 * atomic (all the tasks are queued, or none) and never blocks, so the receipt step can refuse to
 * acknowledge what it cannot queue.
 */
public interface ApplySink {

    /** Queues all of {@code tasks}, in order, or none of them if there is not room for all. */
    boolean offer(Task... tasks);
}
