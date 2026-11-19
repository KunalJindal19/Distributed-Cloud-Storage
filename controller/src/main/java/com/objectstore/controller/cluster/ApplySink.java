package com.objectstore.controller.cluster;

/**
 * Where the receipt step puts work for the block map. In Block 12 the work is applied at once,
 * under the cluster lock the receipt step already holds ({@link #INLINE}); the single writer and
 * its bounded queue replace it later. {@link #hasRoom} lets the receipt step refuse to acknowledge
 * what it cannot queue.
 */
public interface ApplySink {

    /** True if {@code tasks} more tasks can be accepted right now. */
    boolean hasRoom(int tasks);

    /** Accepts a task; only called after {@link #hasRoom} said yes. */
    void submit(String name, Runnable task);

    ApplySink INLINE = new ApplySink() {
        @Override
        public boolean hasRoom(int tasks) {
            return true;
        }

        @Override
        public void submit(String name, Runnable task) {
            task.run();
        }
    };
}
