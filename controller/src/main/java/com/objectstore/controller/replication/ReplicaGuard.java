package com.objectstore.controller.replication;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.springframework.stereotype.Component;

/**
 * The one lock that serialises every change to existing copy records ({@code chunk_replicas}) and
 * the worker-side action that belongs to it: repairing, trimming, purging, scrubbing, marking a
 * copy bad after a failed read, and deleting an object.
 *
 * <p>Holding it across "check the database, act on the worker, update the database" is what makes
 * those steps atomic with respect to each other. For example the repair loop can re-check, under
 * the lock, that a target still lacks a chunk before writing it, and the scrubber can be sure a
 * file it is about to remove as an orphan is not a repair copy whose record is about to appear.
 *
 * <p>Uploads do not take it: they create brand-new chunks nobody else can know about yet (see
 * {@code InFlightUploads} for how the scrubber is kept away from those). The lock is fair, so a
 * client's delete is not starved by background work.
 */
@Component
public class ReplicaGuard {

    private final ReentrantLock lock = new ReentrantLock(true);

    public <T> T call(Supplier<T> action) {
        lock.lock();
        try {
            return action.get();
        } finally {
            lock.unlock();
        }
    }

    public void run(Runnable action) {
        lock.lock();
        try {
            action.run();
        } finally {
            lock.unlock();
        }
    }
}
