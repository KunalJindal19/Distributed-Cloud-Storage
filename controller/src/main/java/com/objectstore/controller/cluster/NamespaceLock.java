package com.objectstore.controller.cluster;

import java.util.concurrent.locks.ReentrantLock;
import org.springframework.stereotype.Component;

/**
 * Held across "SQLite transaction + putting the matching task on the apply queue" for upload
 * commits and deletes (and by the reconcile helper and fsck), so the order of namespace changes in
 * the queue is the order in the database.
 *
 * <p>Lock order: never take the cluster lock while holding this one; the writer never takes it.
 */
@Component
public class NamespaceLock {

    private final ReentrantLock lock = new ReentrantLock(true);

    public void lock() {
        if (ClusterState.lockHeldByCurrentThread()) {
            throw new IllegalStateException("the namespace lock is never taken while holding the cluster lock");
        }
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public boolean isHeldByCurrentThread() {
        return lock.isHeldByCurrentThread();
    }
}
