package com.objectstore.controller.cluster;

import com.objectstore.controller.config.ControllerProperties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * The bounded FIFO of {@link Task}s that the {@link Writer} applies (capacity
 * APPLY_QUEUE_CAPACITY). Putting a task on it takes no cluster lock.
 *
 * <ul>
 *   <li>{@link #offer} never blocks: the receipt step, the health loop and downloads use it while
 *       holding, or instead of waiting for, the cluster lock.</li>
 *   <li>{@link #put} waits for room: uploads, deletes, registrations and the reconcile helper,
 *       which never hold the cluster lock while they wait.</li>
 * </ul>
 */
@Component
public class ApplyQueue implements ApplySink {

    private final LinkedBlockingQueue<Task> queue = new LinkedBlockingQueue<>();
    private final Semaphore room;
    private final int capacity;

    @Autowired
    public ApplyQueue(ControllerProperties properties) {
        this(properties.applyQueueCapacity());
    }

    /** For tests. */
    public ApplyQueue(int capacity) {
        this.capacity = capacity;
        this.room = new Semaphore(capacity);
    }

    @Override
    public boolean offer(Task... tasks) {
        if (!room.tryAcquire(tasks.length)) {
            return false;
        }
        for (Task task : tasks) {
            queue.add(task);
        }
        return true;
    }

    /** Waits for room. Must not be called while holding the cluster lock. */
    public void put(Task task) throws InterruptedException {
        if (ClusterState.lockHeldByCurrentThread()) {
            throw new IllegalStateException("ApplyQueue.put while holding the cluster lock: " + task);
        }
        room.acquire();
        queue.add(task);
    }

    Task take() throws InterruptedException {
        Task task = queue.take();
        room.release();
        return task;
    }

    Task poll() {
        Task task = queue.poll();
        if (task != null) {
            room.release();
        }
        return task;
    }

    public int depth() {
        return queue.size();
    }

    public int capacity() {
        return capacity;
    }

    /** Age of the oldest queued task, in ms (0 if empty). */
    public long oldestAgeMillis() {
        Task oldest = queue.peek();
        return oldest == null ? 0 : oldest.ageMillis();
    }
}
