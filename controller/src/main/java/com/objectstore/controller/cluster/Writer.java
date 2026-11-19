package com.objectstore.controller.cluster;

import com.objectstore.controller.cluster.Task.Step;
import com.objectstore.controller.config.ControllerProperties;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * The single writer (thread {@code writer}): the only thread that applies queued {@link Task}s to
 * the block map (v4 §5.3).
 *
 * <pre>
 * loop:
 *   task = queue.take()
 *   write-lock; start = now
 *     run tasks in order; a task answering MORE is resumed (after unlock + lock, start = now)
 *     before any other task is taken: the continuation keeps its queue position;
 *     after a task, stop once MAX_LOCK_HOLD_MS has passed, or when the queue is empty
 *   a task that throws is logged and counted (writer_task_failures), its future fails, and for a
 *   CommitObject / DeleteObject the reconcile helper repairs that object's mirror
 *   unlock
 * </pre>
 *
 * The writer never touches SQLite and never takes the namespace lock. It starts before the HTTP
 * server and stops after it.
 */
@Component
public class Writer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(Writer.class);
    public static final String THREAD_NAME = "writer";
    private static volatile long delayForTestsMillis;

    private final ClusterState cluster;
    private final ApplyQueue queue;
    private final long maxHoldNanos;
    private final ObjectReconciler reconciler;
    private final AtomicLong busyNanos = new AtomicLong();
    private final AtomicLong tasksApplied = new AtomicLong();
    private volatile Thread thread;
    private volatile boolean running;
    private long busySampledAt = System.nanoTime();
    private long busySampled;

    public Writer(ClusterState cluster, ApplyQueue queue, ControllerProperties properties, ObjectReconciler reconciler) {
        this.cluster = cluster;
        this.queue = queue;
        this.maxHoldNanos = properties.maxLockHold().toNanos();
        this.reconciler = reconciler;
    }

    /** Test hook: wait this long (outside every lock) before each batch of tasks; 0 turns it off. */
    public static void delayTasksForTests(long millis) {
        delayForTestsMillis = millis;
    }

    // ---- lifecycle --------------------------------------------------------------------------

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        thread = Thread.ofPlatform().name(THREAD_NAME).daemon(true).start(this::loop);
        log.info("[writer] started (MAX_LOCK_HOLD_MS={}, APPLY_QUEUE_CAPACITY={})", maxHoldNanos / 1_000_000,
                queue.capacity());
    }

    @Override
    public synchronized void stop() {
        running = false;
        Thread current = thread;
        if (current != null) {
            current.interrupt();
            try {
                current.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        thread = null;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Early: running before the HTTP server accepts requests, stopped after it. */
    @Override
    public int getPhase() {
        return Integer.MIN_VALUE + 1000;
    }

    private void loop() {
        while (running) {
            Task task;
            try {
                task = queue.take();
            } catch (InterruptedException e) {
                return;
            }
            runBatch(task);
        }
    }

    // ---- applying ---------------------------------------------------------------------------

    /** One lock hold: {@code first}, then more queued tasks until MAX_LOCK_HOLD_MS has passed. */
    void runBatch(Task first) {
        pauseForTests();
        long busyStart = System.nanoTime();
        cluster.lockWrite();
        long start = System.nanoTime();
        try {
            Task task = first;
            while (true) {
                start = runTask(task, start);
                if (System.nanoTime() - start >= maxHoldNanos) {
                    break;
                }
                task = queue.poll();
                if (task == null) {
                    break;
                }
            }
        } finally {
            cluster.unlockWrite();
            busyNanos.addAndGet(System.nanoTime() - busyStart);
        }
    }

    /** Runs a task to completion, yielding the lock between its steps. Returns when the current hold began. */
    private long runTask(Task task, long start) {
        try {
            Step step = task.body.run(start + maxHoldNanos);
            while (step == Step.MORE) {
                cluster.unlockWrite();
                cluster.lockWrite();
                start = System.nanoTime();
                step = task.body.run(start + maxHoldNanos);
            }
            tasksApplied.incrementAndGet();
            task.future.complete(task.result);
        } catch (RuntimeException | Error e) {
            log.error("[writer] task {} failed", task.name, e);
            cluster.increment("writer_task_failures");
            task.future.completeExceptionally(e);
            if (task.reconcileObjectId != null && reconciler != null) {
                reconciler.reconcile(task.reconcileObjectId);
            }
        }
        return start;
    }

    private static void pauseForTests() {
        long delay = delayForTestsMillis;
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // ---- waiting for results ----------------------------------------------------------------

    /**
     * Waits for a queued task. Without a writer thread (unit tests) the caller applies the queue
     * itself first.
     *
     * @throws TimeoutException if it did not complete in time (it may still be applied later)
     */
    public Object await(Task task, Duration timeout) throws TimeoutException {
        if (thread == null) {
            drainInline();
        }
        try {
            return task.future.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TimeoutException("interrupted");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException(cause);
        }
    }

    /** Queues a barrier and waits until every task queued before it has been applied. */
    public void barrier(Duration timeout) throws TimeoutException, InterruptedException {
        Task barrier = Task.barrier();
        queue.put(barrier);
        await(barrier, timeout);
    }

    /** Applies everything queued, in the calling thread. Only when no writer thread runs (tests). */
    public void drainInline() {
        if (thread != null) {
            throw new IllegalStateException("the writer thread is running");
        }
        Task task;
        while ((task = queue.poll()) != null) {
            runBatch(task);
        }
    }

    /** True while the writer thread runs. */
    public boolean isThreadRunning() {
        return thread != null && running;
    }

    public long tasksApplied() {
        return tasksApplied.get();
    }

    /** Fraction of the time since the last call that the writer held the lock for tasks; resets. */
    public synchronized double takeBusyRatio() {
        long now = System.nanoTime();
        long busy = busyNanos.get();
        double ratio = now == busySampledAt ? 0 : (double) (busy - busySampled) / (now - busySampledAt);
        busySampledAt = now;
        busySampled = busy;
        return Math.round(Math.min(1, ratio) * 1000) / 1000.0;
    }
}
