package com.objectstore.controller.cluster;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * The {@code pending-timer} thread (v4 §7.4): holds the deadlines of pending copies and of
 * delivered ledger DELETEs in a {@link DelayQueue}; when one passes it queues
 * {@code ExpirePending(id)} or {@code LedgerDeadline(chunk, worker)} for the writer. It never touches
 * cluster state itself, and a deadline whose copy or entry has meanwhile ended does nothing.
 */
@Component
public class PendingTimer implements SmartLifecycle, ClusterState.DeadlineScheduler {

    private static final Logger log = LoggerFactory.getLogger(PendingTimer.class);

    private record Deadline(String kind, long id, String chunkId, String workerId, long at) implements Delayed {
        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(at - System.nanoTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            return Long.compare(at, ((Deadline) other).at);
        }
    }

    private final ClusterState cluster;
    private final ApplyQueue queue;
    private final DelayQueue<Deadline> deadlines = new DelayQueue<>();
    private volatile Thread thread;
    private volatile boolean running;

    public PendingTimer(ClusterState cluster, ApplyQueue queue) {
        this.cluster = cluster;
        this.queue = queue;
        cluster.setDeadlineScheduler(this);
    }

    @Override
    public void pendingCopy(long id, long atNanos) {
        deadlines.add(new Deadline("pending", id, null, null, atNanos));
    }

    @Override
    public void ledgerEntry(String chunkId, String workerId, long atNanos) {
        deadlines.add(new Deadline("ledger", 0, chunkId, workerId, atNanos));
    }

    public int size() {
        return deadlines.size();
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        thread = Thread.ofPlatform().name("pending-timer").daemon(true).start(this::loop);
    }

    @Override
    public synchronized void stop() {
        running = false;
        if (thread != null) {
            thread.interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void loop() {
        while (running) {
            try {
                Deadline due = deadlines.take();
                Task task = "pending".equals(due.kind())
                        ? Task.of("ExpirePending", () -> cluster.expirePending(due.id()))
                        : Task.of("LedgerDeadline", () -> cluster.ledger().deadline(due.chunkId(), due.workerId(), cluster.now()));
                queue.put(task);
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                log.error("[writer] pending-timer failed to queue a deadline", e);
            }
        }
    }
}
