package com.objectstore.controller.cluster;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * The {@code resend-timer} thread (v5 §3.3): holds the resend deadlines of handed-out ops in a
 * {@link DelayQueue}; when one passes it queues {@code OpDeadline(chunk, worker, version)} for the
 * writer, which moves the op back to WAITING (same version) if it is still unanswered. Resend timers
 * only drive retries: no deadline decides correctness, versions do. The timer never touches cluster
 * state itself, and a deadline whose op has meanwhile ended or changed does nothing.
 */
@Component
public class ResendTimer implements SmartLifecycle, ClusterState.DeadlineScheduler {

    private static final Logger log = LoggerFactory.getLogger(ResendTimer.class);

    private record Deadline(String chunkId, String workerId, long version, long at) implements Delayed {
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

    public ResendTimer(ClusterState cluster, ApplyQueue queue) {
        this.cluster = cluster;
        this.queue = queue;
        cluster.setDeadlineScheduler(this);
    }

    @Override
    public void opDeadline(String chunkId, String workerId, long version, long atNanos) {
        deadlines.add(new Deadline(chunkId, workerId, version, atNanos));
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
        thread = Thread.ofPlatform().name("resend-timer").daemon(true).start(this::loop);
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
                queue.put(Task.of("OpDeadline", () -> cluster.ops().deadline(due.chunkId(), due.workerId(), due.version(),
                        cluster.now())));
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                log.error("[writer] resend-timer failed to queue a deadline", e);
            }
        }
    }
}
