package com.objectstore.controller.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.ChangeType;
import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.controller.replication.Monitor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The scale test (v4 §11; {@code mvn -Pscale test}, {@code make test-scale}): 500,000 chunks,
 * RF = 2, 8 workers, on {@link ClusterState} with fake workers and no HTTP.
 *
 * <ol>
 *   <li>a planner tick visits at most PLANNER_VISIT_BUDGET chunks and holds the lock at most
 *       MAX_LOCK_HOLD_MS + 5 at a time;</li>
 *   <li>DEAD of a worker holding 125,000 copies completes with no hold above MAX_LOCK_HOLD_MS + 5
 *       and heartbeat receipt p99 &lt; 50 ms meanwhile;</li>
 *   <li>a steady 5,000 changes/s for 60 s keeps the apply queue below 1,000 and the writer busy
 *       less than half the time;</li>
 *   <li>after every phase the full invariant check passes.</li>
 * </ol>
 */
@Tag("scale")
class ScaleTest {

    private static final int CHUNKS = 500_000;
    private static final int WORKERS = 8;
    private static final int PER_OBJECT = 100_000;
    private static final Duration STEADY = Duration.ofSeconds(Long.getLong("scale.steady.seconds", 60));

    private static String chunk(int i) {
        return "%08x-7d0e-4c1b-9a54-1f2e3d4c5b6a-%05d".formatted(0x10000000 + i / PER_OBJECT, i % PER_OBJECT);
    }

    private static String worker(int w) {
        return "worker" + (w + 1);
    }

    private final TestSupport t = new TestSupport(TestSupport.properties(2, 2, 15, 600, 1200, 0, 2000));
    private final ClusterState cluster = t.cluster;
    private final long maxHold = t.properties.maxLockHoldMs();

    private void assertInvariants(String phase) {
        List<String> violations = cluster.read(cluster::checkInvariants);
        assertThat(violations).as("invariants after " + phase).isEmpty();
    }

    @Test
    void halfAMillionChunks() throws Exception {
        for (int w = 0; w < WORKERS; w++) {
            t.join(worker(w));
        }
        for (int from = 0; from < CHUNKS; from += 10_000) {
            int start = from;
            cluster.write(() -> {
                for (int i = start; i < start + 10_000; i++) {
                    String chunkId = chunk(i);
                    cluster.putChunk(chunkId, new ClusterState.ChunkInfo(chunkId.substring(0, 36), i % PER_OBJECT, 10,
                            TestSupport.SUM));
                    cluster.addReplica(chunkId, worker(i % WORKERS), 0);
                    cluster.addReplica(chunkId, worker((i + 1) % WORKERS), 0);
                }
            });
        }
        assertThat(cluster.read(() -> cluster.chunksOn(worker(7)).size())).isEqualTo(CHUNKS * 2 / WORKERS);
        assertInvariants("setup");
        // The setup grows the heap to ~600 MB in about a second; the young collections that follow
        // (measured with -Xlog:gc: up to 32 ms each) would land inside the timed holds and measure the
        // setup's garbage, not the planner. Collect it once before anything is timed.
        System.gc();

        plannerTick();
        deadWorker();
        steadyChanges();
    }

    /** Phase 1: 50,000 under-replicated chunks; one planner tick. */
    private void plannerTick() throws Exception {
        for (int from = 0; from < 50_000; from += 5_000) {
            int start = from;
            cluster.write(() -> {
                for (int i = start; i < start + 5_000; i++) {
                    cluster.removeReplica(chunk(i * WORKERS), worker(0));
                }
            });
        }
        Monitor monitor = new Monitor(cluster, new SafeMode(cluster, t.properties), t.planner, t.properties);
        double maxHoldMs = 0;
        int scheduled = 0;
        for (int tick = 0; tick < 4; tick++) {                       // tick 0 also warms the JIT up
            cluster.takeLockMaxHoldMs();
            Thread planner = Thread.ofPlatform().name("planner-loop").start(monitor::tick);
            planner.join();
            assertThat(monitor.lastTick().visited()).isLessThanOrEqualTo(t.properties.plannerVisitBudget());
            scheduled += monitor.lastTick().scheduled();
            if (tick > 0) {
                maxHoldMs = Math.max(maxHoldMs, cluster.peekLockMaxHoldMs("planner-loop"));
            }
        }
        assertThat(scheduled).isPositive();
        assertThat(maxHoldMs).as("planner hold (ms)").isLessThanOrEqualTo(maxHold + 5);
        System.out.printf("[scale] planner tick: visited <= %d, longest hold %.2f ms%n",
                t.properties.plannerVisitBudget(), maxHoldMs);
        assertInvariants("the planner tick");
    }

    /** Phase 2: the worker holding 125,000 copies goes DEAD while the others keep heartbeating. */
    private void deadWorker() throws Exception {
        String dead = worker(7);
        t.writer.start();
        try {
            cluster.takeLockMaxHoldMs();
            AtomicBoolean beating = new AtomicBoolean(true);
            List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
            Thread heartbeats = heartbeats(beating, latencies);
            cluster.write(() -> cluster.worker(dead).lastSeen = cluster.now() - 3_600_000_000_000L);
            t.registry.healthPass();                                  // queues MarkDead
            long started = System.nanoTime();
            while (!cluster.read(() -> cluster.worker(dead).isDead() && !cluster.worker(dead).purging)) {
                Thread.sleep(5);
                assertThat(System.nanoTime() - started).as("the purge completes").isLessThan(120_000_000_000L);
            }
            t.writer.barrier(Duration.ofSeconds(60));
            beating.set(false);
            heartbeats.join();

            assertThat(cluster.read(() -> cluster.chunksOn(dead))).isEmpty();
            System.out.printf("[scale] DEAD purge of 125,000 copies: %.1f s, writer's longest hold %.2f ms, "
                            + "heartbeat p99 %.2f ms (%d heartbeats)%n", (System.nanoTime() - started) / 1e9,
                    cluster.peekLockMaxHoldMs(Writer.THREAD_NAME), p99(latencies), latencies.size());
            assertThat(cluster.peekLockMaxHoldMs(Writer.THREAD_NAME)).as("writer hold (ms)").isLessThanOrEqualTo(maxHold + 5);
            assertThat(p99(latencies)).as("heartbeat receipt p99 (ms) during the purge").isLessThan(50);
            assertInvariants("the DEAD purge");
        } finally {
            t.writer.stop();
        }
    }

    /** Phase 3: 5,000 changes per second for 60 s from one worker, the planner ticking meanwhile. */
    private void steadyChanges() throws Exception {
        String busy = worker(0);
        t.writer.start();
        Monitor monitor = new Monitor(cluster, new SafeMode(cluster, t.properties), t.planner, t.properties);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger maxDepth = new AtomicInteger();
        Thread planner = Thread.ofPlatform().name("planner-loop").start(() -> {
            while (running.get()) {
                monitor.tick();
                sleep(3000);
            }
        });
        Thread sampler = Thread.ofPlatform().start(() -> {
            while (running.get()) {
                maxDepth.accumulateAndGet(t.queue.depth(), Math::max);
                sleep(20);
            }
        });
        try {
            List<String> mine = new ArrayList<>(cluster.read(() -> cluster.chunksOn(busy))).subList(0, 5_000);
            t.writer.takeBusyRatio();
            long seq = cluster.read(() -> cluster.worker(busy).received);
            long hb = t.fake(busy).hbSeq;
            String boot = t.fake(busy).boot;
            long end = System.nanoTime() + STEADY.toNanos();
            long next = System.nanoTime();
            boolean deleting = true;
            int cursor = 0;
            while (System.nanoTime() < end) {
                List<Change> changes = new ArrayList<>(500);
                for (int i = 0; i < 500; i++) {
                    String chunkId = mine.get(cursor++ % mine.size());
                    if (cursor % mine.size() == 0) {
                        deleting = !deleting;
                    }
                    changes.add(deleting
                            ? Change.deleted(++seq, chunkId, seq | 1, "deleted")
                            : Change.received(++seq, chunkId, 10L, TestSupport.SUM, seq | 1, null));
                }
                long received = t.registry.heartbeat(new HeartbeatRequest(busy, boot, ++hb, InternalApi.nowSeconds(),
                        TestSupport.STATS, changes, null, null)).changesReceivedUpTo();
                assertThat(received).as("every change acknowledged").isEqualTo(seq);
                next += 100_000_000L;                                 // 500 changes per 100 ms
                long wait = next - System.nanoTime();
                if (wait > 0) {
                    Thread.sleep(wait / 1_000_000, (int) (wait % 1_000_000));
                }
            }
            double busyRatio = t.writer.takeBusyRatio();
            running.set(false);
            planner.join();
            sampler.join();
            t.writer.barrier(Duration.ofSeconds(60));

            System.out.printf("[scale] %d s at 5,000 changes/s: max apply queue depth %d, writer busy ratio %.3f%n",
                    STEADY.toSeconds(), maxDepth.get(), busyRatio);
            assertThat(maxDepth.get()).as("apply queue depth").isLessThan(1000);
            assertThat(busyRatio).as("writer busy ratio").isLessThan(0.5);
            assertInvariants("the steady load");
        } finally {
            running.set(false);
            t.writer.stop();
        }
    }

    private Thread heartbeats(AtomicBoolean beating, List<Long> latencies) {
        return Thread.ofPlatform().start(() -> {
            int round = 0;
            while (beating.get()) {
                String id = worker(round++ % (WORKERS - 1));
                TestSupport.Fake fake = t.fake(id);
                long started = System.nanoTime();
                t.registry.heartbeat(new HeartbeatRequest(id, fake.boot, ++fake.hbSeq, InternalApi.nowSeconds(),
                        TestSupport.STATS));
                latencies.add(System.nanoTime() - started);
                sleep(2);
            }
        });
    }

    private static double p99(List<Long> nanos) {
        List<Long> sorted = new ArrayList<>(nanos);
        Collections.sort(sorted);
        assertThat(sorted).hasSizeGreaterThan(20);
        return sorted.get((int) Math.ceil(sorted.size() * 0.99) - 1) / 1e6;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
