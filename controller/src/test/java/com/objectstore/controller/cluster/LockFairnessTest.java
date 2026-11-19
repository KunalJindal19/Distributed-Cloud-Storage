package com.objectstore.controller.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.ChangeType;
import com.objectstore.common.dto.HeartbeatRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Block 13 (v4 §5.1, §5.11, §10 row 29): while the writer applies 10⁶ changes, heartbeats are
 * received quickly (p99 &lt; 50 ms), readers proceed, and no write hold of the writer exceeds
 * MAX_LOCK_HOLD_MS + 5.
 */
class LockFairnessTest {

    private static final int CHANGES = 1_000_000;
    private static final int PER_MESSAGE = 2000;
    private static final int CHUNKS = 10_000;

    @Test
    void edge29_heartbeatsAndReadersAreNotStarvedByTheWriter() throws Exception {
        TestSupport t = new TestSupport();
        ClusterState cluster = t.cluster;
        for (int i = 0; i < CHUNKS; i++) {
            t.knowChunk(TestSupport.chunk(i));
        }
        String busy = t.register("worker1");
        String quiet = t.register("worker2");
        t.writer.start();
        try {
            cluster.takeLockMaxHoldMs();
            AtomicBoolean feeding = new AtomicBoolean(true);
            List<Long> heartbeatNanos = Collections.synchronizedList(new ArrayList<>());
            AtomicLong reads = new AtomicLong();
            Thread heartbeats = Thread.ofPlatform().start(() -> {
                long seq = t.fake("worker2").hbSeq;
                while (feeding.get()) {
                    long started = System.nanoTime();
                    t.registry.heartbeat(new HeartbeatRequest("worker2", quiet, ++seq, InternalApi.nowSeconds(), TestSupport.STATS));
                    heartbeatNanos.add(System.nanoTime() - started);
                    try {
                        Thread.sleep(2);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            });
            Thread readers = Thread.ofPlatform().start(() -> {
                int i = 0;
                while (feeding.get()) {
                    String chunkId = TestSupport.chunk(i++ % CHUNKS);
                    cluster.read(() -> cluster.readable(chunkId));
                    reads.incrementAndGet();
                }
            });

            long seq = 0;
            long hb = t.fake("worker1").hbSeq;
            for (int sent = 0; sent < CHANGES; sent += PER_MESSAGE) {
                List<Change> changes = new ArrayList<>(PER_MESSAGE);
                for (int i = 0; i < PER_MESSAGE; i++) {
                    long n = ++seq;
                    String chunkId = TestSupport.chunk((int) (n % CHUNKS));
                    changes.add(n % 2 == 1
                            ? new Change(n, ChangeType.RECEIVED, chunkId, 10L, TestSupport.SUM, null, null, null)
                            : new Change(n, ChangeType.DELETED, chunkId, null, null, null, null, null));
                }
                long received;
                do {   // a full queue acknowledges nothing: the worker resends
                    received = t.registry.heartbeat(new HeartbeatRequest("worker1", busy, ++hb, InternalApi.nowSeconds(),
                            TestSupport.STATS, changes, null)).changesReceivedUpTo();
                } while (received < seq);
            }
            t.writer.barrier(Duration.ofSeconds(60));
            long readsWhileBusy = reads.get();
            feeding.set(false);
            heartbeats.join();
            readers.join();

            assertThat(cluster.read(() -> cluster.worker("worker1").applied)).isEqualTo(CHANGES);
            List<Long> sorted = new ArrayList<>(heartbeatNanos);
            Collections.sort(sorted);
            long p99 = sorted.get((int) Math.floor(sorted.size() * 0.99) - 1);
            assertThat(sorted).hasSizeGreaterThan(20);
            assertThat(p99 / 1e6).as("heartbeat receipt p99 (ms)").isLessThan(50);
            assertThat(readsWhileBusy).as("readers proceeded").isPositive();
            assertThat(cluster.peekLockMaxHoldMs(Writer.THREAD_NAME)).as("the writer's longest hold (ms)")
                    .isLessThanOrEqualTo(t.properties.maxLockHoldMs() + 5);
            t.assertNoViolations();
        } finally {
            t.writer.stop();
        }
    }
}
