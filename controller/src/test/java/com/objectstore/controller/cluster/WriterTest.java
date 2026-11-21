package com.objectstore.controller.cluster;

import static com.objectstore.controller.cluster.TestSupport.chunk;
import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.ChangeType;
import com.objectstore.controller.cluster.Task.Step;
import com.objectstore.controller.domain.Chunk;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Block 13 (v4 §5.2, §5.3, §5.9, §5.11): the single writer and its queue. */
class WriterTest {

    private static final Duration WAIT = Duration.ofSeconds(10);

    private final TestSupport t = new TestSupport();
    private final ClusterState cluster = t.cluster;

    @BeforeEach
    void start() {
        t.writer.start();
    }

    @AfterEach
    void stop() {
        t.writer.stop();
    }

    @Test
    void tasksAreAppliedInQueueOrder() throws Exception {
        List<Integer> order = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < 1000; i++) {
            int n = i;
            t.queue.put(Task.of("N" + i, () -> order.add(n)));
        }
        t.writer.barrier(WAIT);

        assertThat(order).hasSize(1000).isSorted();
        assertThat(t.writer.tasksApplied()).isEqualTo(1001);   // and the barrier
    }

    @Test
    void edge5_aTaskForAnEndedEpochIsDropped() throws Exception {
        t.knowChunk(chunk(0));
        t.register("worker1");
        long oldEpoch = cluster.read(() -> cluster.worker("worker1").epoch);
        t.reRegister("worker1");                                // a new epoch

        t.queue.put(t.applier.applyChangesTask("worker1", oldEpoch, List.of(
                Change.received(1, chunk(0), 10L, TestSupport.SUM, 3, null))));
        t.writer.barrier(WAIT);

        assertThat(cluster.read(() -> cluster.chunksOn("worker1"))).isEmpty();
        assertThat(cluster.counter("stale_tasks_dropped")).isEqualTo(1);
    }

    @Test
    void edge6_aContinuationRunsBeforeTheNextTaskAndReleasesTheLockBetweenSteps() throws Exception {
        List<String> order = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger steps = new AtomicInteger();
        AtomicInteger readsDuring = new AtomicInteger();
        AtomicBoolean longTaskRunning = new AtomicBoolean(true);
        Thread reader = Thread.ofPlatform().start(() -> {
            while (longTaskRunning.get()) {
                cluster.read(() -> {
                    if (steps.get() > 0 && steps.get() < 5) {
                        readsDuring.incrementAndGet();
                    }
                });
            }
        });
        Task.Body body = deadline -> {
            int step = steps.incrementAndGet();
            order.add("long-" + step);
            long until = System.nanoTime() + 2_000_000;           // 2 ms of work per step
            while (System.nanoTime() < until) {
                Thread.onSpinWait();
            }
            return step < 5 ? Step.MORE : Step.DONE;
        };
        t.queue.put(Task.steps("Long", body));
        t.queue.put(Task.of("Next", () -> order.add("next")));
        t.writer.barrier(WAIT);
        longTaskRunning.set(false);
        reader.join();

        assertThat(order).containsExactly("long-1", "long-2", "long-3", "long-4", "long-5", "next");
        assertThat(readsDuring.get()).as("readers got the lock between the steps").isPositive();
    }

    @Test
    void edge31_aFailingTaskDoesNotStopTheWriterAndAFailedNamespaceTaskIsReconciled() throws Exception {
        String object = "eeeeeeee-7d0e-4c1b-9a54-1f2e3d4c5b6a";
        List<Chunk> rows = List.of(new Chunk(object + "-00000", object, 0, 10, TestSupport.SUM),
                new Chunk(object + "-00001", object, 1, 10, TestSupport.SUM));
        List<String> readerThreads = Collections.synchronizedList(new ArrayList<>());
        t.namespace.put(object, rows);
        ObjectReconciler reconciler = new ObjectReconciler(t.namespaceLock, t.queue, t.namespaceTasks, objectId -> {
            readerThreads.add(Thread.currentThread().getName() + (ClusterState.lockHeldByCurrentThread() ? "+lock" : ""));
            return t.namespace.get(objectId);
        });
        Writer writer = new Writer(cluster, t.queue, t.properties, reconciler);
        t.writer.stop();
        writer.start();
        try {
            cluster.write(() -> cluster.beginUpload(object));
            // A CommitObject that puts the first chunk and then throws.
            writer.barrier(WAIT);
            t.queue.put(Task.namespace("CommitObject", object, deadline -> {
                cluster.putChunk(rows.get(0).chunkId(), new ClusterState.ChunkInfo(object, 0, 10, TestSupport.SUM));
                throw new IllegalStateException("boom");
            }));
            AtomicBoolean after = new AtomicBoolean();
            t.queue.put(Task.of("After", () -> after.set(true)));
            org.awaitility.Awaitility.await().atMost(WAIT).untilAsserted(() ->
                    assertThat(cluster.counter("namespace_reconciled")).isEqualTo(1));
            writer.barrier(WAIT);

            assertThat(after).as("the next task still ran").isTrue();
            assertThat(cluster.counter("writer_task_failures")).isEqualTo(1);
            cluster.read(() -> {
                assertThat(cluster.chunk(rows.get(0).chunkId())).isNotNull();
                assertThat(cluster.chunk(rows.get(1).chunkId())).as("added by ApplyObjectMirror").isNotNull();
                assertThat(cluster.pendingUploads()).doesNotContain(object);
            });
            assertThat(readerThreads).singleElement().satisfies(name -> {
                assertThat(name).as("the namespace is read by the helper, not the writer, and without the cluster lock")
                        .startsWith("reconcile-").doesNotContain("+lock");
            });
        } finally {
            writer.stop();
        }
    }

    @Test
    void aMirrorReconciliationRemovesChunksTheNamespaceNoLongerHas() throws Exception {
        String object = "ffffffff-7d0e-4c1b-9a54-1f2e3d4c5b6a";
        t.knowChunk(chunk(0));
        t.join("worker1");
        cluster.write(() -> {
            cluster.putChunk(object + "-00000", new ClusterState.ChunkInfo(object, 0, 10, TestSupport.SUM));
            cluster.addReplica(object + "-00000", "worker1", 0);
        });

        t.queue.put(t.namespaceTasks.applyObjectMirror(object, List.of()));   // deleted in the database
        t.writer.barrier(WAIT);

        cluster.read(() -> {
            assertThat(cluster.chunk(object + "-00000")).isNull();
            assertThat(cluster.isGarbage(object)).isTrue();
            assertThat(cluster.worker("worker1").commands).singleElement()
                    .satisfies(c -> assertThat(c.chunkId()).isEqualTo(object + "-00000"));
        });
        t.assertNoViolations();
    }
}
