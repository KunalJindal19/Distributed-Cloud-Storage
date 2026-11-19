package com.objectstore.controller.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.ChangeType;
import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.dto.RegisterRequest;
import com.objectstore.common.dto.ReportEntry;
import com.objectstore.common.dto.ReportSegment;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.ReplicaState;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Block 12 (v4 §4.11, §10 rows 1, 19, 21, 36): a property test of full reports interleaved with
 * the change stream. A simulated worker writes, deletes and corrupts chunks before and during a
 * report; its changes and its report segments reach the controller over heartbeats and PUT
 * answers in a random interleaving, with lost requests, lost replies, resent and reordered
 * heartbeats. After the report completes and the remaining changes are delivered, the block map
 * for that worker must equal its disk exactly.
 */
class ReportInterleavingTest {

    private static final int RUNS = 10_000;
    private static final int CHUNKS = 8;

    /** The worker side: a disk, a change log with hold-back, and a report in progress. */
    private static final class SimWorker {
        final TreeMap<String, Boolean> disk = new TreeMap<>();     // chunk -> known bad
        final List<Change> log = new ArrayList<>();                // un-acknowledged changes
        long lastSeq;
        long acked;
        long holdLimit = Long.MAX_VALUE;
        long hbSeq = -1;
        // the report in progress
        String reportId;
        long baseline;
        List<ReportEntry> listing;
        int segmentSize;
        int index;
        boolean reporting;

        void append(ChangeType type, String chunkId) {
            log.add(new Change(++lastSeq, type, chunkId, type == ChangeType.RECEIVED ? 10L : null,
                    type == ChangeType.RECEIVED ? TestSupport.SUM : null, null, null, null));
        }

        void write(String chunkId) {
            disk.put(chunkId, false);
            append(ChangeType.RECEIVED, chunkId);
        }

        void delete(String chunkId) {
            disk.remove(chunkId);
            append(ChangeType.DELETED, chunkId);                    // logged even if absent
        }

        void corrupt(String chunkId) {
            if (disk.containsKey(chunkId) && !disk.get(chunkId)) {
                disk.put(chunkId, true);
                append(ChangeType.BAD, chunkId);
            }
        }

        void ack(long n) {
            if (n > lastSeq) {
                return;
            }
            log.removeIf(change -> change.seq() <= n);
            acked = Math.max(acked, n);
        }

        List<Change> suffix(long after, int max) {
            List<Change> run = new ArrayList<>();
            for (Change change : log) {
                if (run.size() >= max || change.seq() > holdLimit) {
                    break;
                }
                if (change.seq() > after) {
                    run.add(change);
                }
            }
            return run;
        }

        void beginReport(Random random) {
            holdLimit = lastSeq;
            baseline = lastSeq;
            reportId = UUID.randomUUID().toString();
            index = 0;
            segmentSize = 1 + random.nextInt(4);
            reporting = true;
        }

        void takeListing() {
            listing = new ArrayList<>();
            disk.forEach((chunkId, bad) -> listing.add(new ReportEntry(chunkId, 10, TestSupport.SUM, bad)));
        }

        int segmentCount() {
            return Math.max(1, (listing.size() + segmentSize - 1) / segmentSize);
        }

        ReportSegment segment() {
            int from = Math.min(listing.size(), index * segmentSize);
            int to = Math.min(listing.size(), from + segmentSize);
            return new ReportSegment(reportId, index, index == segmentCount() - 1, baseline, List.copyOf(listing.subList(from, to)));
        }
    }

    private static String chunk(int i) {
        return TestSupport.chunk(i);
    }

    @Test
    void edge21_afterAnyInterleavingTheBlockMapEqualsTheDisk() {
        for (int run = 0; run < RUNS; run++) {
            try {
                oneRun(new Random(run));
            } catch (AssertionError | RuntimeException e) {
                throw new AssertionError("run " + run + " failed: " + e.getMessage(), e);
            }
        }
    }

    private void oneRun(Random random) {
        TestSupport t = new TestSupport();
        ClusterState cluster = t.cluster;
        for (int i = 0; i < CHUNKS; i++) {
            t.knowChunk(chunk(i));
        }
        SimWorker w = new SimWorker();
        for (int i = 0; i < CHUNKS; i++) {                         // the disk at process start
            if (random.nextBoolean()) {
                w.disk.put(chunk(i), random.nextInt(5) == 0);
            }
        }
        String boot = "boot-" + random.nextLong();
        t.registry.register(new RegisterRequest("worker1", "http://worker1:8001", boot, "cluster-1", 1, 0, -1,
                InternalApi.nowSeconds(), TestSupport.STATS));

        // A stale view first: the controller believes in a random set of copies.
        List<ReportEntry> stale = new ArrayList<>();
        for (int i = 0; i < CHUNKS; i++) {
            if (random.nextBoolean()) {
                stale.add(new ReportEntry(chunk(i), 10, TestSupport.SUM, random.nextInt(4) == 0));
            }
        }
        heartbeat(t, w, boot, List.of(), new ReportSegment("stale", 0, true, 0, stale), true);

        // Phase A: changes before the report, partly delivered (with losses).
        for (int step = random.nextInt(12); step > 0; step--) {
            randomOp(random, w);
            if (random.nextInt(3) == 0) {
                deliver(random, t, w, boot);
            }
        }

        // Phase B: the report starts (baseline M); more changes happen before and after the listing.
        w.beginReport(random);
        for (int step = random.nextInt(4); step > 0; step--) {
            randomOp(random, w);
        }
        w.takeListing();

        // Phase C: everything interleaved until the report is done and every change acknowledged.
        int opsLeft = random.nextInt(10);
        int guard = 0;
        while (w.reporting || w.acked < w.lastSeq || opsLeft > 0) {
            if (++guard > 2_000) {
                throw new AssertionError("no progress");
            }
            if (opsLeft > 0 && random.nextInt(3) == 0) {
                randomOp(random, w);
                opsLeft--;
            } else {
                deliver(random, t, w, boot);
            }
        }
        // One clean heartbeat at the end (nothing pending).
        heartbeat(t, w, boot, w.suffix(w.acked, Integer.MAX_VALUE), null, true);

        // The block map for the worker equals the disk.
        cluster.locked(() -> {
            assertThat(cluster.worker("worker1").session).isNull();
            assertThat(new TreeMap<>(stateOnController(cluster))).isEqualTo(w.disk);
            assertThat(cluster.checkInvariants()).isEmpty();
        });
    }

    private static Map<String, Boolean> stateOnController(ClusterState cluster) {
        Map<String, Boolean> state = new HashMap<>();
        for (String chunkId : cluster.chunksOn("worker1")) {
            Replica replica = cluster.replica(chunkId, "worker1");
            state.put(chunkId, replica.state == ReplicaState.CORRUPT);
        }
        return state;
    }

    private static void randomOp(Random random, SimWorker w) {
        String chunkId = chunk(random.nextInt(CHUNKS));
        switch (random.nextInt(3)) {
            case 0 -> w.write(chunkId);
            case 1 -> w.delete(chunkId);
            default -> w.corrupt(chunkId);
        }
    }

    /** One delivery attempt on a random channel: a heartbeat (possibly resent or reordered) or a PUT answer. */
    private static void deliver(Random random, TestSupport t, SimWorker w, String boot) {
        if (random.nextInt(4) == 0) {
            // A PUT answer: the controller sends its "received"; the worker answers with what follows.
            long received = t.cluster.locked(() -> t.cluster.worker("worker1").received);
            w.ack(received);
            List<Change> items = w.suffix(received, 1 + random.nextInt(3));
            if (random.nextInt(5) != 0) {                          // the answer may be lost
                t.receipts.acceptPiggyback("worker1", boot, items);
            }
            return;
        }
        List<Change> changes = w.suffix(w.acked, 1 + random.nextInt(4));
        ReportSegment segment = w.reporting ? w.segment() : null;
        boolean requestLost = random.nextInt(8) == 0;
        boolean replyLost = random.nextInt(5) == 0;
        if (!requestLost) {
            heartbeat(t, w, boot, changes, segment, !replyLost);
            if (random.nextInt(6) == 0) {                          // the same heartbeat again, delivered late
                HeartbeatResponse again = t.registry.heartbeat(new HeartbeatRequest("worker1", boot, w.hbSeq,
                        InternalApi.nowSeconds(), TestSupport.STATS, changes, segment));
                assertThat(again.ignored()).isEqualTo("duplicate");
            }
        } else {
            w.hbSeq++;
        }
    }

    private static void heartbeat(TestSupport t, SimWorker w, String boot, List<Change> changes, ReportSegment segment,
                                  boolean replyArrives) {
        HeartbeatResponse response = t.registry.heartbeat(new HeartbeatRequest("worker1", boot, ++w.hbSeq,
                InternalApi.nowSeconds(), TestSupport.STATS, changes, segment));
        if (!replyArrives) {
            return;
        }
        w.ack(response.changesReceivedUpTo());
        if (w.reporting && segment != null && response.segmentAck() != null
                && response.segmentAck().reportId().equals(w.reportId) && response.segmentAck().index() == w.index) {
            if (w.index == w.segmentCount() - 1) {
                w.reporting = false;
                w.holdLimit = Long.MAX_VALUE;
            } else {
                w.index++;
            }
        }
    }
}
