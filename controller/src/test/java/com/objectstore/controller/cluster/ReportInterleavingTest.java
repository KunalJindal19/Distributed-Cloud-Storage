package com.objectstore.controller.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.dto.RegisterRequest;
import com.objectstore.common.dto.RegisterResponse;
import com.objectstore.common.dto.ReportEntry;
import com.objectstore.common.dto.ReportSegment;
import com.objectstore.common.dto.Resync;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A property test of fenced resyncs interleaved with the change stream (v5 §5.3–§5.5). A simulated
 * worker writes, deletes and quarantines chunks before and during a resync; it asks for the resync,
 * adopts the {@code (t0, attempt)} the controller hands it, holds back everything above its baseline
 * M, and lists its disk, possibly with post-M state already in the listing. Changes and segments
 * reach the controller over heartbeats and PUT answers in a random interleaving, with lost requests,
 * lost replies, resent and reordered heartbeats. After the resync completes and the remaining changes
 * are delivered, the block map for that worker must equal its disk exactly.
 */
class ReportInterleavingTest {

    private static final int RUNS = 10_000;
    private static final int CHUNKS = 8;

    /** The worker side: a disk, a change log with hold-back, and a listing in progress. */
    private static final class SimWorker {
        final TreeSet<String> disk = new TreeSet<>();
        final List<Change> log = new ArrayList<>();                // un-acknowledged changes
        long lastSeq;
        long acked;
        long holdLimit = Long.MAX_VALUE;
        long hbSeq = -1;
        long nextLocal = 1;                                        // odd versions of local observations
        // the resync
        long fence;
        int attempt;
        boolean wantResync;
        boolean reporting;                                         // a hold is in place
        String reportId;
        long baseline;
        List<ReportEntry> listing;                                 // null until the listing is taken
        int segmentSize;
        int index;

        private long local() {
            nextLocal += 2;
            return Math.max(nextLocal, fence | 1);
        }

        void write(String chunkId) {
            disk.add(chunkId);
            log.add(Change.received(++lastSeq, chunkId, 10L, TestSupport.SUM, local(), null));
        }

        void delete(String chunkId) {
            disk.remove(chunkId);
            log.add(Change.deleted(++lastSeq, chunkId, local(), "deleted"));    // logged even if absent
        }

        void quarantine(String chunkId) {
            if (disk.remove(chunkId)) {
                log.add(Change.deleted(++lastSeq, chunkId, local(), "corrupt"));
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

        /** A resync {t0, a} arrived: a new fence or a newer attempt starts a new hold (v5 §5.3). */
        void resync(Resync resync, Random random) {
            if (resync == null || !(resync.t0() > fence || (resync.t0() == fence && resync.attempt() > attempt))) {
                return;
            }
            fence = resync.t0();
            attempt = resync.attempt();
            wantResync = false;
            holdLimit = lastSeq;
            baseline = lastSeq;
            reportId = UUID.randomUUID().toString();
            index = 0;
            segmentSize = 1 + random.nextInt(4);
            reporting = true;
            listing = null;                                       // taken a little later: post-M state may show
        }

        void takeListing() {
            listing = new ArrayList<>();
            disk.forEach(chunkId -> listing.add(new ReportEntry(chunkId, 10, TestSupport.SUM, 1)));
        }

        int segmentCount() {
            return Math.max(1, (listing.size() + segmentSize - 1) / segmentSize);
        }

        ReportSegment segment() {
            int from = Math.min(listing.size(), index * segmentSize);
            int to = Math.min(listing.size(), from + segmentSize);
            return new ReportSegment(reportId, fence, attempt, index, index == segmentCount() - 1, baseline,
                    List.copyOf(listing.subList(from, to)));
        }

        void finish() {
            reporting = false;
            listing = null;
            holdLimit = Long.MAX_VALUE;
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
                w.disk.add(chunk(i));
            }
        }
        String boot = "boot-" + random.nextLong();
        RegisterResponse registered = t.registry.register(new RegisterRequest("worker1", "http://worker1:8001", boot,
                "cluster-1", 1, 0, -1, InternalApi.nowSeconds(), TestSupport.STATS));

        // A stale view first: the registration listing names a random set of copies.
        w.fence = registered.floor();
        w.attempt = registered.attempt();
        List<ReportEntry> stale = new ArrayList<>();
        for (int i = 0; i < CHUNKS; i++) {
            if (random.nextBoolean()) {
                stale.add(new ReportEntry(chunk(i), 10, TestSupport.SUM, 1));
            }
        }
        heartbeat(t, w, boot, List.of(), new ReportSegment("stale", w.fence, w.attempt, 0, true, 0, stale), true, random);
        t.sync();

        // Phase A: changes before the resync, partly delivered (with losses).
        for (int step = random.nextInt(12); step > 0; step--) {
            randomOp(random, w);
            if (random.nextInt(3) == 0) {
                deliver(random, t, w, boot);
            }
        }

        // Phase B and C: the worker asks for a resync; changes go on before and after the hold and the
        // listing; everything interleaves until the resync is done and every change acknowledged.
        w.wantResync = true;
        int opsLeft = 2 + random.nextInt(10);
        int guard = 0;
        while (w.wantResync || w.reporting || w.acked < w.lastSeq || opsLeft > 0) {
            if (++guard > 3_000) {
                throw new AssertionError("no progress");
            }
            if (w.reporting && w.listing == null && random.nextInt(3) == 0) {
                w.takeListing();
            } else if (opsLeft > 0 && random.nextInt(3) == 0) {
                randomOp(random, w);
                opsLeft--;
            } else {
                deliver(random, t, w, boot);
            }
        }
        heartbeat(t, w, boot, w.suffix(w.acked, Integer.MAX_VALUE), null, true, random);
        t.sync();

        cluster.locked(() -> {
            assertThat(cluster.worker("worker1").session).isNull();
            assertThat(cluster.worker("worker1").resyncing).isFalse();
            assertThat(new TreeSet<>(cluster.chunksOn("worker1"))).isEqualTo(w.disk);
            assertThat(cluster.checkInvariants()).isEmpty();
        });
    }

    private static void randomOp(Random random, SimWorker w) {
        String chunkId = chunk(random.nextInt(CHUNKS));
        switch (random.nextInt(3)) {
            case 0 -> w.write(chunkId);
            case 1 -> w.delete(chunkId);
            default -> w.quarantine(chunkId);
        }
    }

    /** One delivery attempt on a random channel: a heartbeat (possibly resent or reordered) or a PUT answer. */
    private static void deliver(Random random, TestSupport t, SimWorker w, String boot) {
        if (random.nextInt(4) == 0) {
            long received = t.cluster.locked(() -> t.cluster.worker("worker1").received);
            w.ack(received);
            List<Change> items = w.suffix(received, 1 + random.nextInt(3));
            if (random.nextInt(5) != 0) {                          // the answer may be lost
                t.receipts.acceptPiggyback("worker1", boot, items);
            }
            maybeApply(random, t);
            return;
        }
        List<Change> changes = w.suffix(w.acked, 1 + random.nextInt(4));
        ReportSegment segment = w.reporting && w.listing != null ? w.segment() : null;
        boolean requestLost = random.nextInt(8) == 0;
        boolean replyLost = random.nextInt(5) == 0;
        if (!requestLost) {
            heartbeat(t, w, boot, changes, segment, !replyLost, random);
            if (random.nextInt(6) == 0) {                          // the same heartbeat again, delivered late
                HeartbeatResponse again = t.registry.heartbeat(new HeartbeatRequest("worker1", boot, w.hbSeq,
                        InternalApi.nowSeconds(), TestSupport.STATS, changes, segment, null));
                assertThat(again.ignored()).isEqualTo("duplicate");
            }
        } else {
            w.hbSeq++;
        }
        maybeApply(random, t);
    }

    /** The writer applies the queue at random moments, so acceptance and application interleave too. */
    private static void maybeApply(Random random, TestSupport t) {
        if (random.nextBoolean()) {
            t.sync();
        }
    }

    private static void heartbeat(TestSupport t, SimWorker w, String boot, List<Change> changes, ReportSegment segment,
                                  boolean replyArrives, Random random) {
        String wanted = w.wantResync ? InternalApi.RESYNC_TOMBSTONES : null;
        HeartbeatResponse response = t.registry.heartbeat(new HeartbeatRequest("worker1", boot, ++w.hbSeq,
                InternalApi.nowSeconds(), TestSupport.STATS, changes, segment, wanted));
        if (!replyArrives) {
            return;
        }
        w.ack(response.changesReceivedUpTo());
        if (w.reporting && segment != null && response.segmentAck() != null
                && response.segmentAck().reportId().equals(w.reportId) && response.segmentAck().index() == w.index) {
            String status = response.segmentAck().status();
            if (InternalApi.SEGMENT_DONE.equals(status)) {
                w.finish();
            } else if (InternalApi.SEGMENT_OK.equals(status)) {
                if (w.index == w.segmentCount() - 1) {
                    w.finish();
                } else {
                    w.index++;
                }
            }
            // stale: the reply carries the next attempt (or the worker would abandon)
        }
        w.resync(response.resync(), random);
    }
}
