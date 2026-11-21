package com.objectstore.controller.replication;

import com.objectstore.controller.cluster.ClusterState;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.ClusterState.WorkerState;
import com.objectstore.controller.cluster.NeededReplication;
import com.objectstore.controller.cluster.PendingOps;
import com.objectstore.controller.cluster.PendingOps.Kind;
import com.objectstore.controller.cluster.PendingOps.Op;
import com.objectstore.controller.cluster.PendingOps.Reason;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.objects.PlacementPolicy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * The replication planner (HDFS RedundancyMonitor and the invalidation of surplus replicas), driven
 * by the priority index (v5 §6.3, §6.4):
 *
 * <ol>
 *   <li><b>Free streams:</b> {@code MAX_STREAMS − busy streams} for workers that are ALIVE, reported
 *       and whose queue is not full. None free: no repair this tick.</li>
 *   <li><b>Repair</b> while {@code L + P < RF}: the schedulable levels ({@code L ≥ 1}) in priority
 *       order; level k of K non-empty ones has weight {@code 2^(K−1−k)} and an integer quota of
 *       PLANNER_VISIT_BUDGET in that proportion (floor plus largest remainder, so the quotas add up
 *       exactly to the budget; unused quota passes on). Each needed copy is
 *       {@code ensure(INSERT, REPAIR)} on a target (ALIVE, reported, not resyncing, not holding the
 *       chunk, no op on the pair, not recently failed) with a REPLICATE queued to a source with a free
 *       stream. A visited chunk still in its level goes to the tail (round-robin). Chunks with no
 *       live copy are never scheduled: they are unrecoverable until a holder returns.</li>
 *   <li><b>Safe excess cleanup</b> (at most CLEANUP_BUDGET chunks): a chunk with {@code L > RF}, every
 *       holder ALIVE, reported and not resyncing, and no op of any kind qualifies. One victim is chosen
 *       (an unverified copy first). If at least RF <em>other</em> holders have fresh controller-timed
 *       VERIFY credit, {@code setDelete(EXCESS)} records them as the survivors; otherwise the survivors
 *       that need it get a VERIFY and the chunk is postponed. Keeping an extra copy is always safer
 *       than deleting on unproven redundancy.</li>
 *   <li><b>Postponed</b> (at most POSTPONED_BUDGET, rotating): chunks whose holders are current and
 *       whose VERIFYs are answered (or due again) return to cleanup.</li>
 *   <li><b>Waiting ops</b> (at most CLEANUP_BUDGET): a WAITING REPAIR gets a source; any waiting op
 *       is queued if its worker can take it.</li>
 * </ol>
 *
 * Each step works on the current head of a set, so the write lock can be released between items
 * (the {@link Slicer}) without losing its place.
 */
@Component
public class ReplicationPlanner {

    private static final Logger log = LoggerFactory.getLogger(ReplicationPlanner.class);

    /** Gives up the write lock briefly when the current slice is used up. */
    @FunctionalInterface
    public interface Slicer {
        void check();

        Slicer NONE = () -> { };
    }

    /** What one tick did. */
    public record TickResult(int visited, int scheduled, int blocked, int freeTotal, int cleaned, int postponed) {
    }

    private final ClusterState cluster;
    private final PlacementPolicy placement;
    private final ControllerProperties properties;

    public ReplicationPlanner(ClusterState cluster, PlacementPolicy placement, ControllerProperties properties) {
        this.cluster = cluster;
        this.placement = placement;
        this.properties = properties;
    }

    /** One complete planning pass without slicing (tests). Must be called under the write lock. */
    public TickResult plan() {
        TickResult result = tick(Slicer.NONE);
        retryWaiting(properties.cleanupBudget(), Slicer.NONE);
        return result;
    }

    /** Steps 1-4 of the planner tick. Must be called under the write lock. */
    public TickResult tick(Slicer slicer) {
        int[] counts = underReplication(slicer);
        int cleaned = cleanup(slicer);
        int postponed = postponed(slicer);
        cluster.setBlockedLastTick(counts[2]);
        return new TickResult(counts[0], counts[1], counts[2], counts[3], cleaned, postponed);
    }

    // ---- repair ------------------------------------------------------------------------------------

    /**
     * Integer quotas for {@code k} levels with weights {@code 2^(k−1−i)}: the floor of each share plus
     * one more for the levels with the largest remainders, so they add up to exactly {@code budget}.
     */
    static long[] quotas(long budget, int k) {
        long[] quota = new long[k];
        if (k == 0) {
            return quota;
        }
        double weights = 0;
        for (int i = 0; i < k; i++) {
            weights += Math.pow(2, k - 1 - i);
        }
        double[] remainder = new double[k];
        long assigned = 0;
        for (int i = 0; i < k; i++) {
            double exact = budget * Math.pow(2, k - 1 - i) / weights;
            quota[i] = (long) Math.floor(exact);
            remainder[i] = exact - quota[i];
            assigned += quota[i];
        }
        long left = budget - assigned;
        while (left > 0) {
            int best = 0;
            for (int i = 1; i < k; i++) {
                if (remainder[i] > remainder[best]) {
                    best = i;
                }
            }
            quota[best]++;
            remainder[best] = -1;
            left--;
        }
        return quota;
    }

    /** @return {visited, scheduled, blocked, freeTotal at the start} */
    private int[] underReplication(Slicer slicer) {
        Map<String, Integer> free = freeStreams();
        int freeTotal = free.values().stream().mapToInt(Integer::intValue).sum();
        NeededReplication levels = cluster.levels();
        List<Integer> schedulable = new ArrayList<>();
        for (int level = 0; level < levels.count(); level++) {
            if (levels.liveOf(level) >= 1 && levels.size(level) > 0) {
                schedulable.add(level);
            }
        }
        if (freeTotal == 0 || schedulable.isEmpty()) {
            return new int[] {0, 0, blockedWithoutVisiting(levels), freeTotal};
        }
        long[] quota = quotas(properties.plannerVisitBudget(), schedulable.size());
        int visited = 0;
        int scheduled = 0;
        int blocked = 0;
        long carry = 0;
        int[] freeLeft = {freeTotal};
        outer:
        for (int i = 0; i < schedulable.size(); i++) {
            int level = schedulable.get(i);
            long allowed = quota[i] + carry;
            long visit = Math.min(allowed, levels.size(level));
            long used = 0;
            while (used < visit) {
                String chunkId = levels.head(level);
                if (chunkId == null) {
                    break;
                }
                used++;
                visited++;
                int made = planChunk(chunkId, free, freeLeft);
                scheduled += made;
                if (made == 0) {
                    blocked++;
                }
                cluster.flushDirty();
                if (levels.levelOf(chunkId) == level) {
                    levels.rotate(chunkId);             // round-robin inside the level
                }
                if (freeLeft[0] == 0) {
                    break outer;                        // early stop: no stream is free
                }
                slicer.check();
            }
            carry = allowed - used;
        }
        return new int[] {visited, scheduled, blocked, freeTotal};
    }

    private Map<String, Integer> freeStreams() {
        Map<String, Integer> free = new HashMap<>();
        for (WorkerInfo worker : cluster.workers().values()) {
            if (worker.state == WorkerState.ALIVE && worker.hasReported
                    && worker.commands.size() < properties.maxCommandsPerWorker()) {
                int streams = properties.maxStreams() - cluster.ops().streamsOf(worker.workerId);
                if (streams > 0) {
                    free.put(worker.workerId, streams);
                }
            }
        }
        return free;
    }

    private int blockedWithoutVisiting(NeededReplication levels) {
        int blocked = 0;
        for (int level = 0; level < levels.count(); level++) {
            if (levels.liveOf(level) >= 1) {
                blocked += levels.size(level);
            }
        }
        return blocked;
    }

    /** Plans one chunk; returns how many copies it got. */
    private int planChunk(String chunkId, Map<String, Integer> free, int[] freeLeft) {
        ClusterState.ChunkInfo chunk = cluster.chunk(chunkId);
        if (chunk == null) {
            return 0;
        }
        int rf = properties.replicationFactor();
        int live = cluster.live(chunkId).size();
        int need = rf - live - pendingCount(chunkId);
        int made = 0;
        while (made < need && freeLeft[0] > 0) {
            Optional<String> source = placement.chooseSource(chunkId, w -> free.getOrDefault(w, 0) > 0);
            Optional<String> target = placement.chooseTarget(chunkId);
            if (source.isEmpty() || target.isEmpty()) {
                break;
            }
            Op op = cluster.ops().ensure(chunkId, target.get(), Reason.REPAIR);
            if (op == null || op.state() != PendingOps.State.WAITING) {
                break;
            }
            cluster.ops().setSource(op, source.get());
            if (!cluster.ops().tryQueue(op)) {
                free.remove(source.get());              // its queue is full: the op waits for a source
                continue;
            }
            free.merge(source.get(), -1, Integer::sum);
            freeLeft[0]--;
            made++;
            log.info("[monitor] REPLICATE chunk={} copies={} {}->{} v={}", chunkId, live, source.get(), target.get(),
                    op.version());
            cluster.record("replicate", chunkId, source.get(), target.get(),
                    "%d of %d live copies; copy ordered (v%d)".formatted(live, rf, op.version()));
        }
        return made;
    }

    /** P: INSERT ops on the chunk whose target is current. */
    private int pendingCount(String chunkId) {
        int pending = 0;
        for (Op op : cluster.ops().opsFor(chunkId)) {
            if (op.kind() == Kind.INSERT && cluster.isCurrent(cluster.worker(op.workerId))) {
                pending++;
            }
        }
        return pending;
    }

    // ---- safe excess cleanup (v5 §6.4) ---------------------------------------------------------------

    private boolean holdersNotCurrent(String chunkId) {
        for (String workerId : cluster.replicasOf(chunkId).keySet()) {
            if (!cluster.isCurrent(cluster.worker(workerId))) {
                return true;
            }
        }
        return false;
    }

    /** A VERIFY of a holder is outstanding and not due for a resend. */
    private boolean awaitingVerify(String chunkId) {
        for (String workerId : cluster.replicasOf(chunkId).keySet()) {
            if (cluster.verifyPending(chunkId, workerId)) {
                return true;
            }
        }
        return false;
    }

    private int cleanup(Slicer slicer) {
        int handled = 0;
        var set = cluster.cleanupSet();
        while (handled < properties.cleanupBudget() && !set.isEmpty()) {
            String chunkId = set.iterator().next();
            set.remove(chunkId);
            handled++;
            if (cluster.chunk(chunkId) != null) {
                excess(chunkId);
            }
            cluster.markDirty(chunkId);
            cluster.flushDirty();
            slicer.check();
        }
        return handled;
    }

    /** The EXCESS rule for one chunk. */
    void excess(String chunkId) {
        int rf = properties.replicationFactor();
        if (cluster.live(chunkId).size() <= rf || cluster.ops().countFor(chunkId) > 0) {
            return;                                     // no longer surplus, or an op is pending on it
        }
        if (holdersNotCurrent(chunkId)) {
            cluster.postponedSet().add(chunkId);
            return;
        }
        Optional<String> victim = placement.chooseVictim(chunkId);
        if (victim.isEmpty()) {
            return;
        }
        List<PendingOps.Survivor> fresh = new ArrayList<>();
        List<String> needVerify = new ArrayList<>();
        for (Map.Entry<String, Replica> holder : cluster.replicasOf(chunkId).entrySet()) {
            if (holder.getKey().equals(victim.get())) {
                continue;
            }
            if (cluster.freshForDelete(holder.getValue(), cluster.worker(holder.getKey()))) {
                fresh.add(new PendingOps.Survivor(holder.getKey(), holder.getValue().gen));
            } else {
                needVerify.add(holder.getKey());
            }
        }
        if (fresh.size() >= rf) {
            Op op = cluster.ops().setDelete(chunkId, victim.get(), fresh);
            if (op != null) {
                log.info("[monitor] EXCESS chunk={} on {} ({} live copies; survivors {} verified)", chunkId, victim.get(),
                        cluster.live(chunkId).size(), fresh.stream().map(PendingOps.Survivor::workerId).toList());
            }
            return;
        }
        int sent = 0;
        for (String workerId : needVerify) {
            if (cluster.requestVerify(chunkId, workerId)) {
                sent++;
            }
        }
        cluster.increment("excess_postponed");
        cluster.postponedSet().add(chunkId);
        if (sent > 0) {
            log.info("[monitor] EXCESS chunk={} postponed: {} survivor(s) asked to VERIFY first", chunkId, sent);
        }
    }

    private int postponed(Slicer slicer) {
        var set = cluster.postponedSet();
        int budget = Math.min(properties.postponedBudget(), set.size());
        int released = 0;
        for (int i = 0; i < budget && !set.isEmpty(); i++) {
            String chunkId = set.iterator().next();
            set.remove(chunkId);
            if (cluster.chunk(chunkId) == null) {
                continue;
            }
            if (holdersNotCurrent(chunkId) || awaitingVerify(chunkId)) {
                set.add(chunkId);                       // rotation
            } else {
                cluster.markDirty(chunkId);             // re-enters cleanup if it still needs it
                released++;
            }
            slicer.check();
        }
        cluster.flushDirty();
        return released;
    }

    // ---- waiting ops -------------------------------------------------------------------------------

    /**
     * Re-checks up to {@code budget} WAITING ops: a REPAIR without a usable source gets one (a source
     * with a free stream), then any op is queued if its worker can take it.
     *
     * @return how many were queued
     */
    public int retryWaiting(int budget, Slicer slicer) {
        int queued = 0;
        for (Op op : cluster.ops().nextWaiting(budget)) {
            if (op.state() != PendingOps.State.WAITING) {
                continue;
            }
            if (op.reason() == Reason.REPAIR) {
                if (cluster.chunk(op.chunkId) == null) {
                    cluster.ops().drop(op, "the chunk left the namespace");
                    continue;
                }
                if (op.source() == null || !placement.chooseSource(op.chunkId, w -> w.equals(op.source())).isPresent()) {
                    Optional<String> source = placement.chooseSource(op.chunkId);
                    cluster.ops().setSource(op, source.orElse(null));
                }
            }
            if (cluster.ops().tryQueue(op)) {
                queued++;
            }
            slicer.check();
        }
        cluster.flushDirty();
        return queued;
    }
}
