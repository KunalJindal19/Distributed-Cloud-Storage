package com.objectstore.controller.replication;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.controller.cluster.ClusterState;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.ReplicaState;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.ClusterState.WorkerState;
import com.objectstore.controller.cluster.NeededReplication;
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
 * The replication planner (HDFS RedundancyMonitor and the invalidation of excess and corrupt
 * replicas), driven by the priority index instead of a sort of every chunk (v4 §7.3):
 *
 * <ol start="2">
 *   <li><b>Free streams:</b> {@code free[W] = MAX_STREAMS − pendingFrom[W]} for workers that are
 *       ALIVE, reported and whose command queue is not full. None free: no repair this tick.</li>
 *   <li><b>Under-replication:</b> the schedulable levels ({@code L ≥ 1}) in priority order; level k
 *       of K non-empty ones has weight {@code 2^(K−1−k)} and a quota of PLANNER_VISIT_BUDGET in that
 *       proportion (unused quota passes on). Up to that many chunks are visited from the head of
 *       each level: an undelivered EXCESS deletion is cancelled first (the copy counts again);
 *       otherwise up to {@code RF − L − P} copies are ordered, each from a source with a free stream
 *       to a target not already chosen. A visited chunk still in the same level goes to the tail
 *       (round-robin), so blocked chunks cannot starve the others; with no free stream left the
 *       step ends. Chunks with no live copy are never scheduled.</li>
 *   <li><b>Cleanup</b> (at most CLEANUP_BUDGET chunks): a chunk whose holders are not all current
 *       (STALE or not reported) is postponed; otherwise surplus copies are marked EXCESS and
 *       deleted through the ledger, and corrupt copies are deleted once RF live copies exist.</li>
 *   <li><b>Postponed</b> (at most POSTPONED_BUDGET, rotating): chunks no longer postponed return to
 *       cleanup.</li>
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
        return tick(Slicer.NONE);
    }

    /** Steps 2-5 of the planner tick. Must be called under the write lock. */
    public TickResult tick(Slicer slicer) {
        int[] counts = underReplication(slicer);
        int cleaned = cleanup(slicer);
        int postponed = postponed(slicer);
        cluster.setBlockedLastTick(counts[2]);
        return new TickResult(counts[0], counts[1], counts[2], counts[3], cleaned, postponed);
    }

    // ---- step 2 and 3: free streams and under-replication ---------------------------------------

    /** @return {visited, scheduled, blocked, freeTotal at the start} */
    private int[] underReplication(Slicer slicer) {
        Map<String, Integer> free = new HashMap<>();
        int freeTotal = 0;
        for (WorkerInfo worker : cluster.workers().values()) {
            if (worker.state == WorkerState.ALIVE && worker.hasReported
                    && worker.commands.size() < properties.maxCommandsPerWorker()) {
                int streams = properties.maxStreams() - cluster.pendingFrom(worker.workerId);
                if (streams > 0) {
                    free.put(worker.workerId, streams);
                    freeTotal += streams;
                }
            }
        }
        int startFree = freeTotal;
        NeededReplication levels = cluster.levels();
        List<Integer> schedulable = new ArrayList<>();
        for (int level = 0; level < levels.count(); level++) {
            if (levels.liveOf(level) >= 1 && levels.size(level) > 0) {
                schedulable.add(level);
            }
        }
        int visited = 0;
        int scheduled = 0;
        int blocked = 0;
        if (freeTotal == 0 || schedulable.isEmpty()) {
            return new int[] {0, 0, blockedWithoutVisiting(levels), startFree};
        }
        int k = schedulable.size();
        double weights = 0;
        for (int i = 0; i < k; i++) {
            weights += Math.pow(2, k - 1 - i);
        }
        long carry = 0;
        int[] freeLeft = {freeTotal};
        outer:
        for (int i = 0; i < k; i++) {
            int level = schedulable.get(i);
            long quota = (long) Math.ceil(properties.plannerVisitBudget() * Math.pow(2, k - 1 - i) / weights) + carry;
            long visit = Math.min(quota, levels.size(level));
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
            carry = quota - used;
        }
        return new int[] {visited, scheduled, blocked, startFree};
    }

    /** With no free stream at all, every schedulable chunk is blocked this tick. */
    private int blockedWithoutVisiting(NeededReplication levels) {
        int blocked = 0;
        for (int level = 0; level < levels.count(); level++) {
            if (levels.liveOf(level) >= 1) {
                blocked += levels.size(level);
            }
        }
        return blocked;
    }

    /** Plans one chunk; returns how many copies (or restorations) it got. */
    private int planChunk(String chunkId, Map<String, Integer> free, int[] freeLeft) {
        // An undelivered surplus deletion is cancelled first: that copy counts again.
        for (Map.Entry<String, Replica> entry : cluster.replicasOf(chunkId).entrySet()) {
            Replica replica = entry.getValue();
            if (replica.state == ReplicaState.EXCESS && !replica.deleteDelivered) {
                cluster.ledger().cancel(chunkId, entry.getKey());
                cluster.setReplicaState(chunkId, entry.getKey(), ReplicaState.OK);
                replica.deleteCmdId = null;
                replica.deleteQueuedAt = 0;
                cluster.record("restored", chunkId, entry.getKey(), null, "surplus copy kept: the chunk is short of copies");
                return 1;
            }
        }
        int rf = properties.replicationFactor();
        int live = cluster.live(chunkId).size();
        int need = rf - live - cluster.pendingOf(chunkId).size();
        int made = 0;
        while (made < need && freeLeft[0] > 0) {
            Optional<String> source = placement.chooseSource(chunkId, w -> free.getOrDefault(w, 0) > 0);
            Optional<String> target = placement.chooseTarget(chunkId);
            if (source.isEmpty() || target.isEmpty()) {
                break;
            }
            WorkerInfo targetWorker = cluster.worker(target.get());
            Command command = cluster.queueCommand(source.get(), InternalApi.COMMAND_REPLICATE, chunkId,
                    cluster.chunk(chunkId).checksum(), targetWorker.url, null);
            if (command == null) {
                free.remove(source.get());              // its queue is full
                continue;
            }
            cluster.addPending(chunkId, source.get(), target.get(), command.cmdId());
            free.merge(source.get(), -1, Integer::sum);
            freeLeft[0]--;
            made++;
            log.info("[monitor] REPLICATE chunk={} copies={} {}->{} cmd={}", chunkId, live, source.get(), target.get(),
                    command.cmdId());
            cluster.record("replicate", chunkId, source.get(), target.get(),
                    "%d of %d live copies; copy ordered".formatted(live, rf));
        }
        return made;
    }

    // ---- step 4 and 5: cleanup and postponed --------------------------------------------------------

    private boolean postpone(String chunkId) {
        for (String workerId : cluster.replicasOf(chunkId).keySet()) {
            WorkerInfo worker = cluster.worker(workerId);
            if (worker == null || worker.state != WorkerState.ALIVE || !worker.hasReported) {
                return true;
            }
        }
        return false;
    }

    private int cleanup(Slicer slicer) {
        int rf = properties.replicationFactor();
        int handled = 0;
        var set = cluster.cleanupSet();
        while (handled < properties.cleanupBudget() && !set.isEmpty()) {
            String chunkId = set.iterator().next();
            set.remove(chunkId);
            handled++;
            if (cluster.chunk(chunkId) == null) {
                continue;
            }
            if (postpone(chunkId)) {
                cluster.postponedSet().add(chunkId);
                continue;
            }
            List<String> live = cluster.live(chunkId);
            if (live.size() > rf && cluster.pendingOf(chunkId).isEmpty()) {
                for (String victim : placement.chooseExcess(chunkId, live.size() - rf)) {
                    cluster.setReplicaState(chunkId, victim, ReplicaState.EXCESS);
                    cluster.sendDelete(victim, chunkId, "excess");
                    log.info("[monitor] EXCESS chunk={} on {} ({} live copies)", chunkId, victim, live.size());
                }
            }
            int liveNow = cluster.live(chunkId).size();
            for (Map.Entry<String, Replica> entry : new ArrayList<>(cluster.replicasOf(chunkId).entrySet())) {
                if (cluster.ledger().entry(chunkId, entry.getKey()) != null) {
                    continue;
                }
                if (entry.getValue().state == ReplicaState.EXCESS && liveNow >= rf) {
                    cluster.sendDelete(entry.getKey(), chunkId, "excess");
                } else if (entry.getValue().state == ReplicaState.CORRUPT && liveNow >= rf) {
                    cluster.sendDelete(entry.getKey(), chunkId, "corrupt");
                    log.info("[monitor] DELETE corrupt chunk={} on {} ({} good copies exist)", chunkId, entry.getKey(), liveNow);
                }
            }
            cluster.markDirty(chunkId);
            cluster.flushDirty();
            slicer.check();
        }
        return handled;
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
            if (postpone(chunkId)) {
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
}
