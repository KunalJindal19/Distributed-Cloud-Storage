package com.objectstore.controller.replication;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.controller.cluster.ClusterState;
import com.objectstore.controller.cluster.ClusterState.PendingCopy;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.ReplicaState;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.ClusterState.WorkerState;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.objects.PlacementPolicy;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Step 3 of the replication monitor (HDFS RedundancyMonitor and the invalidation of excess and
 * corrupt replicas). For every chunk, fewest live copies first:
 *
 * <ol type="a">
 *   <li><b>Under-replicated</b> ({@code live + pending < RF}): if a surplus copy is merely queued
 *       for deletion, cancel that and keep it; if there is no OK copy at all, report the chunk as
 *       missing (once); otherwise order a healthy holder to copy the chunk directly to a new
 *       worker ({@code REPLICATE}, HDFS {@code DNA_TRANSFER}) and track the copy as pending.</li>
 *   <li><b>Over-replicated</b> ({@code live > RF}, nothing pending, no holder STALE or without a
 *       completed full report): mark the surplus copies EXCESS and order their deletion.</li>
 *   <li><b>Clean-up</b> on ALIVE workers: a surplus copy, and a CORRUPT copy once RF good copies
 *       exist again (replace first, then invalidate, as HDFS does), gets a deletion in the ledger,
 *       which re-sends it until the worker confirms.</li>
 * </ol>
 *
 * Runs as part of the monitor's synchronous pass under the cluster lock. Each chunk is handled in
 * its own try/catch, so one bad chunk cannot abort the pass.
 */
@Component
public class ReplicationPlanner {

    private static final Logger log = LoggerFactory.getLogger(ReplicationPlanner.class);

    private final ClusterState cluster;
    private final PlacementPolicy placement;
    private final ControllerProperties properties;
    /** Chunks currently known to have no OK copy; "unrecoverable" is recorded once per transition. */
    private final Set<String> missing = new HashSet<>();

    public ReplicationPlanner(ClusterState cluster, PlacementPolicy placement, ControllerProperties properties) {
        this.cluster = cluster;
        this.placement = placement;
        this.properties = properties;
    }

    /** Must be called under the cluster lock. */
    public void plan() {
        int rf = properties.replicationFactor();
        Map<String, Integer> liveCount = new HashMap<>();
        for (String chunkId : cluster.chunks().keySet()) {
            liveCount.put(chunkId, cluster.live(chunkId).size());
        }
        List<String> order = new ArrayList<>(liveCount.keySet());
        order.sort(Comparator.comparingInt((String c) -> liveCount.get(c)).thenComparing(c -> c));
        missing.retainAll(liveCount.keySet());

        int blocked = 0;
        for (String chunkId : order) {
            try {
                if (!planChunk(chunkId, rf)) {
                    blocked++;
                }
            } catch (RuntimeException e) {
                log.error("[monitor] chunk={} could not be planned", chunkId, e);
            }
        }
        cluster.setBlockedLastTick(blocked);
        if (blocked > 0) {
            for (int i = 0; i < blocked; i++) {
                cluster.increment("blocked");
            }
        }
    }

    /** @return false if the chunk needed a copy but no source or target was available */
    private boolean planChunk(String chunkId, int rf) {
        List<String> live = cluster.live(chunkId);
        List<PendingCopy> pending = cluster.pendingOf(chunkId);
        Map<String, Replica> replicas = cluster.replicasOf(chunkId);
        boolean postpone = replicas.entrySet().stream().anyMatch(entry -> {
            WorkerInfo worker = cluster.worker(entry.getKey());
            return worker.state == WorkerState.STALE || !worker.hasReported;
        });

        if (live.size() + pending.size() < rf) {
            // a) Prefer keeping a surplus copy that has not actually been deleted yet.
            for (Map.Entry<String, Replica> entry : replicas.entrySet()) {
                Replica replica = entry.getValue();
                if (replica.state == ReplicaState.EXCESS && !replica.deleteDelivered) {
                    cluster.ledger().cancel(chunkId, entry.getKey());
                    replica.state = ReplicaState.OK;
                    replica.deleteCmdId = null;
                    replica.deleteQueuedAt = 0;
                    cluster.record("restored", chunkId, entry.getKey(), null, "surplus copy kept: the chunk is short of copies");
                    return true;
                }
            }
            if (live.isEmpty()) {
                if (missing.add(chunkId)) {
                    cluster.record("unrecoverable", chunkId, null, null,
                            "no OK copy on any live worker; waiting for a holder to return");
                }
                cleanUp(chunkId, live.size(), rf, postpone);
                return true;
            }
            missing.remove(chunkId);
            Optional<String> source = placement.chooseSource(chunkId);
            Optional<String> target = placement.chooseTarget(chunkId);
            if (source.isEmpty() || target.isEmpty()) {
                cleanUp(chunkId, live.size(), rf, postpone);
                return false;
            }
            WorkerInfo targetWorker = cluster.worker(target.get());
            Command command = cluster.queueCommand(source.get(), InternalApi.COMMAND_REPLICATE, chunkId,
                    cluster.chunk(chunkId).checksum(), targetWorker.url, null);
            if (command == null) {
                cleanUp(chunkId, live.size(), rf, postpone);
                return false;                                   // the source's queue is full
            }
            cluster.addPending(chunkId, source.get(), target.get(), command.cmdId());
            log.info("[monitor] REPLICATE chunk={} copies={} {}->{} cmd={}", chunkId, live.size(),
                    source.get(), target.get(), command.cmdId());
            cluster.record("replicate", chunkId, source.get(), target.get(),
                    "%d of %d live copies; copy ordered".formatted(live.size(), rf));
        } else {
            missing.remove(chunkId);
            // b) Surplus copies, once every holder is known to be current.
            if (live.size() > rf && pending.isEmpty() && !postpone) {
                for (String victim : placement.chooseExcess(chunkId, live.size() - rf)) {
                    cluster.replica(chunkId, victim).state = ReplicaState.EXCESS;
                    cluster.sendDelete(victim, chunkId, "excess");
                    log.info("[monitor] EXCESS chunk={} on {} ({} live copies)", chunkId, victim, live.size());
                }
            }
        }
        cleanUp(chunkId, cluster.live(chunkId).size(), rf, postpone);
        return true;
    }

    /**
     * c) Make sure every surplus copy, and every corrupt copy once RF good copies exist, has a
     * deletion in the ledger (which re-sends it until the worker confirms).
     */
    private void cleanUp(String chunkId, int live, int rf, boolean postpone) {
        for (Map.Entry<String, Replica> entry : new ArrayList<>(cluster.replicasOf(chunkId).entrySet())) {
            WorkerInfo worker = cluster.worker(entry.getKey());
            Replica replica = entry.getValue();
            if (worker.state != WorkerState.ALIVE || cluster.ledger().entry(chunkId, entry.getKey()) != null) {
                continue;
            }
            if (replica.state == ReplicaState.EXCESS) {
                cluster.sendDelete(entry.getKey(), chunkId, "excess");
            } else if (replica.state == ReplicaState.CORRUPT && live >= rf && !postpone) {
                cluster.sendDelete(entry.getKey(), chunkId, "corrupt");
                log.info("[monitor] DELETE corrupt chunk={} on {} ({} good copies exist)", chunkId, entry.getKey(), live);
            }
        }
    }
}
