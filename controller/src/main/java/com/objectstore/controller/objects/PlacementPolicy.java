package com.objectstore.controller.objects;

import com.objectstore.controller.domain.ReplicaLocation;
import com.objectstore.controller.domain.Worker;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.springframework.stereotype.Component;

/**
 * Decides where copies go. Pure functions of their arguments, so every decision is deterministic
 * and can be reproduced by hand.
 */
@Component
public class PlacementPolicy {

    private static final Comparator<Worker> BY_ID = Comparator.comparing(Worker::workerId);

    /**
     * Where the copies of one chunk go at upload time.
     *
     * <p>The alive workers, sorted by id, form a ring. Chunk {@code i} of an object starts at
     * ring position {@code (offset + i) mod n}, where {@code offset} is derived from the object
     * id, and its copies go to consecutive positions. Consequences:
     * <ul>
     *   <li>the copies of a chunk are always on <em>different</em> workers;</li>
     *   <li>consecutive chunks of an object rotate around the ring, so an object's chunks are
     *       spread evenly over all workers;</li>
     *   <li>different objects start at different positions, so even a store full of one-chunk
     *       objects fills all workers evenly instead of always the first two.</li>
     * </ul>
     *
     * @return all alive workers in preference order: the first {@code replicationFactor} entries
     *         are the intended holders, the rest are fall-backs should one of them fail the write
     */
    public List<Worker> ringFor(Collection<Worker> aliveWorkers, String objectId, int chunkIndex) {
        List<Worker> ring = new ArrayList<>(aliveWorkers);
        ring.sort(BY_ID);
        int n = ring.size();
        if (n == 0) {
            return ring;
        }
        int start = (int) ((ringOffset(objectId) + chunkIndex) % n);
        List<Worker> ordered = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            ordered.add(ring.get((start + i) % n));
        }
        return ordered;
    }

    /** A stable, evenly distributed number taken from the first 32 bits of the object's UUID. */
    static long ringOffset(String objectId) {
        return Long.parseLong(objectId.substring(0, 8), 16);
    }

    /**
     * Where a repair copy goes: an alive worker that does not hold any copy of the chunk yet,
     * preferring the one with the fewest copies overall (ties: lowest id).
     *
     * @param holders ids of all workers that have a copy recorded for the chunk, in any state
     */
    public Optional<Worker> chooseRepairTarget(Collection<Worker> aliveWorkers, Set<String> holders,
                                               Map<String, Integer> replicaCounts) {
        return aliveWorkers.stream()
                .filter(worker -> !holders.contains(worker.workerId()))
                .min(Comparator.<Worker>comparingInt(worker -> replicaCounts.getOrDefault(worker.workerId(), 0))
                        .thenComparing(BY_ID));
    }

    /**
     * Which surplus copy is removed: the one on the worker with the most copies overall (ties:
     * highest id), which pulls the workers back towards an even load.
     */
    public ReplicaLocation chooseTrimVictim(Collection<ReplicaLocation> goodCopies, Map<String, Integer> replicaCounts) {
        return goodCopies.stream()
                .max(Comparator.<ReplicaLocation>comparingInt(copy -> replicaCounts.getOrDefault(copy.workerId(), 0))
                        .thenComparing(ReplicaLocation::workerId))
                .orElseThrow(() -> new IllegalArgumentException("no copies to choose from"));
    }

    /**
     * The order in which copies are tried when reading a chunk: least loaded worker first (ties:
     * lowest id).
     */
    public List<ReplicaLocation> readOrder(Collection<ReplicaLocation> goodCopies, Map<String, Integer> replicaCounts) {
        return goodCopies.stream()
                .sorted(Comparator.<ReplicaLocation>comparingInt(copy -> replicaCounts.getOrDefault(copy.workerId(), 0))
                        .thenComparing(ReplicaLocation::workerId))
                .toList();
    }
}
