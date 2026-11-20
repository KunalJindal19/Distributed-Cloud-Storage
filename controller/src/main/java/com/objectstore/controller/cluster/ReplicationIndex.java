package com.objectstore.controller.cluster;

import com.objectstore.controller.cluster.ClusterState.PendingCopy;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.ReplicaState;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Everything derived from the block map that must be cheap to read (v4 §7.1, §7.2): the
 * {@link NeededReplication} levels, the {@code cleanup} and {@code postponed} sets, and the counters
 * (copies by state, under-/over-replicated and missing chunks, {@code safeCount}).
 *
 * <p><b>One place changes it.</b> Every method of {@link ClusterState} that changes a copy, a
 * pending copy, a ledger entry or the chunk mirror calls {@code markDirty(c)}; before every
 * write-lock release ({@code flushDirty}) each dirty chunk is recomputed from scratch, O(copies of
 * the chunk), and the counters move by the difference. Nothing else touches the index.
 *
 * <p>A chunk is in {@code cleanup} when it has more than RF live copies and nothing pending, or a
 * CORRUPT or EXCESS copy without a ledger entry while RF live copies exist. The planner moves a cleanup chunk whose holders are not all current (STALE or not
 * reported) to {@code postponed}. Guarded by the cluster lock.
 */
final class ReplicationIndex {

    /** What the index last computed for one chunk. */
    record Entry(int level, int live, int pending, int ok, int corrupt, int excess, boolean cleanup) {
    }

    private final ClusterState cluster;
    private final int rf;
    final NeededReplication levels;
    final LinkedHashSet<String> cleanup = new LinkedHashSet<>();
    final LinkedHashSet<String> postponed = new LinkedHashSet<>();
    private final LinkedHashSet<String> dirty = new LinkedHashSet<>();
    /** Every known chunk, as a ring for the rolling invariant check (head → check → tail). */
    private final LinkedHashSet<String> ring = new LinkedHashSet<>();
    private final Map<String, Entry> entries = new HashMap<>();
    private final Set<String> unrecoverable = new HashSet<>();
    private long ok;
    private long corrupt;
    private long excess;
    private long missing;
    private long under;
    private long over;

    ReplicationIndex(ClusterState cluster, int rf) {
        this.cluster = cluster;
        this.rf = rf;
        this.levels = new NeededReplication(rf);
    }

    void markDirty(String chunkId) {
        dirty.add(chunkId);
    }

    boolean hasDirty() {
        return !dirty.isEmpty();
    }

    void flush() {
        while (!dirty.isEmpty()) {
            Iterator<String> it = dirty.iterator();
            String chunkId = it.next();
            it.remove();
            recompute(chunkId);
        }
    }

    /** Computes what {@code recompute} would store for a chunk, without storing it. */
    private Entry compute(String chunkId) {
        if (cluster.chunk(chunkId) == null) {
            return null;
        }
        int live = 0;
        int okCount = 0;
        int corruptCount = 0;
        int excessCount = 0;
        for (Map.Entry<String, Replica> holder : cluster.replicasOf(chunkId).entrySet()) {
            Replica replica = holder.getValue();
            WorkerInfo worker = cluster.worker(holder.getKey());
            switch (replica.state) {
                case OK -> {
                    okCount++;
                    if (worker != null && !worker.isDead()) {
                        live++;
                    }
                }
                case CORRUPT -> corruptCount++;
                case EXCESS -> excessCount++;
            }
        }
        int pending = cluster.pendingOf(chunkId).size();
        boolean wantsCleanup = (live > rf && pending == 0) || needsDeletion(chunkId, live);
        return new Entry(levels.levelFor(live, pending), live, pending, okCount, corruptCount, excessCount, wantsCleanup);
    }

    /** An EXCESS copy without a ledger entry, or a CORRUPT one while RF live copies exist. */
    private boolean needsDeletion(String chunkId, int live) {
        for (Map.Entry<String, Replica> holder : cluster.replicasOf(chunkId).entrySet()) {
            ReplicaState state = holder.getValue().state;
            if ((state == ReplicaState.EXCESS || state == ReplicaState.CORRUPT) && live >= rf
                    && cluster.ledger().entry(chunkId, holder.getKey()) == null) {
                return true;
            }
        }
        return false;
    }

    /** Recomputes one chunk and moves it between levels and sets; the counters move by the difference. */
    void recompute(String chunkId) {
        Entry old = entries.get(chunkId);
        Entry now = compute(chunkId);
        if (old != null) {
            subtract(old);
        }
        if (now == null) {
            entries.remove(chunkId);
            levels.place(chunkId, -1);
            cleanup.remove(chunkId);
            postponed.remove(chunkId);
            ring.remove(chunkId);
            unrecoverable.remove(chunkId);
            return;
        }
        add(now);
        entries.put(chunkId, now);
        ring.add(chunkId);
        levels.place(chunkId, now.level());
        if (!now.cleanup()) {
            cleanup.remove(chunkId);
            postponed.remove(chunkId);
        } else if (!postponed.contains(chunkId)) {
            cleanup.add(chunkId);
        }
        if (now.live() == 0 && old != null && old.live() > 0 && unrecoverable.add(chunkId)) {
            cluster.record("unrecoverable", chunkId, null, null,
                    "no OK copy on any live worker; waiting for a holder to return");
        } else if (now.live() > 0 && unrecoverable.remove(chunkId)) {
            cluster.record("recoverable", chunkId, null, null, "an OK copy is live again");
        }
    }

    private void add(Entry entry) {
        ok += entry.ok();
        corrupt += entry.corrupt();
        excess += entry.excess();
        if (entry.live() == 0) {
            missing++;
        } else if (entry.live() < rf) {
            under++;
        } else if (entry.live() > rf) {
            over++;
        }
    }

    private void subtract(Entry entry) {
        ok -= entry.ok();
        corrupt -= entry.corrupt();
        excess -= entry.excess();
        if (entry.live() == 0) {
            missing--;
        } else if (entry.live() < rf) {
            under--;
        } else if (entry.live() > rf) {
            over--;
        }
    }

    // ---- reading -----------------------------------------------------------------------------------

    Entry entry(String chunkId) {
        return entries.get(chunkId);
    }

    long ok() {
        return ok;
    }

    long corrupt() {
        return corrupt;
    }

    long excess() {
        return excess;
    }

    /** Known chunks with no live copy. */
    long missing() {
        return missing;
    }

    /** Known chunks with 1 ≤ live < RF. */
    long under() {
        return under;
    }

    long over() {
        return over;
    }

    /** Chunks with at least one live copy (safe mode). */
    long safeCount() {
        return entries.size() - missing;
    }

    /** Moves up to {@code budget} postponed chunks back to cleanup (a worker became current). */
    int releasePostponed(int budget) {
        int moved = 0;
        for (Iterator<String> it = postponed.iterator(); it.hasNext() && moved < budget; ) {
            String chunkId = it.next();
            it.remove();
            cleanup.add(chunkId);
            moved++;
        }
        return moved;
    }

    /** The next {@code count} chunks of the ring, rotated to its tail. */
    List<String> nextForRollingCheck(int count) {
        List<String> batch = new ArrayList<>(Math.min(count, ring.size()));
        Iterator<String> it = ring.iterator();
        while (it.hasNext() && batch.size() < count) {
            batch.add(it.next());
            it.remove();
        }
        ring.addAll(batch);
        return batch;
    }

    // ---- checking ------------------------------------------------------------------------------------

    /**
     * Checks one chunk's index entry against a fresh computation (I9 and the per-chunk part of
     * I10); a mismatch is repaired by recomputing. Returns the problem, or null.
     */
    String checkChunk(String chunkId) {
        Entry stored = entries.get(chunkId);
        Entry fresh = compute(chunkId);
        String problem = null;
        if (fresh == null ? stored != null : !fresh.equals(stored)) {
            problem = "I10: index entry of " + chunkId + " is " + stored + ", recomputed " + fresh;
        } else if (fresh != null && levels.levelOf(chunkId) != fresh.level()) {
            problem = "I9: " + chunkId + " is in level " + levels.levelOf(chunkId) + " but belongs in " + fresh.level();
        } else if (fresh != null && fresh.cleanup() != (cleanup.contains(chunkId) || postponed.contains(chunkId))) {
            problem = "I9: cleanup membership of " + chunkId + " is wrong";
        } else if (cleanup.contains(chunkId) && postponed.contains(chunkId)) {
            problem = "I9: " + chunkId + " is in both cleanup and postponed";
        }
        if (problem != null) {
            recompute(chunkId);
        }
        return problem;
    }

    /** The full index check (I9, I10): every chunk and every counter against a recomputation. */
    List<String> checkAll(Set<String> skip) {
        List<String> violations = new ArrayList<>();
        if (!levels.consistent()) {
            violations.add("I9: the level sets and levelOf disagree");
        }
        long expOk = 0;
        long expCorrupt = 0;
        long expExcess = 0;
        long expMissing = 0;
        long expUnder = 0;
        long expOver = 0;
        for (String chunkId : cluster.chunks().keySet()) {
            Entry fresh = compute(chunkId);
            if (!skip.contains(chunkId)) {
                Entry stored = entries.get(chunkId);
                if (!fresh.equals(stored)) {
                    violations.add("I10: index entry of " + chunkId + " is " + stored + ", recomputed " + fresh);
                } else if (levels.levelOf(chunkId) != fresh.level()) {
                    violations.add("I9: " + chunkId + " is in level " + levels.levelOf(chunkId) + ", belongs in " + fresh.level());
                } else if (fresh.cleanup() != (cleanup.contains(chunkId) || postponed.contains(chunkId))) {
                    violations.add("I9: cleanup membership of " + chunkId + " is wrong");
                }
            }
            Entry counted = skip.contains(chunkId) ? entries.get(chunkId) : fresh;
            if (counted == null) {
                continue;
            }
            expOk += counted.ok();
            expCorrupt += counted.corrupt();
            expExcess += counted.excess();
            if (counted.live() == 0) {
                expMissing++;
            } else if (counted.live() < rf) {
                expUnder++;
            } else if (counted.live() > rf) {
                expOver++;
            }
        }
        for (String chunkId : levelsAndSets()) {
            if (cluster.chunk(chunkId) == null) {
                violations.add("I9: unknown chunk " + chunkId + " in the index");
            }
        }
        for (String chunkId : cleanup) {
            if (postponed.contains(chunkId)) {
                violations.add("I9: " + chunkId + " is in both cleanup and postponed");
            }
        }
        if (entries.size() != cluster.chunks().size()) {
            violations.add("I10: the index has " + entries.size() + " chunks, the mirror " + cluster.chunks().size());
        }
        if (expOk != ok || expCorrupt != corrupt || expExcess != excess || expMissing != missing
                || expUnder != under || expOver != over) {
            violations.add("I10: counters ok/corrupt/excess/missing/under/over are " + List.of(ok, corrupt, excess, missing,
                    under, over) + ", recomputed " + List.of(expOk, expCorrupt, expExcess, expMissing, expUnder, expOver));
        }
        return violations;
    }

    private Set<String> levelsAndSets() {
        Set<String> all = new HashSet<>(cleanup);
        all.addAll(postponed);
        for (int level = 0; level < levels.count(); level++) {
            String head = levels.head(level);
            if (head != null) {
                all.add(head);
            }
        }
        return all;
    }
}
