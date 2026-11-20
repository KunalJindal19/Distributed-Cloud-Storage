package com.objectstore.controller.cluster;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * Chunks that need more copies, by priority (HDFS {@code LowRedundancyBlocks}, generalised to any
 * replication factor; v4 §7.1). One insertion-ordered set per pair {@code (L, P)} with
 * {@code L + P < RF}, where L = OK copies on non-DEAD workers and P = pending copies (repair and
 * upload): {@code RF·(RF+1)/2} levels, in priority order L ascending, then P ascending. Levels
 * with {@code L = 0} have no source and are never scheduled. A chunk is in at most one level.
 *
 * <p>Only {@link ReplicationIndex#recompute} moves chunks between levels. Guarded by the cluster lock.
 */
public final class NeededReplication {

    private final int rf;
    private final int[][] index;                 // [L][P] -> level, or -1
    private final int[] levelL;
    private final int[] levelP;
    private final LinkedHashSet<String>[] levels;
    private final Map<String, Integer> levelOf = new HashMap<>();

    @SuppressWarnings("unchecked")
    NeededReplication(int rf) {
        this.rf = rf;
        int count = rf * (rf + 1) / 2;
        this.index = new int[rf][rf];
        this.levelL = new int[count];
        this.levelP = new int[count];
        this.levels = new LinkedHashSet[count];
        for (int[] row : index) {
            java.util.Arrays.fill(row, -1);
        }
        int next = 0;
        for (int l = 0; l < rf; l++) {
            for (int p = 0; l + p < rf; p++) {
                index[l][p] = next;
                levelL[next] = l;
                levelP[next] = p;
                levels[next] = new LinkedHashSet<>();
                next++;
            }
        }
    }

    /** The level for {@code (L, P)}, or -1 if {@code L + P >= RF}. */
    public int levelFor(int live, int pending) {
        if (live < 0 || pending < 0 || live + pending >= rf) {
            return -1;
        }
        return index[live][pending];
    }

    public int count() {
        return levels.length;
    }

    public int liveOf(int level) {
        return levelL[level];
    }

    public int pendingOf(int level) {
        return levelP[level];
    }

    public int size(int level) {
        return levels[level].size();
    }

    /** The level a chunk is in, or -1. */
    public int levelOf(String chunkId) {
        return levelOf.getOrDefault(chunkId, -1);
    }

    /** The first chunk of a level (its head), or null. */
    public String head(int level) {
        Iterator<String> it = levels[level].iterator();
        return it.hasNext() ? it.next() : null;
    }

    /** Puts the chunk in {@code level} (-1: out of every level). Moving levels puts it at the tail. */
    void place(String chunkId, int level) {
        int current = levelOf(chunkId);
        if (current == level) {
            return;
        }
        if (current >= 0) {
            levels[current].remove(chunkId);
            levelOf.remove(chunkId);
        }
        if (level >= 0) {
            levels[level].add(chunkId);
            levelOf.put(chunkId, level);
        }
    }

    /** Round-robin inside a level: the chunk goes to the tail of its level. */
    public void rotate(String chunkId) {
        int level = levelOf(chunkId);
        if (level >= 0 && levels[level].remove(chunkId)) {
            levels[level].add(chunkId);
        }
    }

    public int total() {
        return levelOf.size();
    }

    /** {"L,P": size} for every level, in priority order. */
    public Map<String, Integer> sizes() {
        Map<String, Integer> sizes = new LinkedHashMap<>();
        for (int level = 0; level < levels.length; level++) {
            sizes.put(levelL[level] + "," + levelP[level], levels[level].size());
        }
        return sizes;
    }

    /** I9 helper: the level sets and {@code levelOf} agree. */
    boolean consistent() {
        int total = 0;
        for (int level = 0; level < levels.length; level++) {
            for (String chunkId : levels[level]) {
                if (levelOf(chunkId) != level) {
                    return false;
                }
            }
            total += levels[level].size();
        }
        return total == levelOf.size();
    }
}
