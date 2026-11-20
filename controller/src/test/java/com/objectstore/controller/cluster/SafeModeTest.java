package com.objectstore.controller.cluster;

import static com.objectstore.controller.cluster.TestSupport.chunk;
import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.controller.cluster.ClusterState.ReplicaState;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/** Block 15 (v4 §7.3 step 1, §7.6): safe mode's O(1) counter matches a scan in every transition. */
class SafeModeTest {

    private static long scan(ClusterState cluster) {
        return cluster.read(() -> cluster.chunks().keySet().stream().filter(c -> !cluster.live(c).isEmpty()).count());
    }

    @Test
    void theSafeCountMatchesAScanAfterEveryChange() {
        Random random = new Random(7);
        TestSupport t = new TestSupport();
        ClusterState cluster = t.cluster;
        List<String> workers = List.of("worker1", "worker2", "worker3");
        workers.forEach(t::join);
        for (int i = 0; i < 20; i++) {
            t.knowChunk(chunk(i));
        }
        for (int op = 0; op < 3000; op++) {
            String chunkId = chunk(random.nextInt(25));
            String workerId = workers.get(random.nextInt(workers.size()));
            int kind = random.nextInt(6);
            cluster.write(() -> {
                switch (kind) {
                    case 0, 1 -> cluster.addReplica(chunkId, workerId, ReplicaState.OK);
                    case 2 -> cluster.removeReplica(chunkId, workerId);
                    case 3 -> cluster.markCorrupt(chunkId, workerId);
                    case 4 -> {
                        if (cluster.chunk(chunkId) == null) {
                            cluster.putChunk(chunkId, new ClusterState.ChunkInfo(TestSupport.OBJECT, 0, 10, TestSupport.SUM));
                        } else {
                            cluster.removeChunk(chunkId);
                        }
                    }
                    default -> cluster.setReplicaState(chunkId, workerId, ReplicaState.OK);
                }
            });
            assertThat(cluster.read(cluster::safeCount)).as("after op %d", op).isEqualTo(scan(cluster));
        }
    }

    @Test
    void safeModeEndsOnceTheThresholdHoldsForTheExtension() throws Exception {
        TestSupport t = new TestSupport();
        ClusterState cluster = t.cluster;
        SafeMode safeMode = new SafeMode(cluster, t.properties);
        cluster.setSafeMode(true);
        t.knowChunk(chunk(0));
        t.knowChunk(chunk(1));
        t.join("worker1", chunk(0));                                 // 1 of 2 chunks reported: below 0.999

        assertThat(cluster.write(safeMode::evaluate)).isTrue();
        assertThat(cluster.read(cluster::safeCount)).isEqualTo(1).isEqualTo(scan(cluster));

        t.fullReport("worker1", t.fake("worker1").boot, chunk(0), chunk(1));
        assertThat(cluster.read(cluster::safeCount)).isEqualTo(2).isEqualTo(scan(cluster));
        assertThat(cluster.write(safeMode::evaluate)).as("the extension has not passed").isTrue();
        Thread.sleep((long) (t.properties.safeModeExtensionSeconds() * 1000) + 50);
        assertThat(cluster.write(safeMode::evaluate)).isFalse();
        assertThat(cluster.isSafeMode()).isFalse();
    }
}
