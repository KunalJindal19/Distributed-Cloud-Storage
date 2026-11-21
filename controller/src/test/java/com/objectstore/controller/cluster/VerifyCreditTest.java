package com.objectstore.controller.cluster;

import static com.objectstore.controller.cluster.TestSupport.chunk;
import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.dto.Command;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.config.ControllerProperties;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/** VERIFY credit (v5 §6.4): only the current request, for the same copy generation and epoch, earns it. */
class VerifyCreditTest {

    private final TestSupport t = new TestSupport();
    private final ClusterState cluster = t.cluster;

    private Replica replica() {
        return cluster.read(() -> cluster.replica(chunk(0), "worker1"));
    }

    private boolean fresh() {
        return cluster.read(() -> cluster.freshForDelete(cluster.replica(chunk(0), "worker1"), cluster.worker("worker1")));
    }

    @Test
    void aHandedOutVerifyThatPassesEarnsCreditTimedFromItsHandOut() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        cluster.write(() -> cluster.requestVerify(chunk(0), "worker1"));
        List<Command> verify = t.handOut("worker1");
        long sentAt = replica().verifySentAt;
        assertThat(sentAt).isPositive();

        t.perform("worker1", verify.getFirst());

        assertThat(fresh()).isTrue();
        assertThat(replica().verifiedAt).isEqualTo(sentAt);
        assertThat(cluster.read(cluster::verifyOutstanding)).isZero();
    }

    @Test
    void anAnswerForAReplacedCopyGivesNoCredit() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        cluster.write(() -> cluster.requestVerify(chunk(0), "worker1"));
        Command verify = t.handOut("worker1").getFirst();
        t.ibr("worker1", t.received("worker1", chunk(0), cluster.versions().next()));   // the copy changed: new gen

        t.ibr("worker1", t.receivedVerify("worker1", chunk(0), replica().version, verify.cmdId()));

        assertThat(fresh()).isFalse();
    }

    @Test
    void anAnswerToAReplacedRequestGivesNoCredit() throws Exception {
        TestSupport quick = new TestSupport(new ControllerProperties(1_048_576, 2, Path.of("/tmp/unused.db"), 0.2, 0.4,
                0.5, 0.1, 0.3, 3, 2, 1000, 10, 0.999, 0.1, 5, 0, 0.5, 16 * 1024 * 1024, 2, 10, true, 5, 10_000, 5, 2,
                2000, 2000, 1000, 1000, 2, 240, 5, 3600, 0.1, 1, 60, 3600, 0.05));      // VERIFY_RESEND 50 ms
        quick.knowChunk(chunk(0));
        quick.join("worker1", chunk(0));
        quick.cluster.write(() -> quick.cluster.requestVerify(chunk(0), "worker1"));
        Command first = quick.handOut("worker1").getFirst();
        Thread.sleep(80);
        quick.cluster.write(() -> quick.cluster.requestVerify(chunk(0), "worker1"));      // replaces the request

        Replica replica = quick.cluster.read(() -> quick.cluster.replica(chunk(0), "worker1"));
        quick.ibr("worker1", quick.receivedVerify("worker1", chunk(0), replica.version, first.cmdId()));

        assertThat(quick.cluster.read(() -> quick.cluster.freshForDelete(replica, quick.cluster.worker("worker1")))).isFalse();
    }

    @Test
    void aResyncStartVoidsTheWorkersCreditAtOnce() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        cluster.write(() -> cluster.requestVerify(chunk(0), "worker1"));
        t.perform("worker1", t.handOut("worker1").getFirst());
        assertThat(fresh()).isTrue();

        cluster.write(() -> cluster.startResync(cluster.worker("worker1"), "periodic", true));

        assertThat(fresh()).isFalse();
    }
}
