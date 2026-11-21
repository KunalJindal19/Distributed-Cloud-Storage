package com.objectstore.controller.cluster;

import static com.objectstore.controller.cluster.TestSupport.chunk;
import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.controller.cluster.PendingOps.Op;
import com.objectstore.controller.cluster.PendingOps.Reason;
import com.objectstore.controller.cluster.PendingOps.State;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Applying worker results (v5 §3.4): the physical part always, completion by version, the CMD_FAILED table. */
class VersionedOpsTest {

    private final TestSupport t = new TestSupport();
    private final ClusterState cluster = t.cluster;

    private Op deleteOn(String workerId) {
        return cluster.write(() -> cluster.ops().ensure(chunk(0), workerId, Reason.OBJECT_DELETED));
    }

    @Test
    void theV4DoubleDeleteCannotHappenAnOldDeletedNeverCompletesANewerOp() {
        // v4: any DELETED completed the ledger entry. v5: only the op's own version (or a newer one) does.
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        Op op = cluster.write(() -> cluster.ops().ensure(chunk(0), "worker1", Reason.CORRUPT));
        long first = op.version();
        t.handOut("worker1");                                               // DELETE #1 handed out
        deleteOn("worker1");                                                // superseded: DELETE #2, newer
        long newer = op.version();
        assertThat(newer).isGreaterThan(first);

        t.ibr("worker1", t.deleted("worker1", chunk(0), first, "corrupt"));   // #1's result: an older version

        assertThat(t.op(chunk(0), "worker1")).as("the newer op is not completed by an older result").isNotNull();
        assertThat(t.holders(chunk(0))).as("the physical part is applied anyway").isEmpty();
        t.ibr("worker1", t.deleted("worker1", chunk(0), newer, "deleted"));
        assertThat(t.op(chunk(0), "worker1")).isNull();
    }

    @Test
    void aStaleRejectionReissuesTheSameKindAndReasonWithAFreshVersion() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        Op op = deleteOn("worker1");
        long version = op.version();
        t.handOut("worker1");

        t.ibr("worker1", t.cmdFailed("worker1", chunk(0), version, InternalApi.FAILED_STALE));

        assertThat(op.version()).isGreaterThan(version);
        assertThat(op.reason()).isEqualTo(Reason.OBJECT_DELETED);
        assertThat(op.state()).isEqualTo(State.QUEUED);
        assertThat(cluster.counter("stale_rejections")).isEqualTo(1);
        assertThat(cluster.events()).anyMatch(e -> e.event().equals("stale"));
    }

    @Test
    void busyAndIoErrorsGoBackToWaitingWithTheSameVersion() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        Op op = deleteOn("worker1");
        long version = op.version();
        for (String reason : List.of(InternalApi.FAILED_BUSY, InternalApi.FAILED_IO_ERROR)) {
            t.handOut("worker1");
            t.ibr("worker1", t.cmdFailed("worker1", chunk(0), version, reason));
            assertThat(op.version()).isEqualTo(version);
            assertThat(op.state()).as("queued again").isEqualTo(State.QUEUED);
        }
    }

    @Test
    void aBadCommandIsDroppedAndAFailureForAnotherVersionIsIgnored() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        Op op = deleteOn("worker1");
        t.handOut("worker1");

        t.ibr("worker1", t.cmdFailed("worker1", chunk(0), op.version() + 2, InternalApi.FAILED_BAD_COMMAND));
        assertThat(t.op(chunk(0), "worker1")).as("not this op's version").isNotNull();
        assertThat(cluster.counter("cmd_failed_ignored")).isEqualTo(1);

        t.ibr("worker1", t.cmdFailed("worker1", chunk(0), op.version(), InternalApi.FAILED_BAD_COMMAND));
        assertThat(t.op(chunk(0), "worker1")).isNull();
    }

    @Test
    void anEvenVersionAboveTheHighestIssuedIsABugAndOnlyItsPhysicalPartIsApplied() {
        t.knowChunk(chunk(0));
        t.join("worker1");
        t.join("worker2", chunk(0));
        Op repair = cluster.write(() -> cluster.ops().ensure(chunk(0), "worker1", Reason.REPAIR));
        long future = cluster.versions().current() + 1000;

        t.ibr("worker1", t.received("worker1", chunk(0), future));

        assertThat(t.holders(chunk(0))).contains("worker1");
        assertThat(t.op(chunk(0), "worker1")).as("not completed").isSameAs(repair);
        assertThat(cluster.counter("future_versions")).isEqualTo(1);
    }

    @Test
    void aReceivedOfTheWrongKindLeavesADeleteOpAlone() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        Op op = deleteOn("worker1");
        List<Command> commands = t.handOut("worker1");
        assertThat(commands).hasSize(1);

        t.ibr("worker1", t.received("worker1", chunk(0), op.version()));     // RECEIVED does not match DELETE

        assertThat(t.op(chunk(0), "worker1")).isSameAs(op);
    }
}
