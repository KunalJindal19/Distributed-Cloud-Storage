package com.objectstore.controller.cluster;

import static com.objectstore.controller.cluster.TestSupport.chunk;
import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.controller.cluster.PendingOps.Op;
import com.objectstore.controller.cluster.PendingOps.Reason;
import com.objectstore.controller.cluster.PendingOps.State;
import org.junit.jupiter.api.Test;

/** The resend timer (v5 §3.3): resends with the same version through the writer; UPLOAD → REPAIR; resyncing targets. */
class ResendTimerTest {

    @Test
    void theTimerThreadResendsAnUnansweredDeleteThroughTheWriterWithTheSameVersion() throws Exception {
        TestSupport t = new TestSupport(TestSupport.properties(2, 2, 0.1, 0.2, 0.4, 0, 2000));   // first resend 100 ms
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        ResendTimer timer = new ResendTimer(t.cluster, t.queue);
        timer.start();
        t.writer.start();
        try {
            Op op = t.cluster.write(() -> t.cluster.ops().ensure(chunk(0), "worker1", Reason.OBJECT_DELETED));
            long version = op.version();
            t.handOut("worker1");
            assertThat(t.cluster.read(op::state)).isEqualTo(State.SENT);

            long deadline = System.nanoTime() + 5_000_000_000L;
            while (t.cluster.read(op::attempts) == 0 && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }

            assertThat(t.cluster.read(op::attempts)).isEqualTo(1);
            assertThat(t.cluster.read(op::state)).as("queued again").isEqualTo(State.QUEUED);
            assertThat(t.cluster.read(op::version)).isEqualTo(version);
        } finally {
            timer.stop();
            t.writer.stop();
        }
    }

    @Test
    void anUploadOpWhoseReceivedNeverComesBecomesARepairAtTheSameVersion() {
        TestSupport t = new TestSupport();
        t.knowChunk(chunk(0));
        t.join("worker1");
        long version = t.versions.next();
        Op op = t.cluster.write(() -> t.cluster.ops().upload(chunk(0), "worker1", version));

        t.cluster.write(() -> t.cluster.ops().deadline(chunk(0), "worker1", version, t.cluster.now() + (1L << 50)));

        assertThat(op.reason()).isEqualTo(Reason.REPAIR);
        assertThat(op.version()).isEqualTo(version);
        assertThat(op.state()).isEqualTo(State.WAITING);
        t.assertNoViolations();
    }

    @Test
    void anOpTargetingAResyncingWorkerIsReArmedWithoutCountingAnAttempt() {
        TestSupport t = new TestSupport();
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        Op op = t.cluster.write(() -> t.cluster.ops().ensure(chunk(0), "worker1", Reason.OBJECT_DELETED));
        t.handOut("worker1");
        t.cluster.write(() -> t.cluster.startResync(t.cluster.worker("worker1"), "test", false));

        t.cluster.write(() -> t.cluster.ops().deadline(chunk(0), "worker1", op.version(), t.cluster.now() + (1L << 50)));

        assertThat(op.attempts()).isZero();
        assertThat(op.state()).isEqualTo(State.SENT);
    }
}
