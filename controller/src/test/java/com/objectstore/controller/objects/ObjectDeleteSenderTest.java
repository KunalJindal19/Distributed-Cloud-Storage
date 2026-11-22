package com.objectstore.controller.objects;

import static com.objectstore.controller.cluster.TestSupport.chunk;
import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.controller.cluster.PendingOps.Op;
import com.objectstore.controller.cluster.PendingOps.State;
import com.objectstore.controller.cluster.Task;
import com.objectstore.controller.cluster.TestSupport;
import com.objectstore.controller.cluster.WorkerException;
import com.objectstore.controller.observability.MessageTrace;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;

/**
 * A client's object delete sends its chunk deletes directly (DELETE /internal/chunks/{id}) instead of
 * waiting for heartbeats. Driven through the real DeleteObject task and hand-out rules; only the HTTP
 * call is faked.
 */
class ObjectDeleteSenderTest {

    private static final String OBJECT = chunk(0).substring(0, 36);
    private static final List<String> CHUNKS = List.of(chunk(0), chunk(1), chunk(2));

    private final TestSupport t = new TestSupport();

    /** One recorded call. */
    private record Call(String url, String chunkId, long version, String cmdId, String reason) {
    }

    private final List<Call> calls = Collections.synchronizedList(new ArrayList<>());

    /** Answers 204 unless {@code answer} says otherwise for (worker url, call number on that worker). */
    private ObjectDeleteSender sender(BiFunction<String, Integer, Integer> answer, boolean enabled) {
        Map<String, Integer> perWorker = new ConcurrentHashMap<>();
        ObjectDeleteSender.DeleteCall call = (url, chunkId, version, cmdId, reason) -> {
            calls.add(new Call(url, chunkId, version, cmdId, reason));
            int n = perWorker.merge(url, 1, Integer::sum);
            Integer status = answer.apply(url, n);
            return status == null ? 204 : status;
        };
        return new ObjectDeleteSender(t.cluster, t.writer, call, new MessageTrace("off"), Duration.ofSeconds(5), enabled);
    }

    private void twoHolders() {
        CHUNKS.forEach(t::knowChunk);
        t.join("worker1", CHUNKS.toArray(String[]::new));
        t.join("worker2", CHUNKS.toArray(String[]::new));
    }

    /** What ObjectService.delete does after its transaction: queue DeleteObject and start the sender. */
    private void clientDelete(ObjectDeleteSender sender) throws Exception {
        Task task = t.namespaceTasks.deleteObject(OBJECT, CHUNKS);
        t.queue.put(task);
        sender.sendWhenApplied(task, OBJECT, CHUNKS);
    }

    private void awaitCalls(int count) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (calls.size() < count && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        Thread.sleep(50);                       // let the sender finish its bookkeeping
        assertThat(calls).hasSize(count);
    }

    private Op op(String chunkId, String workerId) {
        return t.cluster.read(() -> t.cluster.ops().get(chunkId, workerId));
    }

    private static String url(String workerId) {
        return "http://" + workerId + ":8001";
    }

    private static WorkerException noAnswer(String workerId, Throwable cause) {
        return new WorkerException(url(workerId), null, cause.toString(), cause);
    }

    @Test
    void aClientDeleteSendsEveryCopysDeleteDirectlyAtItsVersionAndLeavesNothingForHeartbeats() throws Exception {
        twoHolders();

        clientDelete(sender((url, n) -> 204, true));
        awaitCalls(6);

        for (Call call : calls) {
            String workerId = call.url().equals(url("worker1")) ? "worker1" : "worker2";
            Op op = op(call.chunkId(), workerId);
            assertThat(op.state()).as("handed out: SENT, its resend deadline armed").isEqualTo(State.SENT);
            assertThat(call.version()).isEqualTo(op.version());
            assertThat(call.cmdId()).isNotNull();
            assertThat(call.reason()).isEqualTo("object_deleted");
        }
        assertThat(t.handOut("worker1")).as("nothing is delivered twice").isEmpty();
        assertThat(t.handOut("worker2")).isEmpty();
        assertThat(t.cluster.counter("deletes_sent_direct")).isEqualTo(6);

        // The workers report DELETED(v) in their change streams, which completes the ops as before.
        for (Call call : List.copyOf(calls)) {
            String workerId = call.url().equals(url("worker1")) ? "worker1" : "worker2";
            t.ibr(workerId, t.deleted(workerId, call.chunkId(), call.version(), "deleted"));
        }
        assertThat(t.cluster.read(() -> t.cluster.ops().size())).isZero();
        t.assertNoViolations();
    }

    @Test
    void anUnreachableWorkersDeletesGoBackToItsHeartbeatRepliesAtTheSameVersion() throws Exception {
        twoHolders();
        ObjectDeleteSender sender = sender((url, n) -> {
            if (url.equals(url("worker2"))) {
                throw noAnswer("worker2", new ConnectException("Connection refused"));
            }
            return 204;
        }, true);

        clientDelete(sender);
        awaitCalls(4);                          // three to worker1, one attempt to worker2

        for (String chunkId : CHUNKS) {
            assertThat(op(chunkId, "worker1").state()).isEqualTo(State.SENT);
            assertThat(op(chunkId, "worker2").state()).as("back in the queue").isEqualTo(State.QUEUED);
        }
        List<Command> replies = t.handOut("worker2");
        assertThat(replies).extracting(Command::chunkId).containsExactlyInAnyOrderElementsOf(CHUNKS);
        for (Command command : replies) {
            assertThat(command.type()).isEqualTo(InternalApi.COMMAND_DELETE);
            assertThat(command.version()).as("the same version").isEqualTo(op(command.chunkId(), "worker2").version());
        }
        assertThat(t.cluster.counter("deletes_direct_fallback")).isEqualTo(3);
        t.assertNoViolations();
    }

    @Test
    void aTimeOutLeavesThatDeleteToTheResendTimerAndTheRestToHeartbeatReplies() throws Exception {
        twoHolders();
        ObjectDeleteSender sender = sender((url, n) -> {
            if (url.equals(url("worker2"))) {
                throw noAnswer("worker2", new SocketTimeoutException("Read timed out"));
            }
            return 204;
        }, true);

        clientDelete(sender);
        awaitCalls(4);

        Call timedOut = calls.stream().filter(c -> c.url().equals(url("worker2"))).findFirst().orElseThrow();
        assertThat(op(timedOut.chunkId(), "worker2").state()).as("it may have run: stays SENT, the resend timer has it")
                .isEqualTo(State.SENT);
        for (String chunkId : CHUNKS) {
            if (!chunkId.equals(timedOut.chunkId())) {
                assertThat(op(chunkId, "worker2").state()).isEqualTo(State.QUEUED);
            }
        }
        t.assertNoViolations();
    }

    @Test
    void anOlderWorkerWithoutTheEndpointGetsItsDeletesInHeartbeatReplies() throws Exception {
        twoHolders();
        ObjectDeleteSender sender = sender((url, n) -> url.equals(url("worker2")) ? 405 : 204, true);

        clientDelete(sender);
        awaitCalls(4);

        assertThat(t.handOut("worker2")).extracting(Command::chunkId).containsExactlyInAnyOrderElementsOf(CHUNKS);
        t.assertNoViolations();
    }

    @Test
    void aResyncingWorkerGetsNoDirectDeleteItsDeletesWaitForItsHeartbeats() throws Exception {
        twoHolders();
        t.cluster.write(() -> t.cluster.startResync(t.cluster.worker("worker2"), "test", false));

        clientDelete(sender((url, n) -> 204, true));
        awaitCalls(3);

        assertThat(calls).allMatch(c -> c.url().equals(url("worker1")));
        for (String chunkId : CHUNKS) {
            // As before: a resyncing worker is not even given queued deletes (they wait for its resync).
            assertThat(op(chunkId, "worker2").state()).as("not handed out").isNotEqualTo(State.SENT);
        }
        t.assertNoViolations();
    }

    @Test
    void switchedOffNothingIsSentDirectly() throws Exception {
        twoHolders();

        clientDelete(sender((url, n) -> 204, false));
        Thread.sleep(200);
        t.sync();

        assertThat(calls).isEmpty();
        assertThat(t.handOut("worker1")).extracting(Command::chunkId).containsExactlyInAnyOrderElementsOf(CHUNKS);
    }
}
