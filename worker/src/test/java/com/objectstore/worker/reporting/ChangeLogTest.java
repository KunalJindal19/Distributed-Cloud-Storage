package com.objectstore.worker.reporting;

import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.ChangeType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** Block 12 (v4 §4.1, §4.11): numbering, acknowledgement, contiguous suffixes, hold-back, overflow. */
class ChangeLogTest {

    private static final String C = "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a-";

    private static String chunk(int i) {
        return C + "%05d".formatted(i);
    }

    private static List<Long> seqs(List<Change> changes) {
        return changes.stream().map(Change::seq).toList();
    }

    @Test
    void changesAreNumberedFromOneAndKeptUntilAcknowledged() {
        ChangeLog log = new ChangeLog(100);
        assertThat(log.received(chunk(0), 10, "a".repeat(64)).seq()).isEqualTo(1);
        assertThat(log.deleted(chunk(1)).seq()).isEqualTo(2);
        assertThat(log.bad(chunk(2)).seq()).isEqualTo(3);
        Change failed = log.cmdFailed("cmd-1", chunk(3), "http://worker2:8001", "no local copy");
        assertThat(failed.seq()).isEqualTo(4);
        assertThat(failed.type()).isEqualTo(ChangeType.CMD_FAILED);
        assertThat(failed.cmdId()).isEqualTo("cmd-1");
        assertThat(log.lastSeq()).isEqualTo(4);

        assertThat(seqs(log.suffix(0, 100))).containsExactly(1L, 2L, 3L, 4L);
        log.ack(2);
        assertThat(seqs(log.suffix(0, 100))).containsExactly(3L, 4L);
        assertThat(log.size()).isEqualTo(2);
        log.ack(1);                                         // an old acknowledgement changes nothing
        assertThat(log.size()).isEqualTo(2);
        log.ack(99);                                        // beyond lastSeq: ignored
        assertThat(log.size()).isEqualTo(2);
        log.ack(4);
        assertThat(log.size()).isZero();
        assertThat(log.deleted(chunk(5)).seq()).isEqualTo(5); // numbering never restarts
    }

    @Test
    void aSuffixIsContiguousAndBoundedByMax() {
        ChangeLog log = new ChangeLog(100);
        for (int i = 0; i < 10; i++) {
            log.deleted(chunk(i));
        }
        assertThat(seqs(log.suffix(3, 4))).containsExactly(4L, 5L, 6L, 7L);
        assertThat(seqs(log.suffix(8, 100))).containsExactly(9L, 10L);
        assertThat(log.suffix(10, 100)).isEmpty();
        assertThat(log.hasSendable(9)).isTrue();
        assertThat(log.hasSendable(10)).isFalse();
    }

    @Test
    void theHoldLimitAppliesToEveryCaller() {
        ChangeLog log = new ChangeLog(100);
        log.deleted(chunk(0));
        log.deleted(chunk(1));
        long m = log.beginHold();
        assertThat(m).isEqualTo(2);
        assertThat(log.isHolding()).isTrue();
        log.deleted(chunk(2));
        log.deleted(chunk(3));

        // The heartbeat (after the link's ack) and a PUT answer (after the controller's own ack)
        // ask with different "after" values: neither gets anything above M.
        assertThat(seqs(log.suffix(0, 100))).containsExactly(1L, 2L);
        assertThat(seqs(log.suffix(1, 100))).containsExactly(2L);
        assertThat(log.suffix(2, 100)).isEmpty();
        assertThat(log.hasSendable(2)).isFalse();

        log.endHold();
        assertThat(log.isHolding()).isFalse();
        assertThat(seqs(log.suffix(2, 100))).containsExactly(3L, 4L);
    }

    @Test
    void overflowDropsEverythingAndRequestsAReport() {
        ChangeLog log = new ChangeLog(3);
        log.deleted(chunk(0));
        log.deleted(chunk(1));
        log.deleted(chunk(2));
        assertThat(log.takeOverflow()).isFalse();
        log.bad(chunk(3));                                  // the fourth un-acknowledged change

        assertThat(log.size()).isZero();
        assertThat(log.suffix(0, 100)).isEmpty();
        assertThat(log.takeOverflow()).isTrue();
        assertThat(log.takeOverflow()).as("reported once").isFalse();
        assertThat(log.overflows()).isEqualTo(1);
        assertThat(log.lastSeq()).isEqualTo(4);            // numbering continues
        assertThat(log.deleted(chunk(4)).seq()).isEqualTo(5);
    }

    @Test
    void anAppendWakesAWaitingLink() throws Exception {
        ChangeLog log = new ChangeLog(100);
        CountDownLatch woke = new CountDownLatch(1);
        Thread link = Thread.ofPlatform().start(() -> {
            try {
                if (log.awaitSendable(0, System.nanoTime() + 10_000_000_000L)) {
                    woke.countDown();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        Thread.sleep(100);
        long started = System.nanoTime();
        log.deleted(chunk(0));
        assertThat(woke.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        assertThat(System.nanoTime() - started).isLessThan(1_000_000_000L);
        link.join();
        // A held-back change does not wake it.
        long m = log.beginHold();
        log.deleted(chunk(1));
        assertThat(log.awaitSendable(m, System.nanoTime() + 50_000_000L)).isFalse();
    }

    @Test
    void beginHoldIsAtomicWithLastSeqUnderConcurrentAppenders() throws Exception {
        ChangeLog log = new ChangeLog(Integer.MAX_VALUE);
        AtomicBoolean stop = new AtomicBoolean();
        List<Thread> appenders = new ArrayList<>();
        for (int t = 0; t < 4; t++) {
            int id = t;
            appenders.add(Thread.ofPlatform().start(() -> {
                int i = 0;
                while (!stop.get()) {
                    log.deleted(chunk((id * 10_000 + i++) % 100_000));
                }
            }));
        }
        long acked = 0;
        try {
            for (int iteration = 0; iteration < 100_000; iteration++) {
                long m = log.beginHold();
                List<Change> run = log.suffix(acked, Integer.MAX_VALUE);
                // Exactly the changes acked+1 .. M, contiguous: nothing above M, nothing missing below it.
                if (m > acked) {
                    assertThat(run).hasSize((int) (m - acked));
                    assertThat(run.getFirst().seq()).isEqualTo(acked + 1);
                    assertThat(run.getLast().seq()).isEqualTo(m);
                } else {
                    assertThat(run).isEmpty();
                }
                log.ack(m);
                acked = m;
                log.endHold();
            }
        } finally {
            stop.set(true);
            for (Thread appender : appenders) {
                appender.join();
            }
        }
        assertThat(log.lastSeq()).isGreaterThanOrEqualTo(acked);
    }
}
