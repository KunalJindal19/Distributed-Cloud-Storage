package com.objectstore.worker.reporting;

import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.ChangeType;
import com.objectstore.worker.config.WorkerProperties;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * The worker's ordered change stream: every change to the disk gets the next number (the first
 * is 1 in each process) and is kept until the controller acknowledges having received it. Changes
 * travel on heartbeats and on the worker's answers to the controller's chunk PUTs; both channels
 * send {@link #suffix} runs, which are always contiguous.
 *
 * <table>
 *   <caption>Change types</caption>
 *   <tr><td>{@code RECEIVED(chunk, size, checksum)}</td><td>this worker now holds bytes of {@code chunk}
 *       that it verified, with this checksum</td></tr>
 *   <tr><td>{@code DELETED(chunk)}</td><td>this worker does not hold {@code chunk}; logged even if the
 *       chunk was already absent</td></tr>
 *   <tr><td>{@code BAD(chunk)}</td><td>this worker's copy failed verification</td></tr>
 *   <tr><td>{@code CMD_FAILED(cmdId, chunk, target, reason)}</td><td>a REPLICATE command could not be
 *       completed</td></tr>
 * </table>
 *
 * <p><b>Hold-back.</b> While a full report with baseline {@code M} is in progress
 * ({@link #beginHold()} .. {@link #endHold()}), {@link #suffix} returns nothing numbered above
 * {@code M}, on every channel: the report covers everything up to {@code M}, and the controller
 * must not apply anything newer before the report completes.
 *
 * <p><b>Overflow.</b> More than CHANGE_LOG_MAX un-acknowledged changes drop all of them and set
 * {@link #takeOverflow() overflowed}: the link then starts a full report, which supersedes them.
 *
 * <p>Every method is synchronized on the log.
 */
@Component
public class ChangeLog {

    private static final Logger log = LoggerFactory.getLogger(ChangeLog.class);

    private final int max;
    private final ArrayDeque<Change> entries = new ArrayDeque<>();
    private long lastSeq;
    private long holdLimit = Long.MAX_VALUE;
    private boolean overflowed;
    private long overflows;

    @Autowired
    public ChangeLog(WorkerProperties properties) {
        this.max = properties.changeLogMax();
    }

    /** For tests. */
    public ChangeLog(int max) {
        this.max = max;
    }

    public synchronized Change append(ChangeType type, String chunkId, Long size, String checksum, String cmdId,
                                      String target, String reason) {
        Change change = new Change(++lastSeq, type, chunkId, size, checksum, cmdId, target, reason);
        entries.addLast(change);
        if (entries.size() > max) {
            entries.clear();
            overflowed = true;
            overflows++;
            log.warn("[link] change log overflowed ({} un-acknowledged changes); a full report replaces them", max);
        }
        notifyAll();
        return change;
    }

    public Change received(String chunkId, long size, String checksum) {
        return append(ChangeType.RECEIVED, chunkId, size, checksum, null, null, null);
    }

    public Change deleted(String chunkId) {
        return append(ChangeType.DELETED, chunkId, null, null, null, null, null);
    }

    public Change bad(String chunkId) {
        return append(ChangeType.BAD, chunkId, null, null, null, null, null);
    }

    public Change cmdFailed(String cmdId, String chunkId, String target, String reason) {
        return append(ChangeType.CMD_FAILED, chunkId, null, null, cmdId, target, reason);
    }

    /** The controller has received every change up to {@code n}: forget them. Ignores {@code n > lastSeq}. */
    public synchronized void ack(long n) {
        if (n > lastSeq) {
            return;
        }
        while (!entries.isEmpty() && entries.peekFirst().seq() <= n) {
            entries.removeFirst();
        }
    }

    /** Kept changes with {@code after < seq <= holdLimit}, oldest first, at most {@code max}: a contiguous run. */
    public synchronized List<Change> suffix(long after, int maxCount) {
        List<Change> run = new ArrayList<>();
        for (Change change : entries) {
            if (run.size() >= maxCount || change.seq() > holdLimit) {
                break;
            }
            if (change.seq() > after) {
                run.add(change);
            }
        }
        return run;
    }

    /** Starts holding back: returns {@code M = lastSeq} and stops every change above it from leaving. */
    public synchronized long beginHold() {
        holdLimit = lastSeq;
        return lastSeq;
    }

    public synchronized void endHold() {
        holdLimit = Long.MAX_VALUE;
        notifyAll();
    }

    public synchronized boolean isHolding() {
        return holdLimit != Long.MAX_VALUE;
    }

    public synchronized long lastSeq() {
        return lastSeq;
    }

    /** True if changes are waiting to be sent (within the hold limit). */
    public synchronized boolean hasSendable(long after) {
        for (Change change : entries) {
            if (change.seq() > holdLimit) {
                return false;
            }
            if (change.seq() > after) {
                return true;
            }
        }
        return false;
    }

    public synchronized int size() {
        return entries.size();
    }

    /** True once if the log overflowed since the last call. */
    public synchronized boolean takeOverflow() {
        boolean was = overflowed;
        overflowed = false;
        return was;
    }

    public synchronized long overflows() {
        return overflows;
    }

    /**
     * The link's early wake-up: waits until a change above {@code after} may be sent (or the log
     * overflowed), at most until {@code deadlineNanos} ({@link System#nanoTime()} scale).
     *
     * @return true if there is something to send
     */
    public synchronized boolean awaitSendable(long after, long deadlineNanos) throws InterruptedException {
        while (!overflowed && !hasSendable(after)) {
            long left = deadlineNanos - System.nanoTime();
            if (left <= 0) {
                return false;
            }
            TimeUnit.NANOSECONDS.timedWait(this, left);
        }
        return true;
    }
}
