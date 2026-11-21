package com.objectstore.worker.reporting;

import com.objectstore.common.dto.Change;
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
 * send {@link #suffix} runs, which are always contiguous. Each change carries the version of the
 * record it produced (v5 §3): even for the controller's operations, odd for the worker's own
 * observations.
 *
 * <table>
 *   <caption>Change types</caption>
 *   <tr><td>{@code RECEIVED(chunk, size, checksum, version[, cmd_id])}</td><td>this worker holds verified bytes
 *       of {@code chunk} at {@code version} (with a VERIFY's {@code cmd_id}: that VERIFY passed)</td></tr>
 *   <tr><td>{@code DELETED(chunk, version, detail)}</td><td>this worker no longer holds {@code chunk}
 *       ({@code deleted}, {@code corrupt} = quarantined, {@code missing})</td></tr>
 *   <tr><td>{@code CMD_FAILED(chunk, version, reason[, target])}</td><td>the operation with this version
 *       could not be carried out</td></tr>
 * </table>
 *
 * <p><b>Hold-back.</b> While a resync listing with baseline {@code M} is in progress
 * ({@link #beginHold()} .. {@link #endHold()}), {@link #suffix} returns nothing numbered above
 * {@code M}, on every channel: the listing covers everything up to {@code M}, and the controller
 * must not apply anything newer before the resync completes.
 *
 * <p><b>Overflow.</b> More than CHANGE_LOG_MAX un-acknowledged changes drop all of them and set
 * {@link #isOverflowed() overflowed}: the link then asks the controller for a resync
 * ({@code resync_wanted = overflow}), whose listing supersedes them.
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

    private synchronized Change append(Change unnumbered) {
        Change change = new Change(++lastSeq, unnumbered.type(), unnumbered.chunkId(), unnumbered.size(),
                unnumbered.checksum(), unnumbered.version(), unnumbered.cmdId(), unnumbered.target(),
                unnumbered.reason(), unnumbered.detail());
        entries.addLast(change);
        if (entries.size() > max) {
            entries.clear();
            overflowed = true;
            overflows++;
            log.warn("[link] change log overflowed ({} un-acknowledged changes); asking the controller for a resync", max);
        }
        notifyAll();
        return change;
    }

    /** This worker holds verified bytes of the chunk at {@code version} ({@code cmdId}: a VERIFY's answer). */
    public Change received(String chunkId, long size, String checksum, long version, String cmdId) {
        return append(Change.received(0, chunkId, size, checksum, version, cmdId));
    }

    /** This worker no longer holds the chunk, as of {@code version}; detail = deleted / corrupt / missing. */
    public Change deleted(String chunkId, long version, String detail) {
        return append(Change.deleted(0, chunkId, version, detail));
    }

    /** The operation with this version could not be carried out. */
    public Change cmdFailed(String chunkId, long version, String reason, String target, String cmdId, String detail) {
        return append(Change.cmdFailed(0, chunkId, version, reason, target, cmdId, detail));
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

    /** True if the log overflowed and no resync has replaced the dropped changes yet. */
    public synchronized boolean isOverflowed() {
        return overflowed;
    }

    /** A resync attempt started: its listing replaces whatever was dropped. */
    public synchronized void clearOverflow() {
        overflowed = false;
    }

    public synchronized long overflows() {
        return overflows;
    }

    /**
     * The link's early wake-up: waits until a change above {@code after} may be sent, at most until
     * {@code deadlineNanos} ({@link System#nanoTime()} scale).
     *
     * @return true if there is something to send
     */
    public synchronized boolean awaitSendable(long after, long deadlineNanos) throws InterruptedException {
        while (!hasSendable(after)) {
            long left = deadlineNanos - System.nanoTime();
            if (left <= 0) {
                return false;
            }
            TimeUnit.NANOSECONDS.timedWait(this, left);
        }
        return true;
    }
}
