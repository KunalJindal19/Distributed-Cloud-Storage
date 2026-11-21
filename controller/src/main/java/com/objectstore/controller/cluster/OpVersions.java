package com.objectstore.controller.cluster;

import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Operation versions (v5 §3.2): {@code version = (term << 40) | (seq << 1)}. They are even, so they
 * never equal a worker's odd local observation, and never reused: {@code term} is the controller
 * term from SQLite, incremented and committed on every controller start ({@link ClusterBootstrap}),
 * and {@code seq} is an in-memory counter that starts at 0 each term. {@link #next()} returns the
 * next version; {@link #current()} the highest one issued (the term base before the first
 * {@code next()}).
 *
 * <p>{@code seq} has 39 bits, over 500 billion operations per controller run. If it ever reaches
 * {@code 2^39 − 1}, {@code next()} throws, the controller logs an error and stops; the next start
 * uses a new term. There is no reset procedure.
 */
@Component
public class OpVersions {

    private static final Logger log = LoggerFactory.getLogger(OpVersions.class);
    public static final int TERM_SHIFT = 40;
    public static final long SEQ_LIMIT = (1L << 39) - 1;
    public static final long TERM_LIMIT = 1L << 23;

    private volatile long term;
    private final AtomicLong seq = new AtomicLong();
    private volatile Runnable onExhausted = () -> Thread.ofPlatform().name("versions-exhausted")
            .start(() -> System.exit(75));

    /** A Spring bean: the term is set by {@link ClusterBootstrap} before anything issues a version. */
    public OpVersions() {
    }

    /** For tests. */
    public OpVersions(long term) {
        startTerm(term);
    }

    public synchronized void startTerm(long newTerm) {
        if (newTerm <= 0 || newTerm >= TERM_LIMIT) {
            throw new IllegalStateException("controller term " + newTerm + " is out of range");
        }
        this.term = newTerm;
        this.seq.set(0);
    }

    public long term() {
        return term;
    }

    /** The next version of this term: even, larger than every version issued before (in any term). */
    public long next() {
        long t = term;
        if (t == 0) {
            throw new IllegalStateException("a version was requested before the controller term was set");
        }
        long s = seq.incrementAndGet();
        if (s >= SEQ_LIMIT) {
            log.error("[versions] FATAL the version sequence of term {} is exhausted; stopping the controller "
                    + "(the next start uses a new term)", t);
            onExhausted.run();
            throw new IllegalStateException("version sequence exhausted in term " + t);
        }
        return (t << TERM_SHIFT) | (s << 1);
    }

    /** The highest version issued so far (the term base before the first {@link #next()}). */
    public long current() {
        return (term << TERM_SHIFT) | (seq.get() << 1);
    }

    public static long termOf(long version) {
        return version >>> TERM_SHIFT;
    }

    /** Test hook: what happens when the sequence is exhausted (the default stops the process). */
    public void setOnExhaustedForTests(Runnable action) {
        this.onExhausted = action;
    }

    /** Test hook: jump the sequence (to test exhaustion). */
    public void setSeqForTests(long value) {
        seq.set(value);
    }
}
