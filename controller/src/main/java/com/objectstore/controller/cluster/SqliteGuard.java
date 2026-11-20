package com.objectstore.controller.cluster;

import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enforces "no SQLite access on the writer thread or while holding the cluster lock" (v4 rule 4).
 * Every database connection is taken through {@link #check()}; a violation is logged and counted,
 * and in strict mode (tests) it also throws.
 */
public final class SqliteGuard {

    private static final Logger log = LoggerFactory.getLogger(SqliteGuard.class);
    private static final AtomicLong violations = new AtomicLong();
    private static volatile boolean strict;

    private SqliteGuard() {
    }

    public static void check() {
        boolean onWriter = Writer.THREAD_NAME.equals(Thread.currentThread().getName());
        boolean underLock = ClusterState.lockHeldByCurrentThread();
        if (onWriter || underLock) {
            violations.incrementAndGet();
            IllegalStateException e = new IllegalStateException("SQLite accessed " + (onWriter ? "on the writer thread"
                    : "while holding the cluster lock"));
            log.error("[writer] {}", e.getMessage(), e);
            if (strict) {
                throw e;
            }
        }
    }

    public static long violations() {
        return violations.get();
    }

    public static void setStrict(boolean on) {
        strict = on;
    }
}
