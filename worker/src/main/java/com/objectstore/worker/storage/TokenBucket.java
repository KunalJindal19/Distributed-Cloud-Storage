package com.objectstore.worker.storage;

/**
 * Limits the scanner's read rate: {@code bytesPerSecond} tokens are added once per second, up to
 * {@code bytesPerSecond}; the bucket starts empty, so reading {@code n} bytes takes at least
 * {@code n / bytesPerSecond - 1} seconds. A request larger than one second's worth is allowed
 * once the bucket is full and leaves a debt that later refills repay.
 */
final class TokenBucket {

    private static final long SECOND_NANOS = 1_000_000_000L;

    private final long rate;
    private long tokens;
    private long nextRefill;

    TokenBucket(long bytesPerSecond) {
        this.rate = Math.max(1, bytesPerSecond);
        this.nextRefill = System.nanoTime() + SECOND_NANOS;
    }

    void acquire(long n) throws InterruptedException {
        refill();
        while (tokens < n && tokens < rate) {
            long wait = nextRefill - System.nanoTime();
            if (wait > 0) {
                Thread.sleep(wait / 1_000_000, (int) (wait % 1_000_000));
            }
            refill();
        }
        tokens -= n;
    }

    private void refill() {
        long now = System.nanoTime();
        while (now >= nextRefill) {
            tokens = Math.min(rate, tokens + rate);
            nextRefill += SECOND_NANOS;
        }
    }
}
