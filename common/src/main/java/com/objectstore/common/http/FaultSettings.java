package com.objectstore.common.http;

/**
 * Fault injection for internal HTTP calls (FAULT_DELAY_MS, FAULT_DROP_RATE, FAULT_DUP_RATE).
 * All zero by default; only the chaos test turns them on.
 *
 * @param delayMs  each call first waits a random 0..delayMs milliseconds
 * @param dropRate probability a call is lost: half the time before it is sent, half the time after
 *                 (the remote side acted, but the response is lost)
 * @param dupRate  probability a call is sent twice; the second response is used
 */
public record FaultSettings(long delayMs, double dropRate, double dupRate) {

    public static final FaultSettings NONE = new FaultSettings(0, 0, 0);

    public boolean enabled() {
        return delayMs > 0 || dropRate > 0 || dupRate > 0;
    }

    public static FaultSettings fromEnvironment() {
        return new FaultSettings(
                (long) number("FAULT_DELAY_MS"), number("FAULT_DROP_RATE"), number("FAULT_DUP_RATE"));
    }

    private static double number(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return 0;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be a number, got '" + value + "'", e);
        }
    }
}
