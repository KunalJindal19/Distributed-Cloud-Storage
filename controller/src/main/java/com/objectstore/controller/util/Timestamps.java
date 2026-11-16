package com.objectstore.controller.util;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Renders instants as ISO-8601 UTC with a fixed number of digits, e.g.
 * {@code 2026-11-16T06:10:00.123Z}. Because the width never varies, two timestamps can be
 * compared as plain strings.
 */
public final class Timestamps {

    private static final DateTimeFormatter ISO_MILLIS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    private Timestamps() {
    }

    public static String format(Instant instant) {
        return ISO_MILLIS.format(instant);
    }
}
