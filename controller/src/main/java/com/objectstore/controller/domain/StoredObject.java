package com.objectstore.controller.domain;

import java.time.Instant;

/** System-maintained metadata of one object. {@code checksum} is the SHA-256 of the whole byte stream. */
public record StoredObject(
        String objectId,
        String name,
        long size,
        String contentType,
        String checksum,
        int chunkCount,
        Instant createdAt,
        Instant updatedAt) {
}
