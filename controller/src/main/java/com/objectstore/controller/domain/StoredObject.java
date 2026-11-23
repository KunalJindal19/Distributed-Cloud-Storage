package com.objectstore.controller.domain;

import java.time.Instant;

/**
 * System-maintained metadata of one object. {@code checksum} is the SHA-256 of the whole byte stream.
 * {@code ownerId} is the account that uploaded it (only the public API looks at it).
 */
public record StoredObject(
        String objectId,
        String name,
        long size,
        String contentType,
        String checksum,
        int chunkCount,
        Instant createdAt,
        Instant updatedAt,
        String ownerId) {
}
