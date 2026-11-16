package com.objectstore.controller.api.dto;

import java.util.List;
import java.util.Map;

/**
 * Everything known about an object: system-maintained metadata, custom metadata, and for every
 * chunk its id, order, checksum and storage locations. Returned by upload, GET metadata and
 * PATCH metadata. Serialised with snake_case keys ({@code object_id}, {@code content_type}, ...).
 */
public record ObjectResponse(
        String objectId,
        String name,
        long size,
        String contentType,
        String checksum,
        int chunkCount,
        String createdAt,
        String updatedAt,
        boolean replicationOk,
        Map<String, String> customMetadata,
        List<ChunkResponse> chunks) {
}
