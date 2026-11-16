package com.objectstore.controller.api.dto;

/** One entry of the object listing. */
public record ObjectSummary(
        String objectId,
        String name,
        long size,
        String contentType,
        int chunkCount,
        String createdAt,
        String updatedAt) {
}
