package com.objectstore.controller.api.dto;

/** One entry of the object listing. {@code owner} is the email of the account that uploaded it. */
public record ObjectSummary(
        String objectId,
        String name,
        String owner,
        long size,
        String contentType,
        int chunkCount,
        String createdAt,
        String updatedAt) {
}
