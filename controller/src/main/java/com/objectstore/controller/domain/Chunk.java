package com.objectstore.controller.domain;

/** One piece of an object. {@code index} is its zero-based position; {@code checksum} its SHA-256. */
public record Chunk(String chunkId, String objectId, int index, int size, String checksum) {
}
