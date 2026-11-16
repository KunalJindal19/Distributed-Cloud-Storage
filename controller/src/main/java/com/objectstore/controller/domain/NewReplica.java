package com.objectstore.controller.domain;

/** A copy that has just been written to a worker and is about to be recorded. */
public record NewReplica(String chunkId, String workerId) {
}
