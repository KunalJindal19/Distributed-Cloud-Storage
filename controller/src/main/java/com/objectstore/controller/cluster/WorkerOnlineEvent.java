package com.objectstore.controller.cluster;

/**
 * Published when a worker registers or returns from the dead. What it has on disk is unknown at
 * that moment, so the scrubber checks it straight away instead of waiting for its next round.
 */
public record WorkerOnlineEvent(String workerId) {
}
