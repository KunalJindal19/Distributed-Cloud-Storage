package com.objectstore.controller.domain;

/** ALIVE while heartbeats keep arriving; DEAD after HEARTBEAT_TIMEOUT of silence. */
public enum WorkerStatus {
    ALIVE, DEAD
}
