package com.objectstore.controller.domain;

import java.time.Instant;

public record Worker(String workerId, String url, WorkerStatus status, Instant lastSeen, Instant registeredAt) {

    public boolean isAlive() {
        return status == WorkerStatus.ALIVE;
    }
}
