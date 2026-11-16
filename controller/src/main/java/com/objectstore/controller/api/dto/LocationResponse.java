package com.objectstore.controller.api.dto;

/** Where one copy of a chunk is stored, the state of that copy, and whether its worker is up. */
public record LocationResponse(String workerId, String status, String workerStatus) {
}
