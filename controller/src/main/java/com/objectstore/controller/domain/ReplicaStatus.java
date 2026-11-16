package com.objectstore.controller.domain;

/**
 * State of one physical copy of a chunk.
 *
 * <ul>
 *   <li>{@code OK}: believed intact. Copies are only ever created in this state.</li>
 *   <li>{@code CORRUPT}: the bytes on the worker no longer hash to the chunk's checksum.</li>
 *   <li>{@code MISSING}: the worker does not have the file, or could not serve it.</li>
 * </ul>
 */
public enum ReplicaStatus {
    OK, CORRUPT, MISSING
}
