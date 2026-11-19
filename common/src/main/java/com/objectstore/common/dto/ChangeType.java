package com.objectstore.common.dto;

/**
 * The kinds of change a worker reports about its disk, in its ordered change stream.
 *
 * <ul>
 *   <li>{@code RECEIVED(chunk, size, checksum)}: this worker now holds bytes of {@code chunk} that
 *       it verified, with this checksum.</li>
 *   <li>{@code DELETED(chunk)}: this worker does not hold {@code chunk} (logged even if the chunk
 *       was already absent).</li>
 *   <li>{@code BAD(chunk)}: this worker's copy failed verification.</li>
 *   <li>{@code CMD_FAILED(cmdId, chunk, target, reason)}: a REPLICATE command could not be
 *       completed.</li>
 * </ul>
 */
public enum ChangeType { RECEIVED, DELETED, BAD, CMD_FAILED }
