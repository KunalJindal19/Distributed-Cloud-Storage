package com.objectstore.common.dto;

/**
 * The kinds of change a worker reports about its disk, in its ordered change stream. Every change
 * carries the version of the record it produced (v5 §3): an even version is the controller's
 * operation version, an odd one is the worker's own observation.
 *
 * <ul>
 *   <li>{@code RECEIVED(chunk, size, checksum, version[, cmd_id])}: this worker now holds verified
 *       bytes of {@code chunk} at {@code version}. With the {@code cmd_id} of a VERIFY it is that
 *       VERIFY's answer.</li>
 *   <li>{@code DELETED(chunk, version, detail)}: this worker no longer holds {@code chunk};
 *       {@code detail} is {@code deleted}, {@code corrupt} (quarantined) or {@code missing}.</li>
 *   <li>{@code CMD_FAILED(chunk, version, reason[, target])}: the operation with this version could
 *       not be carried out ({@code stale}, {@code busy}, {@code io_error}, {@code no_local_copy},
 *       {@code local_copy_corrupt}, {@code transfer_failed}, {@code bad_command}).</li>
 * </ul>
 */
public enum ChangeType { RECEIVED, DELETED, CMD_FAILED }
