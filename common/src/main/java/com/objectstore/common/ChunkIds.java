package com.objectstore.common;

import java.util.regex.Pattern;

/**
 * Chunk identifiers have the form {@code <object_id>-<5 digit index>}, e.g.
 * {@code 3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a-00003}.
 *
 * <p>The id is unique (the object id is a UUID), encodes the chunk's position inside the object,
 * and is safe to use as a file name: the strict pattern rules out path separators and "..".
 */
public final class ChunkIds {

    /** Highest index that fits in the five digit suffix. */
    public static final int MAX_INDEX = 99_999;

    private static final int OBJECT_ID_LENGTH = 36;
    private static final Pattern CHUNK_ID = Pattern.compile(
            "^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}-\\d{5}$");

    private ChunkIds() {
    }

    public static String of(String objectId, int index) {
        if (index < 0 || index > MAX_INDEX) {
            throw new IllegalArgumentException("chunk index out of range: " + index);
        }
        return "%s-%05d".formatted(objectId, index);
    }

    public static boolean isValid(String chunkId) {
        return chunkId != null && CHUNK_ID.matcher(chunkId).matches();
    }

    /** The id of the object a (valid) chunk id belongs to. */
    public static String objectIdOf(String chunkId) {
        requireValid(chunkId);
        return chunkId.substring(0, OBJECT_ID_LENGTH);
    }

    /** The zero-based position of a (valid) chunk id inside its object. */
    public static int indexOf(String chunkId) {
        requireValid(chunkId);
        return Integer.parseInt(chunkId.substring(OBJECT_ID_LENGTH + 1));
    }

    private static void requireValid(String chunkId) {
        if (!isValid(chunkId)) {
            throw new IllegalArgumentException("not a valid chunk id: " + chunkId);
        }
    }
}
