package com.objectstore.controller.objects;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * The objects that are being uploaded right now.
 *
 * <p>During an upload chunk files already exist on workers while the database does not know the
 * object yet (it is recorded in a single transaction at the very end). To the scrubber such files
 * look exactly like orphans. This registry is how it tells "not recorded <em>yet</em>" from
 * "left over", so it never garbage-collects an upload in progress.
 *
 * <p>Protocol: the uploader calls {@link #begin} before its first write and {@link #end} only
 * after it has either committed the object or cleaned up after a failure. The scrubber takes a
 * {@link #snapshot} <em>before</em> it reads the database. A file whose object is neither in the
 * snapshot nor in the database then really is an orphan.
 */
@Component
public class InFlightUploads {

    private final Set<String> objectIds = ConcurrentHashMap.newKeySet();

    public void begin(String objectId) {
        objectIds.add(objectId);
    }

    public void end(String objectId) {
        objectIds.remove(objectId);
    }

    public Set<String> snapshot() {
        return Set.copyOf(objectIds);
    }
}
