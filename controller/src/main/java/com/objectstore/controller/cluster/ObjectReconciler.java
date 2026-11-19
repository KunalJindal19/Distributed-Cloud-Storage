package com.objectstore.controller.cluster;

import com.objectstore.controller.domain.Chunk;
import com.objectstore.controller.repository.ChunkRepository;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * The reconcile helper (v4 §5.9). The writer never reads SQLite, so when a {@code CommitObject} or
 * {@code DeleteObject} task throws, the writer hands the object to this helper. On a virtual
 * thread it takes the namespace lock, reads the object's chunk rows from SQLite (no cluster lock
 * held), queues {@code ApplyObjectMirror(objectId, rows)} and releases the lock. Holding the
 * namespace lock across "read + queue" keeps it ordered with any concurrent commit or delete of
 * the same object.
 */
@Component
public class ObjectReconciler {

    private static final Logger log = LoggerFactory.getLogger(ObjectReconciler.class);

    /** How the helper reads an object's rows (the database; a stub in tests). */
    @FunctionalInterface
    public interface NamespaceReader {
        List<Chunk> chunksOf(String objectId);
    }

    private final NamespaceLock namespaceLock;
    private final ApplyQueue queue;
    private final NamespaceTasks tasks;
    private final NamespaceReader reader;

    @Autowired
    public ObjectReconciler(NamespaceLock namespaceLock, ApplyQueue queue, NamespaceTasks tasks, ChunkRepository chunks) {
        this(namespaceLock, queue, tasks, chunks::findChunksByObject);
    }

    public ObjectReconciler(NamespaceLock namespaceLock, ApplyQueue queue, NamespaceTasks tasks, NamespaceReader reader) {
        this.namespaceLock = namespaceLock;
        this.queue = queue;
        this.tasks = tasks;
        this.reader = reader;
    }

    /** Starts reconciling {@code objectId}; returns at once. */
    public Thread reconcile(String objectId) {
        return Thread.ofVirtual().name("reconcile-" + objectId.substring(0, Math.min(8, objectId.length())))
                .start(() -> run(objectId));
    }

    private void run(String objectId) {
        namespaceLock.lock();
        try {
            List<Chunk> rows = reader.chunksOf(objectId);
            queue.put(tasks.applyObjectMirror(objectId, rows));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            log.error("[writer] could not reconcile object={}", objectId, e);
        } finally {
            namespaceLock.unlock();
        }
    }
}
