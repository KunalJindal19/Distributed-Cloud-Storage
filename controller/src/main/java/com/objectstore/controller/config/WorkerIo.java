package com.objectstore.controller.config;

import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.springframework.stereotype.Component;

/**
 * Runs blocking calls to workers concurrently, one virtual thread per call. This is how the
 * copies of a chunk are written in parallel and how an object's chunks are deleted in one go.
 */
@Component
public class WorkerIo {

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    /** Fire-and-forget. */
    public void execute(Runnable task) {
        executor.execute(task);
    }

    /**
     * Applies {@code task} to every item concurrently and waits for all of them. Results are in
     * the order of {@code items}. An exception thrown by a task is rethrown to the caller once
     * all tasks have finished.
     */
    public <T, R> List<R> mapAll(Collection<T> items, Function<T, R> task) {
        List<Future<R>> futures = new ArrayList<>(items.size());
        for (T item : items) {
            futures.add(executor.submit(() -> task.apply(item)));
        }
        List<R> results = new ArrayList<>(items.size());
        RuntimeException failure = null;
        for (Future<R> future : futures) {
            try {
                results.add(future.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while waiting for worker calls", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                RuntimeException wrapped = cause instanceof RuntimeException runtime
                        ? runtime
                        : new IllegalStateException(cause);
                if (failure == null) {
                    failure = wrapped;
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
        return results;
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }
}
