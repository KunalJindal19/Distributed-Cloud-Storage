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
 * Runs blocking calls to workers concurrently, one virtual thread per call: the copies of a chunk
 * are written in parallel, and fsck asks every worker at once.
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
     * the order of {@code items}; a task that threw yields {@code null}.
     */
    public <T, R> List<R> mapAll(Collection<T> items, Function<T, R> task) {
        List<Future<R>> futures = new ArrayList<>(items.size());
        for (T item : items) {
            futures.add(executor.submit(() -> task.apply(item)));
        }
        List<R> results = new ArrayList<>(items.size());
        for (Future<R> future : futures) {
            try {
                results.add(future.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while waiting for worker calls", e);
            } catch (ExecutionException e) {
                results.add(null);
            }
        }
        return results;
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }
}
