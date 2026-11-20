package com.objectstore.controller.cluster;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * One unit of work for the {@link Writer}: everything that changes the block map goes through one
 * of these, in queue order. A task runs under the cluster write lock; a long one does a bounded
 * step and answers {@link Step#MORE}, and the writer resumes it, after briefly releasing the
 * lock, before it takes any other task (the continuation keeps its queue position).
 *
 * <p>A task that concerns a worker carries the worker's epoch and checks it when it runs: work
 * that belongs to an earlier life of the worker is dropped.
 */
public final class Task {

    public enum Step { DONE, MORE }

    /** The body: does (part of) the work before {@code deadlineNanos} and says whether more is left. */
    @FunctionalInterface
    public interface Body {
        Step run(long deadlineNanos);
    }

    final String name;
    final Body body;
    final long queuedAt = System.nanoTime();
    /** If this task changes the namespace mirror of an object and fails, that object is reconciled. */
    final String reconcileObjectId;
    final CompletableFuture<Object> future = new CompletableFuture<>();
    Object result;

    private Task(String name, Body body, String reconcileObjectId) {
        this.name = name;
        this.body = body;
        this.reconcileObjectId = reconcileObjectId;
    }

    /** A one-step task. */
    public static Task of(String name, Runnable action) {
        return new Task(name, deadline -> {
            action.run();
            return Step.DONE;
        }, null);
    }

    /** A one-step task whose value completes its future. */
    public static <T> Task call(String name, Supplier<T> action) {
        Task[] self = new Task[1];
        self[0] = new Task(name, deadline -> {
            self[0].result = action.get();
            return Step.DONE;
        }, null);
        return self[0];
    }

    /** A multi-step task. */
    public static Task steps(String name, Body body) {
        return new Task(name, body, null);
    }

    /** A multi-step task that changes the namespace mirror of {@code objectId} (CommitObject, DeleteObject). */
    public static Task namespace(String name, String objectId, Body body) {
        return new Task(name, body, objectId);
    }

    /** A no-op whose completion means every task queued before it has been applied. */
    public static Task barrier() {
        return of("Barrier", () -> { });
    }

    public String name() {
        return name;
    }

    public CompletableFuture<Object> future() {
        return future;
    }

    long ageMillis() {
        return (System.nanoTime() - queuedAt) / 1_000_000;
    }

    @Override
    public String toString() {
        return name;
    }
}
