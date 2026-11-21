package com.objectstore.controller.cluster;

import com.objectstore.common.dto.Command;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * One worker's queue of commands waiting for its next heartbeat. It keeps a dedupe index
 * {@code (type, chunk, target) → command}, so a duplicate check is O(1), and it never drops a command
 * by itself: the cap MAX_COMMANDS_PER_WORKER is enforced by the producers (a pending op waits, the
 * planner does not schedule a copy from a full source). The hand-out filter ({@link #drain}) may leave
 * commands queued (their target is resyncing) or drop ones whose op is gone. Guarded by the cluster lock.
 */
public final class CommandQueue implements Iterable<Command> {

    private final ArrayDeque<Command> commands = new ArrayDeque<>();
    private final Map<String, Command> index = new HashMap<>();

    static String key(String type, String chunkId, String targetUrl) {
        return type + "|" + chunkId + "|" + Objects.toString(targetUrl, "");
    }

    private static String key(Command command) {
        return key(command.type(), command.chunkId(), command.targetUrl());
    }

    /** The queued identical command (same type, chunk and target), or null. */
    public Command find(String type, String chunkId, String targetUrl) {
        return index.get(key(type, chunkId, targetUrl));
    }

    public void add(Command command) {
        commands.addLast(command);
        index.put(key(command), command);
    }

    Command poll() {
        Command command = commands.pollFirst();
        if (command != null) {
            index.remove(key(command), command);
        }
        return command;
    }

    /** Removes the command with this id; true if it was queued. */
    public boolean remove(String cmdId) {
        return removeIf(command -> command.cmdId().equals(cmdId)) > 0;
    }

    int removeIf(Predicate<Command> filter) {
        int removed = 0;
        for (Iterator<Command> it = commands.iterator(); it.hasNext(); ) {
            Command command = it.next();
            if (filter.test(command)) {
                it.remove();
                index.remove(key(command), command);
                removed++;
            }
        }
        return removed;
    }

    void clear() {
        commands.clear();
        index.clear();
    }

    /** What {@link #drain} does with one queued command. */
    public enum Decision { TAKE, KEEP, DROP }

    /**
     * Walks the queue in order and takes (or drops) commands as {@code decide} says, keeping the rest
     * in place (the hand-out filter, v5 §5.2), until {@code max} commands were taken.
     */
    public java.util.List<Command> drain(java.util.function.Function<Command, Decision> decide, int max) {
        java.util.List<Command> taken = new java.util.ArrayList<>();
        for (Iterator<Command> it = commands.iterator(); it.hasNext() && taken.size() < max; ) {
            Command command = it.next();
            Decision decision = decide.apply(command);
            if (decision == Decision.KEEP) {
                continue;
            }
            it.remove();
            index.remove(key(command), command);
            if (decision == Decision.TAKE) {
                taken.add(command);
            }
        }
        return taken;
    }

    public int size() {
        return commands.size();
    }

    public boolean isEmpty() {
        return commands.isEmpty();
    }

    @Override
    public Iterator<Command> iterator() {
        return java.util.Collections.unmodifiableCollection(commands).iterator();
    }

    public java.util.stream.Stream<Command> stream() {
        return commands.stream();
    }
}
