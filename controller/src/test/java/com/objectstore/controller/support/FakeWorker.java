package com.objectstore.controller.support;

import com.objectstore.common.Checksums;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * An in-memory stand-in for a storage worker. It implements the worker side of the internal API
 * faithfully (checksum verification on PUT, checksum of the stored bytes on GET and in listings),
 * and adds what a test needs: stopping and restarting it, and damaging what it stores.
 */
public final class FakeWorker {

    private final String id;
    private final Map<String, byte[]> files = new ConcurrentHashMap<>();
    private final Set<String> unreadable = ConcurrentHashMap.newKeySet();
    private final AtomicInteger chunkRequests = new AtomicInteger();
    private final AtomicInteger deleteRequests = new AtomicInteger();
    private volatile long writeDelayMillis;
    private volatile long deleteDelayMillis;
    private HttpServer server;
    private int port;
    private volatile boolean running;

    public FakeWorker(String id) {
        this.id = id;
    }

    public synchronized void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        port = server.getAddress().getPort();
        server.createContext("/", this::handle);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        running = true;
    }

    /** Like {@code docker stop}: the process is gone, what is on its "disk" stays. */
    public synchronized void stop() {
        running = false;
        if (server != null) {
            server.stop(0);
            server = null;
        }
    }

    public boolean isRunning() {
        return running;
    }

    public String id() {
        return id;
    }

    public String url() {
        return "http://127.0.0.1:" + port;
    }

    /** chunk id &rarr; stored bytes; a live view of the worker's "disk". */
    public Map<String, byte[]> files() {
        return files;
    }

    public Set<String> chunkIdsOf(String objectId) {
        return files.keySet().stream().filter(chunkId -> chunkId.startsWith(objectId)).collect(Collectors.toSet());
    }

    /** Flips one byte in place, exactly what the demo's "make corrupt" does on a real worker. */
    public void corrupt(String chunkId) {
        files.computeIfPresent(chunkId, (key, data) -> {
            byte[] damaged = data.clone();
            damaged[Math.min(100, damaged.length - 1)] ^= 0x5A;
            return damaged;
        });
    }

    /** Makes GETs of this chunk fail with 500 while leaving the stored bytes intact. */
    public void failReadsOf(String chunkId) {
        unreadable.add(chunkId);
    }

    /** Makes every chunk PUT take at least this long, like a slow disk or network. */
    public void delayWrites(long millis) {
        writeDelayMillis = millis;
    }

    /** Makes every chunk DELETE take at least this long. */
    public void delayDeletes(long millis) {
        deleteDelayMillis = millis;
    }

    /** Number of chunk DELETE requests received so far. */
    public int deleteRequests() {
        return deleteRequests.get();
    }

    /** Back to a well-behaved worker: reads work again and nothing is delayed. */
    public void heal() {
        unreadable.clear();
        writeDelayMillis = 0;
        deleteDelayMillis = 0;
    }

    /** Number of chunk PUT/GET/DELETE requests received so far (listings are not counted). */
    public int chunkRequests() {
        return chunkRequests.get();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
            String path = exchange.getRequestURI().getPath();
            if (path.equals("/internal/workers/" + id + "/chunks") && method.equals("GET")) {
                list(exchange);
            } else if (path.startsWith("/internal/chunks/")) {
                chunkRequests.incrementAndGet();
                String chunkId = path.substring("/internal/chunks/".length());
                switch (method) {
                    case "PUT" -> put(exchange, chunkId);
                    case "GET" -> get(exchange, chunkId);
                    case "DELETE" -> delete(exchange, chunkId);
                    default -> error(exchange, 405, "method_not_allowed");
                }
            } else {
                error(exchange, 404, "not_found");
            }
        }
    }

    private void put(HttpExchange exchange, String chunkId) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        pause(writeDelayMillis);
        String expected = exchange.getRequestHeaders().getFirst("X-Checksum");
        String actual = Checksums.sha256Hex(body);
        if (!actual.equals(expected)) {
            error(exchange, 400, "checksum_mismatch");
            return;
        }
        byte[] existing = files.get(chunkId);
        if (existing != null && !Checksums.sha256Hex(existing).equals(expected)) {
            error(exchange, 409, "conflict");
            return;
        }
        files.put(chunkId, body);
        json(exchange, existing == null ? 201 : 200,
                "{\"chunk_id\":\"%s\",\"size\":%d,\"checksum\":\"%s\"}".formatted(chunkId, body.length, actual));
    }

    private void get(HttpExchange exchange, String chunkId) throws IOException {
        byte[] data = files.get(chunkId);
        if (data == null) {
            error(exchange, 404, "chunk_not_found");
            return;
        }
        if (unreadable.contains(chunkId)) {
            error(exchange, 500, "internal_error");
            return;
        }
        exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
        exchange.getResponseHeaders().add("X-Checksum", Checksums.sha256Hex(data));
        exchange.sendResponseHeaders(200, data.length == 0 ? -1 : data.length);
        if (data.length > 0) {
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(data);
            }
        }
    }

    private void delete(HttpExchange exchange, String chunkId) throws IOException {
        deleteRequests.incrementAndGet();
        pause(deleteDelayMillis);
        if (files.remove(chunkId) == null) {
            error(exchange, 404, "chunk_not_found");
        } else {
            exchange.sendResponseHeaders(204, -1);
        }
    }

    private void list(HttpExchange exchange) throws IOException {
        String chunks = new TreeMap<>(files).entrySet().stream()
                .map(e -> "{\"chunk_id\":\"%s\",\"size\":%d,\"checksum\":\"%s\"}"
                        .formatted(e.getKey(), e.getValue().length, Checksums.sha256Hex(e.getValue())))
                .collect(Collectors.joining(","));
        json(exchange, 200, "{\"worker_id\":\"%s\",\"chunks\":[%s]}".formatted(id, chunks));
    }

    private static void pause(long millis) {
        if (millis > 0) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void error(HttpExchange exchange, int status, String code) throws IOException {
        json(exchange, status, "{\"error\":\"%s\",\"detail\":\"fake worker\"}".formatted(code));
    }

    private static void json(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
