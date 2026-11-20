package com.objectstore.systemtest;

import com.objectstore.controller.ControllerApplication;
import com.objectstore.worker.WorkerApplication;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * A complete cluster inside one JVM: the real controller application and four real worker
 * applications, each its own Spring context with its own port and data directory, talking
 * HTTP to each other exactly as the containers do. Timers are scaled down roughly tenfold.
 *
 * <p>Stopping a worker closes its context (like {@code docker stop}); starting it again boots a
 * new process on the same port and volume, with a new boot id.
 */
public final class InProcessCluster implements AutoCloseable {

    /** Production timers divided by about ten. */
    public static final Map<String, String> CONTROLLER_TIMERS = Map.ofEntries(
            Map.entry("objectstore.stale-after-seconds", "0.8"),
            Map.entry("objectstore.dead-after-seconds", "1.6"),
            Map.entry("objectstore.health-interval-seconds", "0.1"),
            Map.entry("objectstore.monitor-interval-seconds", "0.3"),
            Map.entry("objectstore.pending-timeout-seconds", "2"),
            Map.entry("objectstore.target-backoff-seconds", "3"),
            Map.entry("objectstore.command-ttl-seconds", "4"),
            Map.entry("objectstore.max-message-age-seconds", "3"),
            Map.entry("objectstore.safe-mode-extension-seconds", "0.3"),
            Map.entry("objectstore.safe-mode-timeout-seconds", "4"),
            Map.entry("objectstore.orphan-gc-delay-seconds", "2"),
            Map.entry("objectstore.full-report-interval-seconds", "30"),
            Map.entry("objectstore.read-timeout-seconds", "3"));

    public static final Map<String, String> WORKER_TIMERS = Map.ofEntries(
            Map.entry("worker.heartbeat-interval-seconds", "0.2"),
            Map.entry("worker.heartbeat-http-timeout-seconds", "0.5"),
            Map.entry("worker.full-report-interval-seconds", "30"),
            Map.entry("worker.heartbeat-min-gap-ms", "25"),
            Map.entry("worker.scan-interval-seconds", "0.5"),
            Map.entry("worker.disk-check-interval-seconds", "1"),
            Map.entry("worker.read-timeout-seconds", "3"));

    private final Path root;
    private final int controllerPort;
    private final Map<String, Integer> workerPorts = new LinkedHashMap<>();
    private final Map<String, ConfigurableApplicationContext> workers = new LinkedHashMap<>();
    private final Map<String, String> controllerOverrides = new LinkedHashMap<>();
    private final Map<String, String> workerOverrides = new LinkedHashMap<>();
    private ConfigurableApplicationContext controller;
    private Path dbFile;

    public InProcessCluster(int workerCount) {
        try {
            this.root = Files.createTempDirectory("objectstore-system-test");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.controllerPort = freePort();
        this.dbFile = root.resolve("controller").resolve("meta.db");
        for (int i = 1; i <= workerCount; i++) {
            workerPorts.put("worker" + i, freePort());
        }
    }

    /** Overrides a controller setting for the next controller start. */
    public InProcessCluster controllerSetting(String key, String value) {
        controllerOverrides.put(key, value);
        return this;
    }

    /** Overrides a worker setting (for every worker) for the next worker starts. */
    public InProcessCluster workerSetting(String key, String value) {
        workerOverrides.put(key, value);
        return this;
    }

    /** A bean of the running controller, e.g. its {@code Writer} or {@code ClusterState}. */
    public <T> T controllerBean(Class<T> type) {
        return controller.getBean(type);
    }

    /** A bean of a running worker, e.g. its {@code ControllerLink}. */
    public <T> T workerBean(String workerId, Class<T> type) {
        return workers.get(workerId).getBean(type);
    }

    public InProcessCluster startAll() {
        startController();
        workerPorts.keySet().forEach(this::startWorker);
        return this;
    }

    public void startController() {
        Map<String, String> settings = new LinkedHashMap<>(CONTROLLER_TIMERS);
        settings.put("server.port", String.valueOf(controllerPort));
        settings.put("server.shutdown", "immediate");
        settings.put("objectstore.db-path", dbFile.toString());
        settings.putAll(controllerOverrides);
        controller = start(ControllerApplication.class, ControllerApplication.CONFIG_NAME, settings);
    }

    public void stopController() {
        if (controller != null) {
            controller.close();
            controller = null;
        }
    }

    /** Points the next controller start at a brand-new, empty database (a wiped controller volume). */
    public void wipeControllerDatabase() {
        dbFile = root.resolve("controller-" + System.nanoTime()).resolve("meta.db");
    }

    public void startWorker(String workerId) {
        int port = workerPorts.get(workerId);
        Map<String, String> settings = new LinkedHashMap<>(WORKER_TIMERS);
        settings.put("server.port", String.valueOf(port));
        settings.put("server.shutdown", "immediate");
        settings.put("worker.id", workerId);
        settings.put("worker.data-dir", dataDir(workerId).toString());
        settings.put("worker.controller-url", controllerUrl());
        settings.put("worker.advertise-url", "http://127.0.0.1:" + port);
        settings.putAll(workerOverrides);
        workers.put(workerId, start(WorkerApplication.class, WorkerApplication.CONFIG_NAME, settings));
    }

    public void stopWorker(String workerId) {
        ConfigurableApplicationContext context = workers.remove(workerId);
        if (context != null) {
            context.close();
        }
    }

    public boolean isRunning(String workerId) {
        return workers.containsKey(workerId);
    }

    public List<String> workerIds() {
        return new ArrayList<>(workerPorts.keySet());
    }

    public String controllerUrl() {
        return "http://127.0.0.1:" + controllerPort;
    }

    public String workerUrl(String workerId) {
        return "http://127.0.0.1:" + workerPorts.get(workerId);
    }

    public Path dataDir(String workerId) {
        return root.resolve(workerId);
    }

    public Path chunkFile(String workerId, String chunkId) {
        return dataDir(workerId).resolve("chunks").resolve(chunkId);
    }

    /** Chunk data files (not sidecars) currently on a worker's disk. */
    public List<String> chunkFilesOn(String workerId) {
        Path dir = dataDir(workerId).resolve("chunks");
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(dir)) {
            return files.map(p -> p.getFileName().toString())
                    .filter(name -> !name.endsWith(".meta") && !name.endsWith(".tmp"))
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        new ArrayList<>(workers.keySet()).forEach(this::stopWorker);
        stopController();
    }

    private static ConfigurableApplicationContext start(Class<?> application, String configName, Map<String, String> settings) {
        List<String> args = new ArrayList<>();
        args.add("--spring.config.name=" + configName);
        settings.forEach((key, value) -> args.add("--" + key + "=" + value));
        return new SpringApplicationBuilder(application).run(args.toArray(String[]::new));
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
