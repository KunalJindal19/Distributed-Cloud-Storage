package com.objectstore.controller.replication;

import com.objectstore.controller.cluster.Registry;
import com.objectstore.controller.config.ControllerProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * The controller's two background loops, each a plain "sleep, then one pass under the cluster
 * lock" cycle on its own thread:
 *
 * <ul>
 *   <li><b>health</b>, every HEALTH_INTERVAL: STALE and DEAD detection, with stall protection;</li>
 *   <li><b>monitor</b>, every MONITOR_INTERVAL: safe mode, time-outs, replication, invariants.</li>
 * </ul>
 *
 * A loop never overlaps itself, and a failing pass is logged and the loop carries on.
 */
@Component
public class MaintenanceLoops implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceLoops.class);

    private final ControllerProperties properties;
    private final Registry registry;
    private final Monitor monitor;
    private final List<Thread> threads = new ArrayList<>();
    private volatile boolean running;

    public MaintenanceLoops(ControllerProperties properties, Registry registry, Monitor monitor) {
        this.properties = properties;
        this.registry = registry;
        this.monitor = monitor;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        threads.add(loop("health-loop", properties.healthInterval(), registry::healthPass));
        threads.add(loop("monitor-loop", properties.monitorInterval(), monitor::tick));
        log.info("[api] background loops started: health every {}s, monitor every {}s",
                properties.healthIntervalSeconds(), properties.monitorIntervalSeconds());
    }

    @Override
    public synchronized void stop() {
        running = false;
        threads.forEach(Thread::interrupt);
        threads.clear();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private Thread loop(String name, Duration interval, Runnable pass) {
        return Thread.ofPlatform().name(name).daemon(true).start(() -> {
            while (running) {
                try {
                    Thread.sleep(interval);
                    pass.run();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Throwable t) {
                    log.error("[{}] pass failed; will try again in {}s", name, interval.toMillis() / 1000.0, t);
                }
            }
        });
    }
}
