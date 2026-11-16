package com.objectstore.controller.replication;

import com.objectstore.controller.cluster.WorkerRegistry;
import com.objectstore.controller.config.ControllerProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * The three background loops of the controller, each on its own thread:
 *
 * <ul>
 *   <li><b>health monitor</b>, every HEALTH_INTERVAL: declares silent workers DEAD;</li>
 *   <li><b>repair</b>, every REPAIR_INTERVAL: restores the replication factor;</li>
 *   <li><b>scrub</b>, every SCRUB_INTERVAL: compares the workers' disks with the database.</li>
 * </ul>
 *
 * Each loop is a plain "run, then sleep" cycle, so a loop can never overlap itself and a slow
 * pass of one loop never delays another. A failing pass is logged and the loop carries on.
 */
@Component
public class MaintenanceLoops implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceLoops.class);

    private final ControllerProperties properties;
    private final WorkerRegistry registry;
    private final RepairService repair;
    private final ScrubService scrub;

    private final List<Thread> threads = new ArrayList<>();
    private volatile boolean running;

    public MaintenanceLoops(ControllerProperties properties, WorkerRegistry registry, RepairService repair,
                            ScrubService scrub) {
        this.properties = properties;
        this.registry = registry;
        this.repair = repair;
        this.scrub = scrub;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        threads.add(loop("health-monitor", properties.healthInterval(), registry::detectDeadWorkers));
        threads.add(loop("repair-loop", properties.repairInterval(), repair::runOnce));
        threads.add(loop("scrub-loop", properties.scrubInterval(), scrub::runOnce));
        log.info("[api] background loops started: health every {}s, repair every {}s, scrub every {}s",
                properties.healthIntervalSeconds(), properties.repairIntervalSeconds(),
                properties.scrubIntervalSeconds());
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
        return Thread.ofVirtual().name(name).start(() -> {
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
