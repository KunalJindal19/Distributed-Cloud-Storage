package com.objectstore.controller.cluster;

import com.objectstore.common.web.ApiException;
import com.objectstore.common.web.ErrorCodes;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.domain.Worker;
import com.objectstore.controller.repository.WorkerRepository;
import com.objectstore.controller.repository.WorkerRepository.Transition;
import com.objectstore.controller.util.Timestamps;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Knows which workers exist and which of them are alive.
 *
 * <p><b>Failure detection.</b> A worker is ALIVE as long as its heartbeats arrive. The health
 * monitor marks it DEAD once it has been silent for longer than HEARTBEAT_TIMEOUT; the next
 * heartbeat or registration brings it back.
 *
 * <p><b>Verification.</b> When a worker (re)appears, the controller cannot know whether its disk
 * still holds what the database says. Each appearance therefore starts a new <em>epoch</em>, and
 * the worker counts as <em>verified</em> only after the scrubber has compared its disk with the
 * database within that epoch. Surplus copies are trimmed only when all copies involved are on
 * verified workers, so a copy is never deleted in favour of one that nobody has looked at.
 */
@Service
public class WorkerRegistry {

    private static final Logger log = LoggerFactory.getLogger(WorkerRegistry.class);

    private final WorkerRepository workers;
    private final ControllerProperties properties;
    private final Clock clock;
    private final ApplicationEventPublisher events;
    private final Instant startedAt;

    private final Map<String, Long> epochs = new ConcurrentHashMap<>();
    private final Map<String, Long> verifiedEpochs = new ConcurrentHashMap<>();

    public WorkerRegistry(WorkerRepository workers, ControllerProperties properties, Clock clock,
                          ApplicationEventPublisher events) {
        this.workers = workers;
        this.properties = properties;
        this.clock = clock;
        this.events = events;
        this.startedAt = clock.instant();
    }

    /** {@code POST /internal/workers/register}: insert-or-update, always results in ALIVE. */
    public Worker register(String workerId, String url) {
        Transition transition = workers.register(workerId, url, clock.instant());
        if (transition.isNewWorker()) {
            log.info("[registry] registered {} url={}", workerId, url);
        } else {
            log.info("[registry] re-registered {} url={}", workerId, url);
            if (transition.cameBack()) {
                log.info("[registry] {} is back", workerId);
            }
        }
        // A registration means the worker process (re)started: whatever we knew about its disk is stale.
        cameOnline(workerId);
        return transition.worker();
    }

    /** {@code POST /internal/workers/heartbeat}. Unknown workers are told to register (404). */
    public Worker heartbeat(String workerId) {
        Transition transition = workers.heartbeat(workerId, clock.instant())
                .orElseThrow(() -> ApiException.notFound(ErrorCodes.UNKNOWN_WORKER,
                        "worker '" + workerId + "' is not registered; register first"));
        if (transition.cameBack()) {
            log.info("[registry] {} is back", workerId);
            cameOnline(workerId);
        }
        return transition.worker();
    }

    /**
     * One pass of the health monitor: every ALIVE worker that has been silent for longer than
     * HEARTBEAT_TIMEOUT becomes DEAD.
     *
     * <p>Nothing is declared dead during the first STARTUP_GRACE seconds after the controller
     * itself started. At that moment every {@code last_seen} in the database is necessarily old
     * (the controller was not there to receive heartbeats), and when the whole stack is coming up
     * the workers are still booting. Without the grace period every worker would be marked DEAD
     * and "come back" a few seconds later. A worker that is really gone is still detected, as
     * soon as the grace period is over.
     */
    public List<Worker> detectDeadWorkers() {
        Instant now = clock.instant();
        Duration grace = properties.startupGrace().compareTo(properties.heartbeatTimeout()) > 0
                ? properties.startupGrace()
                : properties.heartbeatTimeout();
        if (now.isBefore(startedAt.plus(grace))) {
            return List.of();
        }
        List<Worker> dead = workers.markSilentWorkersDead(now.minus(properties.heartbeatTimeout()));
        for (Worker worker : dead) {
            log.warn("[registry] {} marked DEAD (last seen {})", worker.workerId(), Timestamps.format(worker.lastSeen()));
        }
        return dead;
    }

    public List<Worker> aliveWorkers() {
        return workers.findAlive();
    }

    public List<Worker> allWorkers() {
        return workers.findAll();
    }

    /** worker_id &rarr; number of copies recorded on that worker. */
    public Map<String, Integer> replicaCounts() {
        return workers.replicaCounts();
    }

    /** The current epoch of a worker; read it <em>before</em> starting a scrub. */
    public long epochOf(String workerId) {
        return epochs.getOrDefault(workerId, 0L);
    }

    /** Called by the scrubber after a complete pass that started in {@code epoch}. */
    public void markVerified(String workerId, long epoch) {
        verifiedEpochs.put(workerId, epoch);
    }

    /** True once the worker's disk has been compared with the database since it last appeared. */
    public boolean isVerified(String workerId) {
        Long verified = verifiedEpochs.get(workerId);
        return verified != null && verified == epochOf(workerId);
    }

    private void cameOnline(String workerId) {
        epochs.merge(workerId, 1L, Long::sum);
        events.publishEvent(new WorkerOnlineEvent(workerId));
    }
}
