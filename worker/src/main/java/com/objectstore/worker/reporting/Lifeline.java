package com.objectstore.worker.reporting;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.web.ErrorCodes;
import com.objectstore.worker.commands.CommandExecutor;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.storage.ChunkStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import tools.jackson.databind.ObjectMapper;

/**
 * The heartbeat loop (HDFS lifeline). Every HEARTBEAT_INTERVAL it tells the controller "I am
 * alive, here are my statistics" and receives the commands the controller has for this worker.
 * There is no retry inside a tick: the next tick is the retry. It never waits for a command to
 * finish, and it runs on its own HTTP client, so nothing else can delay it.
 */
@Component
public class Lifeline implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Lifeline.class);
    private static final long DOWN_LOG_PERIOD_NANOS = 30_000_000_000L;

    private final WorkerProperties properties;
    private final WorkerIdentity identity;
    private final ControllerClients clients;
    private final ChunkStore store;
    private final CommandExecutor executor;
    private final Reporter reporter;
    private final ObjectMapper json;
    private long controllerDownSince;
    private long lastDownLog;

    public Lifeline(WorkerProperties properties, WorkerIdentity identity, ControllerClients clients, ChunkStore store,
                    CommandExecutor executor, Reporter reporter, ObjectMapper json) {
        this.properties = properties;
        this.identity = identity;
        this.clients = clients;
        this.store = store;
        this.executor = executor;
        this.reporter = reporter;
        this.json = json;
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(properties.heartbeatInterval());
                if (identity.isRegistered()) {
                    beat();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void beat() {
        HeartbeatRequest request = new HeartbeatRequest(properties.id(), identity.bootId(),
                identity.nextHeartbeatSeq(), InternalApi.nowSeconds(), store.stats(executor.activeTransfers()));
        try {
            HeartbeatResponse response = clients.lifeline().post()
                    .uri(InternalApi.HEARTBEAT_PATH)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(request)
                    .retrieve()
                    .body(HeartbeatResponse.class);
            if (controllerDownSince != 0) {
                log.info("[heartbeat] controller reachable again");
                controllerDownSince = 0;
            }
            if (response == null || response.ignored() != null) {
                return;
            }
            if (response.commands() != null) {
                for (Command command : response.commands()) {
                    executor.submit(command);
                }
            }
            if (response.requestFullReport()) {
                reporter.requestFullReport();
            }
        } catch (HttpClientErrorException e) {
            String code = errorCode(e);
            if (e.getStatusCode().value() == 404 || ErrorCodes.STALE_BOOT.equals(code)) {
                log.warn("[heartbeat] controller does not know this process ({}); registering again", code);
                identity.setRegistered(false);
            } else {
                log.warn("[heartbeat] rejected: {} {}", e.getStatusCode().value(), e.getResponseBodyAsString());
            }
        } catch (RestClientException e) {
            long now = System.nanoTime();
            if (controllerDownSince == 0 || now - lastDownLog > DOWN_LOG_PERIOD_NANOS) {
                log.warn("[heartbeat] controller unreachable ({}); will keep trying", Reporter.rootMessage(e));
                lastDownLog = now;
            }
            if (controllerDownSince == 0) {
                controllerDownSince = now;
            }
        }
    }

    private String errorCode(HttpClientErrorException e) {
        try {
            return json.readTree(e.getResponseBodyAsString()).path("error").asString(null);
        } catch (RuntimeException notJson) {
            return null;
        }
    }
}
