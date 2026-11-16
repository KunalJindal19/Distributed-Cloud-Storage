package com.objectstore.worker.registration;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.AckResponse;
import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.RegisterRequest;
import com.objectstore.worker.config.WorkerProperties;
import jakarta.annotation.PreDestroy;
import java.net.http.HttpClient;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * Keeps the controller informed that this worker exists and is alive.
 *
 * <p>Once the HTTP server is accepting requests the worker registers (retrying until the
 * controller answers), then sends a heartbeat every HEARTBEAT_INTERVAL. If the controller answers
 * a heartbeat with 404 it has forgotten this worker (for example its database was wiped), so the
 * worker registers again. A controller outage is survived silently: the loop keeps trying.
 */
@Component
@ConditionalOnProperty(name = "worker.registration-enabled", havingValue = "true", matchIfMissing = true)
public class ControllerLink {

    private static final Logger log = LoggerFactory.getLogger(ControllerLink.class);
    private static final Duration FAST_RETRY = Duration.ofSeconds(2);
    private static final Duration SLOW_RETRY = Duration.ofSeconds(10);
    private static final int FAST_ATTEMPTS = 60;

    private final WorkerProperties properties;
    private final Environment environment;
    private final RestClient controller;

    private volatile boolean running;
    private volatile Thread thread;
    private boolean controllerReachable = true;

    public ControllerLink(WorkerProperties properties, Environment environment, RestClient.Builder restClientBuilder) {
        this.properties = properties;
        this.environment = environment;
        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(2))
                .build();
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(Duration.ofSeconds(5));
        this.controller = restClientBuilder
                .baseUrl(properties.controllerUrl())
                .requestFactory(requestFactory)
                .build();
    }

    /** Runs only after the web server is up, so the controller can reach us as soon as it knows us. */
    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        running = true;
        thread = Thread.ofPlatform().name("controller-link").daemon(true).start(this::run);
    }

    @PreDestroy
    public void stop() {
        running = false;
        Thread current = thread;
        if (current != null) {
            current.interrupt();
        }
    }

    private void run() {
        try {
            registerWithRetry();
            while (running) {
                Thread.sleep(properties.heartbeatInterval());
                heartbeat();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Every 2 s for the first 60 attempts, then every 10 s, for as long as it takes. */
    private void registerWithRetry() throws InterruptedException {
        int attempt = 0;
        while (running) {
            attempt++;
            if (tryRegister(attempt)) {
                return;
            }
            Thread.sleep(attempt < FAST_ATTEMPTS ? FAST_RETRY : SLOW_RETRY);
        }
    }

    private boolean tryRegister(int attempt) {
        String advertiseUrl = advertiseUrl();
        try {
            AckResponse ack = controller.post()
                    .uri(InternalApi.REGISTER_PATH)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(new RegisterRequest(properties.id(), advertiseUrl))
                    .retrieve()
                    .body(AckResponse.class);
            log.info("[heartbeat] registered with controller {} as {} url={} status={}",
                    properties.controllerUrl(), properties.id(), advertiseUrl, ack == null ? "?" : ack.status());
            controllerReachable = true;
            return true;
        } catch (RestClientException e) {
            log.warn("[heartbeat] register attempt {} failed: {}", attempt, rootMessage(e));
            return false;
        }
    }

    private void heartbeat() {
        try {
            controller.post()
                    .uri(InternalApi.HEARTBEAT_PATH)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(new HeartbeatRequest(properties.id()))
                    .retrieve()
                    .toBodilessEntity();
            if (!controllerReachable) {
                log.info("[heartbeat] controller is reachable again");
                controllerReachable = true;
            }
        } catch (HttpClientErrorException.NotFound unknownWorker) {
            log.warn("[heartbeat] controller does not know {}; registering again", properties.id());
            tryRegister(1);
        } catch (RestClientException e) {
            // Log the transition only, not every missed beat, to keep the log readable.
            if (controllerReachable) {
                log.warn("[heartbeat] controller unreachable, will keep trying: {}", rootMessage(e));
                controllerReachable = false;
            }
        }
    }

    private String advertiseUrl() {
        String configured = properties.advertiseUrl();
        if (configured != null && !configured.isBlank()) {
            return configured.trim();
        }
        String port = environment.getProperty("local.server.port", environment.getProperty("server.port", "8001"));
        return "http://" + properties.id() + ":" + port;
    }

    private static String rootMessage(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        return root.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }
}
