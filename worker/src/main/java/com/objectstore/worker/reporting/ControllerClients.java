package com.objectstore.worker.reporting;

import com.objectstore.common.http.FaultSettings;
import com.objectstore.common.http.HttpClients;
import com.objectstore.worker.config.WorkerProperties;
import java.time.Duration;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * The worker's HTTP clients.
 *
 * <ul>
 *   <li>{@link #lifeline()}: heartbeats (which carry the change stream and the full-report
 *       segments). Its own client with a short time-out (HEARTBEAT_HTTP_TIMEOUT), so a slow
 *       transfer can never delay a heartbeat.</li>
 *   <li>{@link #registration()}: registration, with the normal read time-out.</li>
 *   <li>{@link #transfers()}: chunk copies to other workers, with the data-path time-outs.</li>
 * </ul>
 */
@Component
public class ControllerClients {

    private final RestClient lifeline;
    private final RestClient registration;
    private final RestClient transfers;

    public ControllerClients(RestClient.Builder builder, WorkerProperties properties) {
        FaultSettings faults = FaultSettings.fromEnvironment();
        Duration connect = properties.connectTimeout();
        this.lifeline = HttpClients.restClient(
                builder.baseUrl(properties.controllerUrl()), connect, properties.heartbeatHttpTimeout(), faults);
        this.registration = HttpClients.restClient(
                builder.baseUrl(properties.controllerUrl()), connect, properties.readTimeout(), faults);
        this.transfers = HttpClients.restClient(builder, connect, properties.readTimeout(), faults);
    }

    public RestClient lifeline() {
        return lifeline;
    }

    public RestClient registration() {
        return registration;
    }

    public RestClient transfers() {
        return transfers;
    }
}
