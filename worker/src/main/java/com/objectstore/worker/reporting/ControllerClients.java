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
 *   <li>{@link #lifeline()}: heartbeats only. Its own client with a short time-out so a slow
 *       report or transfer can never delay a heartbeat (HDFS's lifeline channel).</li>
 *   <li>{@link #reports()}: registration, full and incremental block reports.</li>
 *   <li>{@link #transfers()}: chunk copies to other workers, with the data-path time-outs.</li>
 * </ul>
 */
@Component
public class ControllerClients {

    private final RestClient lifeline;
    private final RestClient reports;
    private final RestClient transfers;

    public ControllerClients(RestClient.Builder builder, WorkerProperties properties) {
        FaultSettings faults = FaultSettings.fromEnvironment();
        Duration connect = properties.connectTimeout();
        this.lifeline = HttpClients.restClient(
                builder.baseUrl(properties.controllerUrl()), connect, properties.heartbeatHttpTimeout(), faults);
        this.reports = HttpClients.restClient(
                builder.baseUrl(properties.controllerUrl()), connect, properties.readTimeout(), faults);
        this.transfers = HttpClients.restClient(builder, connect, properties.readTimeout(), faults);
    }

    public RestClient lifeline() {
        return lifeline;
    }

    public RestClient reports() {
        return reports;
    }

    public RestClient transfers() {
        return transfers;
    }
}
