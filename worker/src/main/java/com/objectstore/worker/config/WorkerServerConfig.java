package com.objectstore.worker.config;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import org.springframework.boot.tomcat.TomcatConnectorCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * Server settings derived from {@link WorkerProperties}, and the start-up checks that span several
 * settings (v5 §13): SERVER_READ_TIMEOUT bounds every blocking read of a request body (the socket
 * timeout), and SHUTDOWN_DRAIN_TIMEOUT must stay below Spring's {@code timeout-per-shutdown-phase}
 * (which in turn must stay below the container's {@code stop_grace_period}, 10 s).
 */
@Configuration
public class WorkerServerConfig {

    private final WorkerProperties properties;
    private final Environment environment;

    public WorkerServerConfig(WorkerProperties properties, Environment environment) {
        this.properties = properties;
        this.environment = environment;
    }

    @PostConstruct
    void validate() {
        Duration phase = environment.getProperty("spring.lifecycle.timeout-per-shutdown-phase", Duration.class,
                Duration.ofSeconds(30));
        if (properties.shutdownDrainTimeout().compareTo(phase) >= 0) {
            throw new IllegalStateException("SHUTDOWN_DRAIN_TIMEOUT (" + properties.shutdownDrainTimeoutSeconds()
                    + " s) must be below spring.lifecycle.timeout-per-shutdown-phase (" + phase.toMillis() / 1000.0 + " s)");
        }
    }

    @Bean
    TomcatConnectorCustomizer serverReadTimeout() {
        int millis = (int) Math.min(Integer.MAX_VALUE, properties.serverReadTimeout().toMillis());
        return connector -> connector.setProperty("connectionTimeout", Integer.toString(millis));
    }
}
