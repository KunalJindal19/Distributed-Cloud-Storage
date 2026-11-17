package com.objectstore.worker;

import java.security.Security;
import java.util.Map;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * A storage worker. It knows nothing about objects, replication or other workers: it stores,
 * serves, deletes and lists chunks, and tells the controller that it is alive.
 */
@SpringBootApplication(scanBasePackages = {"com.objectstore.worker", "com.objectstore.common.web"})
@ConfigurationPropertiesScan
public class WorkerApplication {

    /**
     * The configuration file is {@code worker.yml}, not {@code application.yml}, so that the
     * controller and the workers can share one classpath (the in-process system tests do).
     */
    public static final String CONFIG_NAME_PROPERTY = "spring.config.name";
    public static final String CONFIG_NAME = "worker";

    public static void main(String[] args) {
        // The controller's container can be recreated with a new IP; do not cache DNS answers for long.
        Security.setProperty("networkaddress.cache.ttl", "2");
        Security.setProperty("networkaddress.cache.negative.ttl", "1");
        SpringApplication application = new SpringApplication(WorkerApplication.class);
        application.setDefaultProperties(Map.of(CONFIG_NAME_PROPERTY, CONFIG_NAME));
        application.run(args);
    }
}
