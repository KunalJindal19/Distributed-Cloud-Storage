package com.objectstore.worker;

import java.security.Security;
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

    public static void main(String[] args) {
        // The controller's container can be recreated with a new IP; do not cache DNS answers for long.
        Security.setProperty("networkaddress.cache.ttl", "2");
        Security.setProperty("networkaddress.cache.negative.ttl", "1");
        SpringApplication.run(WorkerApplication.class, args);
    }
}
