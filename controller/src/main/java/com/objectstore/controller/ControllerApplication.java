package com.objectstore.controller;

import java.security.Security;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * The controller: the only component a client ever talks to, and the only one that makes
 * decisions. It splits objects into chunks, chooses where the copies go, remembers everything in
 * its metadata database, watches the workers, and repairs whatever breaks.
 */
@SpringBootApplication(scanBasePackages = {"com.objectstore.controller", "com.objectstore.common.web"})
@ConfigurationPropertiesScan
public class ControllerApplication {

    public static void main(String[] args) {
        // A restarted worker container may come back with a different IP address: do not let the
        // JVM keep serving the old one (or a "no such host" answer) from its DNS cache.
        Security.setProperty("networkaddress.cache.ttl", "2");
        Security.setProperty("networkaddress.cache.negative.ttl", "1");
        SpringApplication.run(ControllerApplication.class, args);
    }
}
