package com.objectstore.controller;

import java.security.Security;
import java.util.Map;
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

    /**
     * The configuration file is {@code controller.yml}, not {@code application.yml}, so that the
     * controller and the workers can share one classpath (the in-process system tests do).
     */
    public static final String CONFIG_NAME_PROPERTY = "spring.config.name";
    public static final String CONFIG_NAME = "controller";

    public static void main(String[] args) {
        // A restarted worker container may come back with a different IP address: do not let the
        // JVM keep serving the old one (or a "no such host" answer) from its DNS cache.
        Security.setProperty("networkaddress.cache.ttl", "2");
        Security.setProperty("networkaddress.cache.negative.ttl", "1");
        SpringApplication application = new SpringApplication(ControllerApplication.class);
        application.setDefaultProperties(Map.of(CONFIG_NAME_PROPERTY, CONFIG_NAME));
        application.run(args);
    }
}
