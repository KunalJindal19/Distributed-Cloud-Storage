package com.objectstore.controller.auth;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * Client accounts (controller.yml {@code objectstore.auth}).
 *
 * @param adminEmail    the one admin account (ADMIN_EMAIL); it sees every object and may run fsck
 * @param adminPassword its password (ADMIN_PASSWORD), applied at every start-up; blank: a random one
 *                      is generated and logged
 * @param sessionHours  how long a sign-in session lasts (SESSION_HOURS)
 */
@Validated
@ConfigurationProperties(prefix = "objectstore.auth")
public record AuthProperties(
        @DefaultValue("admin@gmail.com") @NotBlank String adminEmail,
        @DefaultValue("") String adminPassword,
        @DefaultValue("168") @Positive double sessionHours) {
}
