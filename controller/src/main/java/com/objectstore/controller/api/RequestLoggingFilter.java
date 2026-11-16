package com.objectstore.controller.api;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Gives every request an id ({@code X-Request-ID}, taken from the client or generated) that is
 * echoed in the response, and writes one {@code [api]} line per request.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class RequestLoggingFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(RequestLoggingFilter.class);
    public static final String REQUEST_ID_HEADER = "X-Request-ID";
    private static final Pattern SAFE_REQUEST_ID = Pattern.compile("^[A-Za-z0-9._-]{1,64}$");

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String requestId = request.getHeader(REQUEST_ID_HEADER);
        if (requestId == null || !SAFE_REQUEST_ID.matcher(requestId).matches()) {
            requestId = UUID.randomUUID().toString();
        }
        response.setHeader(REQUEST_ID_HEADER, requestId);
        long startedNanos = System.nanoTime();
        try {
            chain.doFilter(request, response);
        } finally {
            long millis = (System.nanoTime() - startedNanos) / 1_000_000;
            String line = "[api] {} {} -> {} {}ms rid={}";
            if (isRoutine(request.getRequestURI())) {
                log.debug(line, request.getMethod(), request.getRequestURI(), response.getStatus(), millis, requestId);
            } else {
                log.info(line, request.getMethod(), request.getRequestURI(), response.getStatus(), millis, requestId);
            }
        }
    }

    /** Polling and heartbeat traffic would drown everything else at INFO. */
    private static boolean isRoutine(String path) {
        return path.equals("/health")
                || path.equals("/dashboard")
                || path.equals("/favicon.ico")
                || path.equals("/api/v1/cluster/status")
                || path.startsWith("/internal/workers/");
    }
}
