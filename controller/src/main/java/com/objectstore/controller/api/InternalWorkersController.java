package com.objectstore.controller.api;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.dto.RegisterRequest;
import com.objectstore.common.dto.RegisterResponse;
import com.objectstore.controller.cluster.Registry;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * The controller side of the worker protocol: registration and heartbeats. A heartbeat carries the
 * worker's change stream and at most one full-report segment (there are no separate report
 * endpoints). The request body is parsed before, and the answer serialised after, the one brief
 * block that touches cluster state.
 */
@RestController
public class InternalWorkersController {

    private final Registry registry;

    public InternalWorkersController(Registry registry) {
        this.registry = registry;
    }

    /** Register a worker (process) with the controller. Safe to repeat. */
    @PostMapping(InternalApi.REGISTER_PATH)
    public RegisterResponse register(@Valid @RequestBody RegisterRequest request) {
        return registry.register(request);
    }

    /** Liveness and statistics, the worker's next changes and report segment; receive commands and acks. */
    @PostMapping(InternalApi.HEARTBEAT_PATH)
    public HeartbeatResponse heartbeat(@Valid @RequestBody HeartbeatRequest request) {
        return registry.heartbeat(request);
    }
}
