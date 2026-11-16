package com.objectstore.controller.api;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.AckResponse;
import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.RegisterRequest;
import com.objectstore.controller.cluster.WorkerRegistry;
import com.objectstore.controller.domain.Worker;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** The controller side of the internal API: the two endpoints workers call. */
@RestController
public class InternalWorkersController {

    private final WorkerRegistry registry;

    public InternalWorkersController(WorkerRegistry registry) {
        this.registry = registry;
    }

    /** Register a worker with the controller. Safe to repeat; always answers ALIVE. */
    @PostMapping(InternalApi.REGISTER_PATH)
    public AckResponse register(@Valid @RequestBody RegisterRequest request) {
        String url = request.url().endsWith("/")
                ? request.url().substring(0, request.url().length() - 1)
                : request.url();
        Worker worker = registry.register(request.workerId(), url);
        return new AckResponse(true, worker.workerId(), worker.status().name());
    }

    /** Report worker availability. 404 {@code unknown_worker} tells the worker to register again. */
    @PostMapping(InternalApi.HEARTBEAT_PATH)
    public AckResponse heartbeat(@Valid @RequestBody HeartbeatRequest request) {
        Worker worker = registry.heartbeat(request.workerId());
        return new AckResponse(true, worker.workerId(), worker.status().name());
    }
}
