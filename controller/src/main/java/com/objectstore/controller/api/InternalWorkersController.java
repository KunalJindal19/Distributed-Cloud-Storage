package com.objectstore.controller.api;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.BlockReportRequest;
import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.dto.IncrementalReportRequest;
import com.objectstore.common.dto.RegisterRequest;
import com.objectstore.common.dto.RegisterResponse;
import com.objectstore.common.dto.ReportResponse;
import com.objectstore.controller.cluster.Registry;
import com.objectstore.controller.cluster.Reports;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * The controller side of the worker protocol: registration, heartbeats and block reports. The
 * request body is parsed before, and the answer serialised after, the one synchronous block that
 * touches cluster state.
 */
@RestController
public class InternalWorkersController {

    private final Registry registry;
    private final Reports reports;

    public InternalWorkersController(Registry registry, Reports reports) {
        this.registry = registry;
        this.reports = reports;
    }

    /** Register a worker (process) with the controller. Safe to repeat. */
    @PostMapping(InternalApi.REGISTER_PATH)
    public RegisterResponse register(@Valid @RequestBody RegisterRequest request) {
        return registry.register(request);
    }

    /** Report availability and statistics; receive commands. */
    @PostMapping(InternalApi.HEARTBEAT_PATH)
    public HeartbeatResponse heartbeat(@Valid @RequestBody HeartbeatRequest request) {
        return registry.heartbeat(request);
    }

    /** Full block report: everything on the worker's disk. */
    @PostMapping(InternalApi.BLOCK_REPORT_PATH)
    public ReportResponse blockReport(@Valid @RequestBody BlockReportRequest request) {
        return reports.fullReport(request);
    }

    /** Incremental block report: what changed since the previous report. */
    @PostMapping(InternalApi.IBR_PATH)
    public ReportResponse incrementalReport(@Valid @RequestBody IncrementalReportRequest request) {
        return reports.ibr(request);
    }
}
