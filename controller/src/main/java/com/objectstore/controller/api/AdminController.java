package com.objectstore.controller.api;

import com.objectstore.controller.auth.AuthInterceptor;
import com.objectstore.controller.objects.ObjectInspector;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** The admin's view of the inner state ({@link AuthInterceptor} refuses everyone else with 403). */
@RestController
@RequestMapping("/api/v1/admin")
public class AdminController {

    private final ObjectInspector inspector;

    public AdminController(ObjectInspector inspector) {
        this.inspector = inspector;
    }

    /**
     * Any object, whoever owns it: its metadata, every copy of every chunk (worker, state, version,
     * VERIFY credit), the operations pending on each chunk, and what each worker holds of it.
     */
    @GetMapping("/objects/{objectId}")
    public ObjectInspector.Inspection inspect(@PathVariable String objectId) {
        return inspector.inspect(objectId);
    }
}
