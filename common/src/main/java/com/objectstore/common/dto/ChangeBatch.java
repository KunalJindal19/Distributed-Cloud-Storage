package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** Changes carried by a PUT answer, tagged with the worker process that numbered them. */
public record ChangeBatch(
        @JsonProperty("boot_id") String bootId,
        @JsonProperty("items") List<Change> items) {
}
