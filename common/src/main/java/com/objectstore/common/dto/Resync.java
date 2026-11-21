package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The controller asks for a fenced resync (v5 §5): adopt the fence {@code t0} (if it is new) and
 * report every chunk, as report attempt {@code attempt} of that fence.
 */
public record Resync(
        @JsonProperty("t0") long t0,
        @JsonProperty("attempt") int attempt) {
}
