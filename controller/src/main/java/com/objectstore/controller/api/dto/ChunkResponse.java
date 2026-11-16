package com.objectstore.controller.api.dto;

import java.util.List;

public record ChunkResponse(String chunkId, int index, int size, String checksum, List<LocationResponse> locations) {
}
