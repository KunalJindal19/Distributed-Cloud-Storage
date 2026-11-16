package com.objectstore.controller.api.dto;

import java.util.List;

/** A page of objects plus the paging parameters that produced it and the total number of objects. */
public record ObjectListResponse(List<ObjectSummary> items, long total, int limit, long offset) {
}
