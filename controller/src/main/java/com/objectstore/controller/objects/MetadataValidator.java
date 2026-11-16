package com.objectstore.controller.objects;

import com.objectstore.common.web.ApiException;
import com.objectstore.common.web.ErrorCodes;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Rules for custom (user-defined) metadata, applied identically on upload and on PATCH:
 *
 * <ul>
 *   <li>a flat JSON object;</li>
 *   <li>keys: 1-64 characters from {@code A-Z a-z 0-9 _ . -};</li>
 *   <li>values: strings of at most 1024 characters;</li>
 *   <li>at most 64 keys per object;</li>
 *   <li>in a PATCH only, a value of JSON {@code null} removes the key.</li>
 * </ul>
 *
 * Anything else is rejected with 400 {@code invalid_metadata}.
 */
@Component
public class MetadataValidator {

    public static final int MAX_KEYS = 64;
    public static final int MAX_KEY_LENGTH = 64;
    public static final int MAX_VALUE_LENGTH = 1024;

    private static final Pattern KEY = Pattern.compile("^[A-Za-z0-9_.-]{1," + MAX_KEY_LENGTH + "}$");

    /** The changes requested by a PATCH. */
    public record Patch(Map<String, String> upserts, Set<String> removals) {
    }

    private final ObjectMapper json;

    public MetadataValidator(ObjectMapper json) {
        this.json = json;
    }

    /** The optional {@code metadata} form field of an upload. Absent or blank means "no metadata". */
    public Map<String, String> parseForUpload(String raw) {
        if (raw == null || raw.isBlank()) {
            return Map.of();
        }
        Patch patch = parse(readObject(raw), false);
        requireKeyLimit(patch.upserts().size());
        return patch.upserts();
    }

    /** The JSON body of {@code PATCH .../metadata}. */
    public Patch parseForPatch(byte[] body) {
        if (body == null || body.length == 0) {
            throw invalid("the request body must be a JSON object of metadata keys and values");
        }
        return parse(readObject(body), true);
    }

    public static void requireKeyLimit(long keys) {
        if (keys > MAX_KEYS) {
            throw invalid("an object can have at most " + MAX_KEYS + " metadata keys");
        }
    }

    private JsonNode readObject(Object source) {
        JsonNode node;
        try {
            node = source instanceof byte[] bytes ? json.readTree(bytes) : json.readTree((String) source);
        } catch (JacksonException e) {
            throw invalid("metadata is not valid JSON");
        }
        if (node == null || !node.isObject()) {
            throw invalid("metadata must be a JSON object, e.g. {\"owner\": \"alice\"}");
        }
        return node;
    }

    private static Patch parse(JsonNode object, boolean nullRemoves) {
        Map<String, String> upserts = new LinkedHashMap<>();
        Set<String> removals = new LinkedHashSet<>();
        for (Map.Entry<String, JsonNode> property : object.properties()) {
            String key = property.getKey();
            JsonNode value = property.getValue();
            if (!KEY.matcher(key).matches()) {
                throw invalid("metadata key '" + abbreviate(key) + "' is invalid: keys are 1-" + MAX_KEY_LENGTH
                        + " characters from A-Z a-z 0-9 _ . -");
            }
            if (value.isNull() && nullRemoves) {
                removals.add(key);
            } else if (value.isString()) {
                String text = value.stringValue();
                if (text.length() > MAX_VALUE_LENGTH) {
                    throw invalid("the value of metadata key '" + key + "' is longer than " + MAX_VALUE_LENGTH
                            + " characters");
                }
                upserts.put(key, text);
            } else {
                throw invalid("the value of metadata key '" + key + "' must be a string"
                        + (nullRemoves ? " (or null to remove the key)" : ""));
            }
        }
        return new Patch(upserts, removals);
    }

    private static String abbreviate(String key) {
        return key.length() <= 80 ? key : key.substring(0, 80) + "...";
    }

    private static ApiException invalid(String detail) {
        return ApiException.badRequest(ErrorCodes.INVALID_METADATA, detail);
    }
}
