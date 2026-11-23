package com.objectstore.systemtest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * A small HTTP client for the controller's public and internal API, used by the system tests. It
 * signs every request in as the admin unless created for another account ({@link #as}).
 */
public final class Api {

    /** The admin account of the in-process controller (InProcessCluster sets the password). */
    public static final String ADMIN_EMAIL = "admin@gmail.com";
    public static final String ADMIN_PASSWORD = "system-test-admin-password";

    private final String base;
    private final String authorization;
    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(2))
            .build();
    private final JsonMapper json = JsonMapper.builder().build();

    public Api(String base) {
        this(base, basic(ADMIN_EMAIL, ADMIN_PASSWORD));
    }

    /** {@code authorization}: the Authorization header of every request, or null for none. */
    public Api(String base, String authorization) {
        this.base = base;
        this.authorization = authorization;
    }

    /** The same controller, signed in as another account (HTTP Basic). */
    public Api as(String email, String password) {
        return new Api(base, basic(email, password));
    }

    /** The same controller with another Authorization header (null: none). */
    public Api withAuthorization(String header) {
        return new Api(base, header);
    }

    public static String basic(String email, String password) {
        return "Basic " + Base64.getEncoder().encodeToString((email + ":" + password).getBytes(StandardCharsets.UTF_8));
    }

    /** Any request; {@code headers} are added to (and may replace) the account's Authorization header. */
    public HttpResponse<String> request(String method, String path, String body, Map<String, String> headers) {
        HttpRequest.Builder request = HttpRequest.newBuilder(uri(path)).method(method,
                body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        if (body != null) {
            request.header("Content-Type", "application/json");
        }
        headers.forEach(request::setHeader);
        return send(request, HttpResponse.BodyHandlers.ofString());
    }

    public JsonNode json(String text) {
        return json.readTree(text);
    }

    public JsonNode status() {
        return json(get("/api/v1/cluster/status").body());
    }

    public JsonNode worker(String workerId) {
        for (JsonNode worker : status().get("workers")) {
            if (worker.get("worker_id").asString().equals(workerId)) {
                return worker;
            }
        }
        return null;
    }

    public String workerState(String workerId) {
        JsonNode worker = worker(workerId);
        return worker == null ? "UNKNOWN" : worker.get("state").asString();
    }

    public JsonNode metadata(String objectId) {
        HttpResponse<String> response = get("/api/v1/objects/" + objectId + "/metadata");
        if (response.statusCode() != 200) {
            throw new IllegalStateException("metadata " + response.statusCode() + " " + response.body());
        }
        return json(response.body());
    }

    /** Workers holding a copy of chunk {@code index} of the object, with any state. */
    public List<String> holders(String objectId, int index) {
        List<String> holders = new ArrayList<>();
        metadata(objectId).get("chunks").get(index).get("locations")
                .forEach(l -> holders.add(l.get("worker_id").asString()));
        return holders;
    }

    public HttpResponse<String> get(String path) {
        return send(HttpRequest.newBuilder(uri(path)).GET(), HttpResponse.BodyHandlers.ofString());
    }

    public HttpResponse<byte[]> download(String objectId) {
        return send(HttpRequest.newBuilder(uri("/api/v1/objects/" + objectId)).GET(), HttpResponse.BodyHandlers.ofByteArray());
    }

    public HttpResponse<String> delete(String objectId) {
        return send(HttpRequest.newBuilder(uri("/api/v1/objects/" + objectId)).DELETE(), HttpResponse.BodyHandlers.ofString());
    }

    public HttpResponse<String> patch(String objectId, String body) {
        return send(HttpRequest.newBuilder(uri("/api/v1/objects/" + objectId + "/metadata"))
                .header("Content-Type", "application/json")
                .method("PATCH", HttpRequest.BodyPublishers.ofString(body)), HttpResponse.BodyHandlers.ofString());
    }

    public HttpResponse<String> postJson(String path, String body) {
        return send(HttpRequest.newBuilder(uri(path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)), HttpResponse.BodyHandlers.ofString());
    }

    public HttpResponse<String> upload(String fileName, byte[] data, String metadata, String idempotencyKey) {
        String boundary = "----test" + UUID.randomUUID().toString().replace("-", "");
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        try {
            if (metadata != null) {
                body.write(("--" + boundary + "\r\nContent-Disposition: form-data; name=\"metadata\"\r\n\r\n")
                        .getBytes(StandardCharsets.UTF_8));
                body.write(metadata.getBytes(StandardCharsets.UTF_8));
                body.write("\r\n".getBytes(StandardCharsets.UTF_8));
            }
            body.write(("--" + boundary + "\r\nContent-Disposition: form-data; name=\"file\"; filename=\"" + fileName
                    + "\"\r\nContent-Type: application/octet-stream\r\n\r\n").getBytes(StandardCharsets.UTF_8));
            body.write(data);
            body.write(("\r\n--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        HttpRequest.Builder request = HttpRequest.newBuilder(uri("/api/v1/objects"))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body.toByteArray()));
        if (idempotencyKey != null) {
            request.header("Idempotency-Key", idempotencyKey);
        }
        return send(request, HttpResponse.BodyHandlers.ofString());
    }

    /** Uploads and returns the new object's id, failing the test on anything but 201. */
    public String uploadOk(String fileName, byte[] data) {
        HttpResponse<String> response = upload(fileName, data, null, null);
        if (response.statusCode() != 201) {
            throw new IllegalStateException("upload answered " + response.statusCode() + " " + response.body());
        }
        return json(response.body()).get("object_id").asString();
    }

    public static byte[] randomBytes(int length, long seed) {
        byte[] data = new byte[length];
        new Random(seed).nextBytes(data);
        return data;
    }

    private URI uri(String path) {
        return URI.create(base + path);
    }

    private <T> HttpResponse<T> send(HttpRequest.Builder request, HttpResponse.BodyHandler<T> handler) {
        if (authorization != null && request.build().headers().firstValue("Authorization").isEmpty()) {
            request.header("Authorization", authorization);
        }
        try {
            return http.send(request.timeout(Duration.ofSeconds(30)).build(), handler);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
