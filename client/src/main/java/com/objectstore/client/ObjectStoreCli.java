package com.objectstore.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Command-line client for the object store's public REST API.
 *
 * <p>It knows exactly one address, the controller's, and never learns where chunks are stored.
 * Successful commands print JSON to stdout. Exit codes: 0 success, 1 the server answered with an
 * error (its JSON error body goes to stderr), 2 the server could not be reached or the transfer
 * broke, 3 a downloaded file does not match the checksum announced by the server, 64 wrong usage.
 */
public final class ObjectStoreCli {

    static final int EXIT_OK = 0;
    static final int EXIT_HTTP_ERROR = 1;
    static final int EXIT_CONNECTION = 2;
    static final int EXIT_INTEGRITY = 3;
    static final int EXIT_USAGE = 64;

    private static final String DEFAULT_BASE_URL = "http://localhost:8000";
    private static final String USAGE = """
            Usage: oscli [--base URL] <command> [arguments]

              upload PATH [--name NAME] [--type MIME] [--meta key=value ...]
                                        store a file; prints the new object's description
              download ID OUT_PATH      fetch an object and verify it against its checksum
              meta ID                   show system + custom metadata and chunk locations
              patch ID [key=value ...] [--remove key ...]
                                        add/update/remove custom metadata
              list [--limit N] [--offset N]
              delete ID
              where ID                  table: which workers hold each chunk
              status                    cluster overview (workers, replication health, repairs)

            The controller address comes from --base, else $BASE_URL, else http://localhost:8000.
            """;

    private final PrintStream out;
    private final PrintStream err;
    private final String environmentBaseUrl;
    private final JsonMapper json = JsonMapper.builder().build();
    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    private String baseUrl;

    ObjectStoreCli(PrintStream out, PrintStream err, String environmentBaseUrl) {
        this.out = out;
        this.err = err;
        this.environmentBaseUrl = environmentBaseUrl;
    }

    public static void main(String[] args) {
        int exitCode = new ObjectStoreCli(System.out, System.err, System.getenv("BASE_URL")).run(args);
        System.out.flush();
        System.exit(exitCode);
    }

    int run(String... args) {
        try {
            List<String> remaining = new ArrayList<>(List.of(args));
            baseUrl = stripTrailingSlash(takeBaseUrl(remaining));
            if (remaining.isEmpty()) {
                err.print(USAGE);
                return EXIT_USAGE;
            }
            String command = remaining.remove(0);
            return switch (command) {
                case "upload" -> upload(remaining);
                case "download" -> download(remaining);
                case "meta" -> meta(remaining);
                case "patch" -> patch(remaining);
                case "list" -> list(remaining);
                case "delete" -> delete(remaining);
                case "where" -> where(remaining);
                case "status" -> status(remaining);
                case "help", "--help", "-h" -> {
                    out.print(USAGE);
                    yield EXIT_OK;
                }
                default -> throw new CliException(EXIT_USAGE, "unknown command '" + command + "'\n\n" + USAGE);
            };
        } catch (CliException e) {
            err.println(e.getMessage());
            return e.exitCode();
        } catch (IOException e) {
            err.println(errorJson("connection_failed", "could not reach " + baseUrl + ": " + describe(e)));
            return EXIT_CONNECTION;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            err.println(errorJson("interrupted", "the command was interrupted"));
            return EXIT_CONNECTION;
        }
    }

    // ---- commands -----------------------------------------------------------------------------

    private int upload(List<String> args) throws IOException, InterruptedException {
        Arguments arguments = Arguments.parse(args, Set.of("--name", "--type"), Set.of("--meta"));
        Path file = Path.of(onlyPositional(arguments, "upload PATH [--name NAME] [--type MIME] [--meta key=value ...]"));
        if (!Files.isRegularFile(file) || !Files.isReadable(file)) {
            throw new CliException(EXIT_USAGE, "'" + file + "' is not a readable file");
        }
        String name = arguments.option("--name") != null ? arguments.option("--name") : file.getFileName().toString();
        String type = arguments.option("--type") != null ? arguments.option("--type") : ContentTypes.guess(file);

        String boundary = "----objectstore" + UUID.randomUUID().toString().replace("-", "");
        StringBuilder head = new StringBuilder();
        if (!arguments.options("--meta").isEmpty()) {
            ObjectNode metadata = json.createObjectNode();
            for (String pair : arguments.options("--meta")) {
                Map.Entry<String, String> entry = Arguments.keyValue(pair);
                metadata.put(entry.getKey(), entry.getValue());
            }
            head.append("--").append(boundary).append("\r\n")
                    .append("Content-Disposition: form-data; name=\"metadata\"\r\n\r\n")
                    .append(json.writeValueAsString(metadata)).append("\r\n");
        }
        head.append("--").append(boundary).append("\r\n")
                .append("Content-Disposition: form-data; name=\"file\"; filename=\"").append(quoted(name)).append("\"\r\n")
                .append("Content-Type: ").append(type).append("\r\n\r\n");
        String tail = "\r\n--" + boundary + "--\r\n";

        // The file is streamed from disk; it is never loaded into memory as a whole.
        HttpRequest request = HttpRequest.newBuilder(uri("/api/v1/objects"))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(BodyPublishers.concat(
                        BodyPublishers.ofByteArray(head.toString().getBytes(StandardCharsets.UTF_8)),
                        BodyPublishers.ofFile(file),
                        BodyPublishers.ofByteArray(tail.getBytes(StandardCharsets.UTF_8))))
                .build();
        out.println(pretty(expect(http.send(request, BodyHandlers.ofString()), 201)));
        return EXIT_OK;
    }

    private int download(List<String> args) throws IOException, InterruptedException {
        Arguments arguments = Arguments.parse(args, Set.of(), Set.of());
        if (arguments.positionals().size() != 2) {
            throw new CliException(EXIT_USAGE, "usage: download ID OUT_PATH");
        }
        String objectId = arguments.positionals().get(0);
        Path target = Path.of(arguments.positionals().get(1)).toAbsolutePath();
        Path partial = target.resolveSibling(target.getFileName() + ".part");

        HttpResponse<InputStream> response = http.send(
                HttpRequest.newBuilder(uri("/api/v1/objects/" + objectId)).GET().build(), BodyHandlers.ofInputStream());
        if (response.statusCode() != 200) {
            try (InputStream body = response.body()) {
                throw new CliException(EXIT_HTTP_ERROR, pretty(new String(body.readAllBytes(), StandardCharsets.UTF_8)));
            }
        }

        MessageDigest digest = sha256();
        long bytes = 0;
        try (InputStream in = response.body(); OutputStream file = Files.newOutputStream(partial)) {
            byte[] buffer = new byte[64 * 1024];
            int read;
            while ((read = in.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
                file.write(buffer, 0, read);
                bytes += read;
            }
        } catch (IOException e) {
            Files.deleteIfExists(partial);
            throw new CliException(EXIT_CONNECTION, errorJson("download_incomplete",
                    "the transfer broke off after " + bytes + " bytes: " + describe(e)));
        }
        Files.move(partial, target, StandardCopyOption.REPLACE_EXISTING);

        String sha = HexFormat.of().formatHex(digest.digest());
        String etag = response.headers().firstValue("ETag").orElse("").replace("\"", "");
        boolean matches = sha.equals(etag);
        ObjectNode result = json.createObjectNode();
        result.put("object_id", objectId);
        result.put("path", target.toString());
        result.put("bytes", bytes);
        result.put("content_type", response.headers().firstValue("Content-Type").orElse(""));
        result.put("sha256", sha);
        result.put("etag", etag);
        result.put("etag_match", matches);
        out.println(pretty(result));
        return matches ? EXIT_OK : EXIT_INTEGRITY;
    }

    private int meta(List<String> args) throws IOException, InterruptedException {
        String objectId = onlyPositional(Arguments.parse(args, Set.of(), Set.of()), "meta ID");
        out.println(pretty(expect(get("/api/v1/objects/" + objectId + "/metadata"), 200)));
        return EXIT_OK;
    }

    private int patch(List<String> args) throws IOException, InterruptedException {
        Arguments arguments = Arguments.parse(args, Set.of(), Set.of("--remove"));
        List<String> positionals = arguments.positionals();
        if (positionals.isEmpty() || positionals.size() + arguments.options("--remove").size() < 2) {
            throw new CliException(EXIT_USAGE, "usage: patch ID [key=value ...] [--remove key ...]");
        }
        ObjectNode changes = json.createObjectNode();
        for (String pair : positionals.subList(1, positionals.size())) {
            Map.Entry<String, String> entry = Arguments.keyValue(pair);
            changes.put(entry.getKey(), entry.getValue());
        }
        for (String key : arguments.options("--remove")) {
            changes.putNull(key);
        }
        HttpRequest request = HttpRequest.newBuilder(uri("/api/v1/objects/" + positionals.get(0) + "/metadata"))
                .header("Content-Type", "application/json")
                .method("PATCH", BodyPublishers.ofString(json.writeValueAsString(changes)))
                .build();
        out.println(pretty(expect(http.send(request, BodyHandlers.ofString()), 200)));
        return EXIT_OK;
    }

    private int list(List<String> args) throws IOException, InterruptedException {
        Arguments arguments = Arguments.parse(args, Set.of("--limit", "--offset"), Set.of());
        if (!arguments.positionals().isEmpty()) {
            throw new CliException(EXIT_USAGE, "usage: list [--limit N] [--offset N]");
        }
        List<String> query = new ArrayList<>();
        if (arguments.option("--limit") != null) {
            query.add("limit=" + arguments.option("--limit"));
        }
        if (arguments.option("--offset") != null) {
            query.add("offset=" + arguments.option("--offset"));
        }
        out.println(pretty(expect(get("/api/v1/objects" + (query.isEmpty() ? "" : "?" + String.join("&", query))), 200)));
        return EXIT_OK;
    }

    private int delete(List<String> args) throws IOException, InterruptedException {
        String objectId = onlyPositional(Arguments.parse(args, Set.of(), Set.of()), "delete ID");
        HttpRequest request = HttpRequest.newBuilder(uri("/api/v1/objects/" + objectId)).DELETE().build();
        expect(http.send(request, BodyHandlers.ofString()), 204);
        ObjectNode result = json.createObjectNode();
        result.put("deleted", objectId);
        out.println(pretty(result));
        return EXIT_OK;
    }

    private int status(List<String> args) throws IOException, InterruptedException {
        if (!args.isEmpty()) {
            throw new CliException(EXIT_USAGE, "usage: status");
        }
        out.println(pretty(expect(get("/api/v1/cluster/status"), 200)));
        return EXIT_OK;
    }

    /** Shows chunking, distribution and replication at a glance: one row per chunk. */
    private int where(List<String> args) throws IOException, InterruptedException {
        String objectId = onlyPositional(Arguments.parse(args, Set.of(), Set.of()), "where ID");
        JsonNode object = json.readTree(expect(get("/api/v1/objects/" + objectId + "/metadata"), 200));

        out.printf("object  %s%n", object.get("object_id").asString());
        out.printf("name    %s   (%d bytes, %s)%n", object.get("name").asString(), object.get("size").asLong(),
                object.get("content_type").asString());
        out.printf("chunks  %d   replication_ok=%s%n%n", object.get("chunk_count").asInt(),
                object.get("replication_ok").asBoolean());
        out.printf("%-4s %-42s %8s  %s%n", "IDX", "CHUNK_ID", "SIZE", "COPIES");
        Map<String, Integer> copiesPerWorker = new TreeMap<>();
        for (JsonNode chunk : object.get("chunks")) {
            List<String> copies = new ArrayList<>();
            for (JsonNode location : chunk.get("locations")) {
                String worker = location.get("worker_id").asString();
                String state = location.get("status").asString();
                if (!"ALIVE".equals(location.get("worker_status").asString())) {
                    state += "," + location.get("worker_status").asString();
                }
                copies.add(worker + "(" + state + ")");
                copiesPerWorker.merge(worker, 1, Integer::sum);
            }
            out.printf("%-4d %-42s %8d  %s%n", chunk.get("index").asInt(), chunk.get("chunk_id").asString(),
                    chunk.get("size").asInt(), String.join("  ", copies));
        }
        if (!copiesPerWorker.isEmpty()) {
            List<String> summary = new ArrayList<>();
            copiesPerWorker.forEach((worker, count) -> summary.add(worker + "=" + count));
            out.printf("%ncopies per worker: %s%n", String.join("  ", summary));
        }
        return EXIT_OK;
    }

    // ---- plumbing -----------------------------------------------------------------------------

    private String takeBaseUrl(List<String> args) {
        String base = environmentBaseUrl == null || environmentBaseUrl.isBlank() ? DEFAULT_BASE_URL : environmentBaseUrl;
        int index = args.indexOf("--base");
        if (index >= 0) {
            if (index + 1 >= args.size()) {
                throw new CliException(EXIT_USAGE, "--base needs a URL");
            }
            base = args.get(index + 1);
            args.remove(index + 1);
            args.remove(index);
        }
        return base;
    }

    private URI uri(String path) {
        try {
            return URI.create(baseUrl + path);
        } catch (IllegalArgumentException e) {
            throw new CliException(EXIT_USAGE, "not a valid URL: " + baseUrl + path);
        }
    }

    private HttpResponse<String> get(String path) throws IOException, InterruptedException {
        return http.send(HttpRequest.newBuilder(uri(path)).GET().build(), BodyHandlers.ofString());
    }

    /** Returns the body if the status is the expected one; otherwise fails with the server's error. */
    private String expect(HttpResponse<String> response, int status) {
        if (response.statusCode() != status) {
            String body = response.body();
            throw new CliException(EXIT_HTTP_ERROR, body == null || body.isBlank()
                    ? errorJson("http_" + response.statusCode(), "the server answered " + response.statusCode())
                    : pretty(body));
        }
        return response.body();
    }

    private static String onlyPositional(Arguments arguments, String usage) {
        if (arguments.positionals().size() != 1) {
            throw new CliException(EXIT_USAGE, "usage: " + usage);
        }
        return arguments.positionals().get(0);
    }

    private String pretty(String body) {
        try {
            return pretty(json.readTree(body));
        } catch (JacksonException notJson) {
            return body;
        }
    }

    private String pretty(JsonNode node) {
        return json.writerWithDefaultPrettyPrinter().writeValueAsString(node);
    }

    private String errorJson(String code, String detail) {
        ObjectNode error = json.createObjectNode();
        error.put("error", code);
        error.put("detail", detail);
        return pretty(error);
    }

    private static String describe(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        return root.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }

    /** A file name as it may appear inside a quoted multipart header parameter. */
    private static String quoted(String name) {
        return name.replace("\"", "%22").replace("\r", "").replace("\n", "");
    }

    private static String stripTrailingSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
