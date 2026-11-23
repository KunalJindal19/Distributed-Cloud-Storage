package com.objectstore.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Runs the CLI against a stub controller and checks what goes over the wire and what comes out. */
class ObjectStoreCliTest {

    private static final String ID = "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a";
    private static final String OBJECT_JSON = """
            {"object_id":"%s","name":"big.bin","size":1048586,"content_type":"application/octet-stream",
             "checksum":"abc","chunk_count":2,"created_at":"t","updated_at":"t","replication_ok":true,
             "custom_metadata":{"owner":"ansh"},
             "chunks":[
               {"chunk_id":"%s-00000","index":0,"size":1048576,"checksum":"c0","locations":[
                  {"worker_id":"worker1","status":"OK","worker_status":"ALIVE"},
                  {"worker_id":"worker2","status":"OK","worker_status":"DEAD"}]},
               {"chunk_id":"%s-00001","index":1,"size":10,"checksum":"c1","locations":[
                  {"worker_id":"worker2","status":"CORRUPT","worker_status":"ALIVE"},
                  {"worker_id":"worker3","status":"OK","worker_status":"ALIVE"}]}]}
            """.formatted(ID, ID, ID);

    @TempDir
    Path dir;

    private HttpServer server;
    private String base;
    private final ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    private final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
    private final AtomicReference<String> lastRequest = new AtomicReference<>();
    private final AtomicReference<byte[]> lastBody = new AtomicReference<>();
    private final AtomicReference<String> lastContentType = new AtomicReference<>();
    private volatile byte[] downloadBody = "hello object store".getBytes(StandardCharsets.UTF_8);
    private volatile String downloadEtag;
    private volatile double retrySeconds = 0;
    private final java.util.concurrent.atomic.AtomicInteger unavailableAnswers = new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.List<String> idempotencyKeys = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final AtomicReference<String> lastAuthorization = new AtomicReference<>();
    private final java.util.Map<String, String> environment = new java.util.HashMap<>();

    @BeforeEach
    void startStubController() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
        base = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void stopStubController() {
        server.stop(0);
    }

    @Test
    void loginKeepsTheSessionForLaterCommandsAndLogoutEndsIt() throws Exception {
        Path session = dir.resolve("conf/session");
        environment.put("OSCLI_SESSION_FILE", session.toString());

        assertThat(run("login", "alice@example.com", "secret-password")).isZero();
        assertThat(lastRequest.get()).isEqualTo("POST /api/v1/auth/login");
        assertThat(new String(lastBody.get(), StandardCharsets.UTF_8))
                .isEqualTo("{\"email\":\"alice@example.com\",\"password\":\"secret-password\"}");
        assertThat(Files.readString(session).trim()).isEqualTo("tok-123");
        assertThat(stdout.toString()).contains("\"email\" : \"alice@example.com\"").doesNotContain("tok-123");

        assertThat(run("list")).isZero();
        assertThat(lastAuthorization.get()).isEqualTo("Bearer tok-123");

        assertThat(run("logout")).isZero();
        assertThat(lastRequest.get()).isEqualTo("POST /api/v1/auth/logout");
        assertThat(lastAuthorization.get()).isEqualTo("Bearer tok-123");
        assertThat(session).doesNotExist();
    }

    @Test
    void emailAndPasswordFromTheEnvironmentAreSentWithEveryRequest() throws Exception {
        environment.put("OSCLI_EMAIL", "bob@example.com");
        environment.put("OSCLI_PASSWORD", "pw:with:colons");

        assertThat(run("list")).isZero();

        assertThat(lastAuthorization.get()).isEqualTo("Basic " + java.util.Base64.getEncoder()
                .encodeToString("bob@example.com:pw:with:colons".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void withoutCredentialsTheErrorSuggestsSigningIn() throws Exception {
        assertThat(run("whoami")).isEqualTo(1);

        assertThat(lastAuthorization.get()).isNull();
        assertThat(stderr.toString()).contains("unauthorized").contains("oscli login EMAIL");
    }

    @Test
    void uploadSendsTheFileAndMetadataAsMultipartAndPrintsTheObject() throws Exception {
        byte[] content = new byte[70_000];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) (i * 7);
        }
        Path file = Files.write(dir.resolve("photo.jpg"), content);

        int exit = run("upload", file.toString(), "--meta", "owner=ansh", "note=a=b");

        assertThat(exit).isZero();
        assertThat(lastRequest.get()).isEqualTo("POST /api/v1/objects");
        assertThat(lastContentType.get()).startsWith("multipart/form-data; boundary=");
        String sent = new String(lastBody.get(), StandardCharsets.ISO_8859_1);
        assertThat(sent).contains("name=\"metadata\"\r\n\r\n{\"owner\":\"ansh\",\"note\":\"a=b\"}\r\n");
        assertThat(sent).contains("name=\"file\"; filename=\"photo.jpg\"\r\nContent-Type: image/jpeg\r\n\r\n");
        assertThat(sent).contains(new String(content, StandardCharsets.ISO_8859_1));
        assertThat(stdout.toString()).contains("\"object_id\" : \"" + ID + "\"");
    }

    @Test
    void anUploadIsRetriedThroughSafeModeWithTheSameIdempotencyKey() throws Exception {
        Path file = Files.write(dir.resolve("a.bin"), new byte[] {1, 2, 3});
        retrySeconds = 10;
        unavailableAnswers.set(2);

        int exit = run("upload", file.toString());

        assertThat(exit).isZero();
        assertThat(idempotencyKeys).hasSize(3).allMatch(k -> k.equals(idempotencyKeys.get(0)));
        assertThat(stderr.toString()).contains("answered 503 (Retry-After 5); retrying");
    }

    @Test
    void anExplicitIdempotencyKeyIsSent() throws Exception {
        Path file = Files.write(dir.resolve("a.bin"), new byte[] {1});

        assertThat(run("upload", file.toString(), "--key", "my-key-1")).isZero();

        assertThat(idempotencyKeys).containsExactly("my-key-1");
    }

    @Test
    void withoutARetryBudgetA503IsReportedAsAnError() throws Exception {
        unavailableAnswers.set(1);

        assertThat(run("status")).isEqualTo(1);
        assertThat(stderr.toString()).contains("safe_mode");
    }

    @Test
    void uploadHonoursNameAndTypeOverrides() throws Exception {
        Path file = Files.write(dir.resolve("data.bin"), new byte[] {1, 2, 3});

        assertThat(run("upload", file.toString(), "--name", "report.pdf", "--type", "application/pdf")).isZero();

        String sent = new String(lastBody.get(), StandardCharsets.ISO_8859_1);
        assertThat(sent).contains("filename=\"report.pdf\"\r\nContent-Type: application/pdf\r\n");
        assertThat(sent).doesNotContain("name=\"metadata\"");
    }

    @Test
    void downloadWritesTheFileAndConfirmsItMatchesTheEtag() throws Exception {
        downloadEtag = sha256(downloadBody);
        Path target = dir.resolve("out.bin");

        int exit = run("download", ID, target.toString());

        assertThat(exit).isZero();
        assertThat(Files.readAllBytes(target)).isEqualTo(downloadBody);
        assertThat(stdout.toString()).contains("\"etag_match\" : true", "\"bytes\" : " + downloadBody.length,
                "\"sha256\" : \"" + downloadEtag + "\"");
        assertThat(dir.resolve("out.bin.part")).doesNotExist();
    }

    @Test
    void downloadExitsWith3WhenTheBytesDoNotMatchTheAnnouncedChecksum() throws Exception {
        downloadEtag = "0".repeat(64);

        int exit = run("download", ID, dir.resolve("out.bin").toString());

        assertThat(exit).isEqualTo(3);
        assertThat(stdout.toString()).contains("\"etag_match\" : false");
    }

    @Test
    void patchSendsStringsToSetAndNullsToRemove() throws Exception {
        assertThat(run("patch", ID, "owner=team", "tag=v1", "--remove", "old", "older")).isZero();

        assertThat(lastRequest.get()).isEqualTo("PATCH /api/v1/objects/" + ID + "/metadata");
        assertThat(new String(lastBody.get(), StandardCharsets.UTF_8))
                .isEqualTo("{\"owner\":\"team\",\"tag\":\"v1\",\"old\":null,\"older\":null}");
    }

    @Test
    void listPassesPagingParameters() throws Exception {
        assertThat(run("list", "--limit", "5", "--offset", "10")).isZero();
        assertThat(lastRequest.get()).isEqualTo("GET /api/v1/objects?limit=5&offset=10");

        assertThat(run("list")).isZero();
        assertThat(lastRequest.get()).isEqualTo("GET /api/v1/objects");
    }

    @Test
    void deleteExpects204() throws Exception {
        assertThat(run("delete", ID)).isZero();
        assertThat(lastRequest.get()).isEqualTo("DELETE /api/v1/objects/" + ID);
        assertThat(stdout.toString()).contains("\"deleted\" : \"" + ID + "\"");
    }

    @Test
    void whereShowsOneRowPerChunkWithItsCopies() throws Exception {
        assertThat(run("where", ID)).isZero();

        String table = stdout.toString();
        assertThat(table).contains("chunks  2   replication_ok=true");
        assertThat(table).containsPattern("0\\s+" + ID + "-00000\\s+1048576\\s+worker1\\(OK\\)\\s+worker2\\(OK,DEAD\\)");
        assertThat(table).containsPattern("1\\s+" + ID + "-00001\\s+10\\s+worker2\\(CORRUPT\\)\\s+worker3\\(OK\\)");
        assertThat(table).contains("copies per worker: worker1=1  worker2=2  worker3=1");
    }

    @Test
    void aServerErrorIsPrintedToStderrAndExitsWith1() throws Exception {
        int exit = run("meta", "00000000-0000-0000-0000-000000000000");

        assertThat(exit).isEqualTo(1);
        assertThat(stdout.toString()).isEmpty();
        assertThat(stderr.toString()).contains("\"error\" : \"object_not_found\"");
    }

    @Test
    void anUnreachableControllerExitsWith2() {
        server.stop(0);

        int exit = run("status");

        assertThat(exit).isEqualTo(2);
        assertThat(stderr.toString()).contains("\"error\" : \"connection_failed\"");
    }

    @Test
    void wrongUsageExitsWith64() {
        assertThat(run()).isEqualTo(64);
        assertThat(run("frobnicate")).isEqualTo(64);
        assertThat(run("download", ID)).isEqualTo(64);
        assertThat(run("upload", dir.resolve("missing.bin").toString())).isEqualTo(64);
        assertThat(run("patch", ID)).isEqualTo(64);
        assertThat(run("patch", ID, "novalue")).isEqualTo(64);
    }

    @Test
    void theBaseUrlComesFromTheEnvironmentUnlessOverridden() {
        ObjectStoreCli fromEnvironment = new ObjectStoreCli(new PrintStream(stdout), new PrintStream(stderr), base + "/");
        assertThat(fromEnvironment.run("status")).isZero();
        assertThat(lastRequest.get()).isEqualTo("GET /api/v1/cluster/status");

        ObjectStoreCli overridden = new ObjectStoreCli(new PrintStream(stdout), new PrintStream(stderr), "http://127.0.0.1:1");
        assertThat(overridden.run("status", "--base", base)).isZero();
    }

    private int run(String... args) {
        String[] withBase = new String[args.length + 2];
        withBase[0] = "--base";
        withBase[1] = base;
        System.arraycopy(args, 0, withBase, 2, args.length);
        environment.putIfAbsent("OSCLI_SESSION_FILE", dir.resolve("no-session").toString());
        return new ObjectStoreCli(new PrintStream(stdout), new PrintStream(stderr), null, retrySeconds, environment)
                .run(withBase);
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
            String target = exchange.getRequestURI().toString();
            lastRequest.set(method + " " + target);
            lastContentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
            lastBody.set(exchange.getRequestBody().readAllBytes());
            lastAuthorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            String key = exchange.getRequestHeaders().getFirst("Idempotency-Key");
            if (key != null) {
                idempotencyKeys.add(key);
            }
            if (unavailableAnswers.getAndDecrement() > 0) {
                exchange.getResponseHeaders().add("Retry-After", "5");
                respond(exchange, 503, "{\"error\":\"safe_mode\",\"detail\":\"starting\"}");
                return;
            }

            if (target.equals("/api/v1/auth/login")) {
                respond(exchange, 200, "{\"token\":\"tok-123\",\"token_type\":\"Bearer\",\"expires_at\":\"t\","
                        + "\"account\":{\"user_id\":\"u1\",\"email\":\"alice@example.com\",\"admin\":false}}");
            } else if (target.equals("/api/v1/auth/logout")) {
                exchange.sendResponseHeaders(204, -1);
            } else if (target.equals("/api/v1/auth/me") && lastAuthorization.get() == null) {
                respond(exchange, 401, "{\"error\":\"unauthorized\",\"detail\":\"sign in first\"}");
            } else if (target.contains("00000000-0000-0000-0000-000000000000")) {
                respond(exchange, 404, "{\"error\":\"object_not_found\",\"detail\":\"no such object\"}");
            } else if (method.equals("POST")) {
                respond(exchange, 201, OBJECT_JSON);
            } else if (method.equals("DELETE")) {
                exchange.sendResponseHeaders(204, -1);
            } else if (method.equals("GET") && target.equals("/api/v1/objects/" + ID)) {
                exchange.getResponseHeaders().add("Content-Type", "text/plain");
                exchange.getResponseHeaders().add("ETag", "\"" + downloadEtag + "\"");
                exchange.sendResponseHeaders(200, downloadBody.length);
                exchange.getResponseBody().write(downloadBody);
            } else if (target.startsWith("/api/v1/objects/" + ID)) {
                respond(exchange, 200, OBJECT_JSON);
            } else {
                respond(exchange, 200, "{\"items\":[],\"total\":0}");
            }
        }
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    private static String sha256(byte[] data) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
    }
}
