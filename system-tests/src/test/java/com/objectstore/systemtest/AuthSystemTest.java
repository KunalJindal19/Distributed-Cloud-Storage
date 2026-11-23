package com.objectstore.systemtest;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.http.HttpResponse;
import java.util.Map;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

/** Client accounts: registration, sign-in, and one client never reaching another client's objects. */
class AuthSystemTest extends ClusterTest {

    private static final String PASSWORD = "correct horse battery";

    private Api register(String email) {
        HttpResponse<String> response = api.withAuthorization(null)
                .postJson("/api/v1/auth/register", "{\"email\":\"" + email + "\",\"password\":\"" + PASSWORD + "\"}");
        assertThat(response.statusCode()).as(response.body()).isEqualTo(201);
        return api.as(email, PASSWORD);
    }

    private String error(HttpResponse<String> response) {
        return api.json(response.body()).get("error").asString();
    }

    @Test
    void anObjectIsVisibleOnlyToItsOwnerAndTheAdmin() {
        Api alice = register("alice@example.com");
        Api bob = register("Bob@Example.com");

        HttpResponse<String> uploaded = alice.upload("alice.bin", Api.randomBytes(3 * MIB + 5, 1), "{\"k\":\"v\"}", null);
        assertThat(uploaded.statusCode()).isEqualTo(201);
        JsonNode object = api.json(uploaded.body());
        String id = object.get("object_id").asString();
        assertThat(object.get("owner").asString()).isEqualTo("alice@example.com");

        // Another client: the object does not exist for it, whatever it tries.
        assertThat(bob.get("/api/v1/objects/" + id + "/metadata").statusCode()).isEqualTo(404);
        assertThat(error(bob.get("/api/v1/objects/" + id + "/metadata"))).isEqualTo("object_not_found");
        assertThat(bob.download(id).statusCode()).isEqualTo(404);
        assertThat(bob.patch(id, "{\"k\":\"stolen\"}").statusCode()).isEqualTo(404);
        assertThat(bob.delete(id).statusCode()).isEqualTo(404);
        assertThat(api.json(bob.get("/api/v1/objects").body()).get("total").asLong()).isZero();

        // The owner sees it, unchanged by the attempts above.
        JsonNode mine = api.json(alice.get("/api/v1/objects").body());
        assertThat(mine.get("total").asLong()).isEqualTo(1);
        assertThat(mine.get("items").get(0).get("owner").asString()).isEqualTo("alice@example.com");
        assertThat(alice.metadata(id).get("custom_metadata").get("k").asString()).isEqualTo("v");
        assertThat(alice.download(id).body()).isEqualTo(Api.randomBytes(3 * MIB + 5, 1));

        // Idempotency-Keys belong to the account: the same key from two clients makes two objects.
        HttpResponse<String> a = alice.upload("a.bin", Api.randomBytes(1000, 2), null, "shared-key");
        HttpResponse<String> b = bob.upload("b.bin", Api.randomBytes(1000, 3), null, "shared-key");
        assertThat(a.statusCode()).isEqualTo(201);
        assertThat(b.statusCode()).isEqualTo(201);
        assertThat(api.json(a.body()).get("object_id").asString()).isNotEqualTo(api.json(b.body()).get("object_id").asString());
        assertThat(api.json(bob.get("/api/v1/objects").body()).get("total").asLong()).isEqualTo(1);

        // The admin sees and may change everything, and can inspect any object's copies on the workers.
        assertThat(api.json(api.get("/api/v1/objects").body()).get("total").asLong()).isEqualTo(3);
        JsonNode inspection = api.json(api.get("/api/v1/admin/objects/" + id).body());
        assertThat(inspection.get("object").get("owner").asString()).isEqualTo("alice@example.com");
        assertThat(inspection.get("chunks")).hasSize(4);
        assertThat(inspection.get("chunks_by_health").get("OK").asInt()).isEqualTo(4);
        for (JsonNode chunk : inspection.get("chunks")) {
            assertThat(chunk.get("copies")).hasSize(2);
            assertThat(chunk.get("good").asInt()).isEqualTo(2);
            assertThat(chunk.get("copies").get(0).get("version_label").asString()).startsWith("t1.");
        }
        int copies = 0;
        for (JsonNode worker : inspection.get("workers")) {
            copies += worker.get("copies").asInt();
        }
        assertThat(copies).isEqualTo(8);
        assertThat(alice.get("/api/v1/admin/objects/" + id).statusCode()).isEqualTo(403);
        assertThat(api.get("/api/v1/admin/objects/00000000-0000-0000-0000-000000000000").statusCode()).isEqualTo(404);
        assertThat(api.metadata(id).get("owner").asString()).isEqualTo("alice@example.com");
        assertThat(api.delete(id).statusCode()).isEqualTo(204);
        assertThat(alice.get("/api/v1/objects/" + id + "/metadata").statusCode()).isEqualTo(404);
    }

    @Test
    void registrationSignInSessionsAndCookies() {
        Api anonymous = api.withAuthorization(null);
        HttpResponse<String> refused = anonymous.get("/api/v1/objects");
        assertThat(refused.statusCode()).isEqualTo(401);
        assertThat(error(refused)).isEqualTo("unauthorized");
        assertThat(refused.headers().firstValue("WWW-Authenticate").orElse("")).startsWith("Bearer");
        assertThat(anonymous.get("/api/v1/cluster/status").statusCode()).isEqualTo(401);

        Api carol = register("carol@example.com");
        String carolJson = "{\"email\":\"carol@example.com\",\"password\":\"" + PASSWORD + "\"}";
        assertThat(error(anonymous.postJson("/api/v1/auth/register", carolJson))).isEqualTo("email_taken");
        assertThat(error(anonymous.postJson("/api/v1/auth/register",
                "{\"email\":\"ADMIN@gmail.com\",\"password\":\"" + PASSWORD + "\"}"))).isEqualTo("email_taken");
        assertThat(anonymous.postJson("/api/v1/auth/register", "{\"email\":\"not-an-email\",\"password\":\"" + PASSWORD + "\"}")
                .statusCode()).isEqualTo(400);
        assertThat(anonymous.postJson("/api/v1/auth/register", "{\"email\":\"dave@example.com\",\"password\":\"short\"}")
                .statusCode()).isEqualTo(400);

        // Wrong passwords, as a sign-in and as Basic credentials.
        HttpResponse<String> wrong = anonymous.postJson("/api/v1/auth/login",
                "{\"email\":\"carol@example.com\",\"password\":\"wrong password\"}");
        assertThat(wrong.statusCode()).isEqualTo(401);
        assertThat(error(wrong)).isEqualTo("invalid_credentials");
        assertThat(api.as("carol@example.com", "wrong password").get("/api/v1/objects").statusCode()).isEqualTo(401);
        assertThat(api.as("nobody@example.com", PASSWORD).get("/api/v1/objects").statusCode()).isEqualTo(401);

        // A session token.
        HttpResponse<String> login = anonymous.postJson("/api/v1/auth/login", carolJson);
        assertThat(login.statusCode()).isEqualTo(200);
        String token = api.json(login.body()).get("token").asString();
        String cookie = login.headers().firstValue("Set-Cookie").orElse("");
        assertThat(cookie).startsWith("os_session=" + token).contains("HttpOnly").contains("SameSite=Strict");
        Api bearer = api.withAuthorization("Bearer " + token);
        JsonNode me = api.json(bearer.get("/api/v1/auth/me").body());
        assertThat(me.get("email").asString()).isEqualTo("carol@example.com");
        assertThat(me.get("admin").asBoolean()).isFalse();
        assertThat(api.json(api.get("/api/v1/auth/me").body()).get("admin").asBoolean()).isTrue();

        // The browser cookie: reads work; a change also needs X-Requested-With (cross-site requests cannot add it).
        Map<String, String> cookieOnly = Map.of("Cookie", "os_session=" + token);
        assertThat(anonymous.request("GET", "/api/v1/objects", null, cookieOnly).statusCode()).isEqualTo(200);
        String id = carol.uploadOk("carol.bin", Api.randomBytes(500, 4));
        HttpResponse<String> forged = anonymous.request("DELETE", "/api/v1/objects/" + id, null, cookieOnly);
        assertThat(forged.statusCode()).isEqualTo(403);
        assertThat(error(forged)).isEqualTo("forbidden");
        assertThat(anonymous.request("DELETE", "/api/v1/objects/" + id, null,
                Map.of("Cookie", "os_session=" + token, "X-Requested-With", "XMLHttpRequest")).statusCode()).isEqualTo(204);

        // The cluster's inner state (the dashboard's data) and fsck: the admin only. Clients get a summary.
        assertThat(carol.get("/api/v1/fsck").statusCode()).isEqualTo(403);
        assertThat(carol.get("/api/v1/cluster/status").statusCode()).isEqualTo(403);
        assertThat(carol.get("/api/v1/admin/objects/" + id).statusCode()).isEqualTo(403);
        JsonNode health = api.json(carol.get("/api/v1/cluster/health").body());
        assertThat(health.get("workers_ready").asInt()).isEqualTo(4);
        assertThat(health.get("objects").asLong()).as("carol's own objects only").isZero();
        assertThat(api.json(api.get("/api/v1/cluster/health").body()).get("objects").asLong()).isZero();

        // Signing out ends the session.
        assertThat(bearer.postJson("/api/v1/auth/logout", "{}").statusCode()).isEqualTo(204);
        assertThat(bearer.get("/api/v1/auth/me").statusCode()).isEqualTo(401);
        assertThat(anonymous.request("GET", "/api/v1/objects", null, cookieOnly).statusCode()).isEqualTo(401);
    }
}
