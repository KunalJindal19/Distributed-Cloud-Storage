package com.objectstore.common.dto;

import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.InternalApi;
import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** Block 17: the v5 protocol messages survive a JSON round trip with their wire names. */
class ProtocolDtoTest {

    private final ObjectMapper json = JsonMapper.builder().build();

    private <T> T roundTrip(T value, Class<T> type) {
        return json.readValue(json.writeValueAsString(value), type);
    }

    @Test
    void changesCarryVersionAndDetail() {
        Change received = Change.received(7, "c", 10, "s", 42, "cmd");
        Change deleted = Change.deleted(8, "c", 43, InternalApi.DELETED_CORRUPT);
        Change failed = Change.cmdFailed(9, "c", 44, InternalApi.FAILED_TRANSFER, "http://w:1", "cmd", "boom");
        assertThat(roundTrip(received, Change.class)).isEqualTo(received);
        assertThat(roundTrip(deleted, Change.class)).isEqualTo(deleted);
        assertThat(roundTrip(failed, Change.class)).isEqualTo(failed);
        JsonNode tree = json.readTree(json.writeValueAsString(deleted));
        assertThat(tree.path("version").asLong()).isEqualTo(43);
        assertThat(tree.path("detail").asString()).isEqualTo("corrupt");
        assertThat(ChangeType.values()).containsExactly(ChangeType.RECEIVED, ChangeType.DELETED, ChangeType.CMD_FAILED);
    }

    @Test
    void commandsHaveVersionAndExpectedChecksumButNoExpiry() {
        Command command = new Command("id", InternalApi.COMMAND_VERIFY, "c", "sum", null, null, 12);
        assertThat(roundTrip(command, Command.class)).isEqualTo(command);
        JsonNode tree = json.readTree(json.writeValueAsString(command));
        assertThat(tree.has("expected_checksum")).isTrue();
        assertThat(tree.has("expires_at")).isFalse();
        assertThat(tree.has("checksum")).isFalse();
    }

    @Test
    void registrationAndHeartbeatCarryTheResync() {
        RegisterResponse register = new RegisterResponse(true, "cluster", "ALIVE", 1L << 41, 1);
        assertThat(roundTrip(register, RegisterResponse.class)).isEqualTo(register);
        HeartbeatResponse reply = new HeartbeatResponse(true, "ALIVE", List.of(), new Resync(88, 2), null, 5,
                new SegmentAck("r", 0, InternalApi.SEGMENT_DONE));
        assertThat(roundTrip(reply, HeartbeatResponse.class)).isEqualTo(reply);
        JsonNode tree = json.readTree(json.writeValueAsString(reply));
        assertThat(tree.path("resync").path("t0").asLong()).isEqualTo(88);
        assertThat(tree.has("request_full_report")).isFalse();
        HeartbeatRequest heartbeat = new HeartbeatRequest("w", "b", 3, 1.5, new WorkerStats(1, 2, 3, 0, 4, 5, 6),
                List.of(), new ReportSegment("r", 88, 2, 0, true, 9, List.of(new ReportEntry("c", 1, "s", 40))),
                InternalApi.RESYNC_TOMBSTONES);
        assertThat(roundTrip(heartbeat, HeartbeatRequest.class)).isEqualTo(heartbeat);
    }

    @Test
    void controllerVersionsAreEvenAndPositive() {
        assertThat(InternalApi.isControllerVersion(2)).isTrue();
        assertThat(InternalApi.isControllerVersion(3)).isFalse();
        assertThat(InternalApi.isControllerVersion(0)).isFalse();
        assertThat(InternalApi.isControllerVersion(-2)).isFalse();
    }
}
