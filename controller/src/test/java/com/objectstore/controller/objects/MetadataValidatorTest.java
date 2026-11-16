package com.objectstore.controller.objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.objectstore.common.web.ApiException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.HttpStatus;
import tools.jackson.databind.json.JsonMapper;

class MetadataValidatorTest {

    private final MetadataValidator validator = new MetadataValidator(JsonMapper.builder().build());

    @Test
    void uploadMetadataIsOptional() {
        assertThat(validator.parseForUpload(null)).isEmpty();
        assertThat(validator.parseForUpload("")).isEmpty();
        assertThat(validator.parseForUpload("   ")).isEmpty();
        assertThat(validator.parseForUpload("{}")).isEmpty();
    }

    @Test
    void acceptsAFlatObjectOfStrings() {
        assertThat(validator.parseForUpload("{\"owner\":\"ansh\",\"project.name\":\"cloud\",\"a-b_c.9\":\"\"}"))
                .containsEntry("owner", "ansh")
                .containsEntry("project.name", "cloud")
                .containsEntry("a-b_c.9", "")
                .hasSize(3);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "[1,2]",
            "\"text\"",
            "42",
            "null",
            "{not json",
            "{\"owner\":1}",
            "{\"owner\":true}",
            "{\"owner\":null}",
            "{\"owner\":{\"nested\":\"x\"}}",
            "{\"owner\":[\"x\"]}",
            "{\"\":\"empty key\"}",
            "{\"has space\":\"x\"}",
            "{\"slash/key\":\"x\"}",
            "{\"unicod\u00e9\":\"x\"}"
    })
    void rejectsEverythingElseOnUpload(String raw) {
        assertInvalid(() -> validator.parseForUpload(raw));
    }

    @Test
    void enforcesKeyAndValueLengths() {
        String longestKey = "k".repeat(64);
        String longestValue = "v".repeat(1024);
        assertThat(validator.parseForUpload("{\"" + longestKey + "\":\"" + longestValue + "\"}")).hasSize(1);

        assertInvalid(() -> validator.parseForUpload("{\"" + longestKey + "k\":\"v\"}"));
        assertInvalid(() -> validator.parseForUpload("{\"k\":\"" + longestValue + "v\"}"));
    }

    @Test
    void allowsAtMostSixtyFourKeysOnUpload() {
        assertThat(validator.parseForUpload(objectWithKeys(64))).hasSize(64);
        assertInvalid(() -> validator.parseForUpload(objectWithKeys(65)));
    }

    @Test
    void inAPatchNullRemovesAKeyAndStringsSetIt() {
        MetadataValidator.Patch patch = validator.parseForPatch(bytes("{\"owner\":\"team\",\"tag\":null,\"v\":\"1\"}"));

        assertThat(patch.upserts()).containsOnly(entry("owner", "team"), entry("v", "1"));
        assertThat(patch.removals()).containsExactly("tag");
    }

    @Test
    void aPatchNeedsAJsonObjectBody() {
        assertInvalid(() -> validator.parseForPatch(null));
        assertInvalid(() -> validator.parseForPatch(new byte[0]));
        assertInvalid(() -> validator.parseForPatch(bytes("[]")));
        assertInvalid(() -> validator.parseForPatch(bytes("owner=team")));
        assertInvalid(() -> validator.parseForPatch(bytes("{\"owner\":5}")));
        assertInvalid(() -> validator.parseForPatch(bytes("{\"bad key\":\"x\"}")));
    }

    private static String objectWithKeys(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> "\"key" + i + "\":\"v\"")
                .collect(Collectors.joining(",", "{", "}"));
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static java.util.Map.Entry<String, String> entry(String key, String value) {
        return java.util.Map.entry(key, value);
    }

    private static void assertInvalid(ThrowingCallable call) {
        assertThatThrownBy(call)
                .isInstanceOfSatisfying(ApiException.class, e -> {
                    assertThat(e.status()).isEqualTo(HttpStatus.BAD_REQUEST);
                    assertThat(e.code()).isEqualTo("invalid_metadata");
                });
    }
}
