package com.objectstore.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ChecksumsTest {

    private static final String EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    private static final String ABC_SHA256 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    @Test
    void matchesTheKnownTestVectors() {
        assertThat(Checksums.sha256Hex(new byte[0])).isEqualTo(EMPTY_SHA256);
        assertThat(Checksums.sha256Hex("abc".getBytes(StandardCharsets.US_ASCII))).isEqualTo(ABC_SHA256);
    }

    @Test
    void hashingAFileGivesTheSameResultAsHashingItsBytes(@TempDir Path dir) throws IOException {
        byte[] data = new byte[200_000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i * 31);
        }
        Path file = Files.write(dir.resolve("blob.bin"), data);

        assertThat(Checksums.sha256Hex(file)).isEqualTo(Checksums.sha256Hex(data));
    }

    @Test
    void incrementalDigestEqualsOneShotDigest() {
        MessageDigest digest = Checksums.newSha256();
        digest.update("a".getBytes(StandardCharsets.US_ASCII));
        digest.update("bc".getBytes(StandardCharsets.US_ASCII));

        assertThat(Checksums.hex(digest)).isEqualTo(ABC_SHA256);
    }

    @Test
    void validatesTheHexFormat() {
        assertThat(Checksums.isValidSha256Hex(ABC_SHA256)).isTrue();
        assertThat(Checksums.isValidSha256Hex(ABC_SHA256.toUpperCase())).isFalse();
        assertThat(Checksums.isValidSha256Hex(ABC_SHA256.substring(1))).isFalse();
        assertThat(Checksums.isValidSha256Hex("sha256:" + ABC_SHA256)).isFalse();
        assertThat(Checksums.isValidSha256Hex(null)).isFalse();
    }
}
