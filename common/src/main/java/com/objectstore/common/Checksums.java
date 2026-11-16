package com.objectstore.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.regex.Pattern;

/**
 * SHA-256 helpers. Every checksum in the system is SHA-256 rendered as 64 lowercase hex characters,
 * with no prefix.
 */
public final class Checksums {

    private static final HexFormat HEX = HexFormat.of();
    private static final Pattern SHA256_HEX = Pattern.compile("^[a-f0-9]{64}$");

    private Checksums() {
    }

    public static MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated by the Java platform specification.
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    public static String sha256Hex(byte[] data) {
        return HEX.formatHex(newSha256().digest(data));
    }

    /** Hashes a file without loading it fully into memory. */
    public static String sha256Hex(Path file) throws IOException {
        MessageDigest digest = newSha256();
        byte[] buffer = new byte[64 * 1024];
        try (InputStream in = Files.newInputStream(file)) {
            int read;
            while ((read = in.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        return HEX.formatHex(digest.digest());
    }

    /** Finishes the digest and renders it as lowercase hex. */
    public static String hex(MessageDigest digest) {
        return HEX.formatHex(digest.digest());
    }

    public static boolean isValidSha256Hex(String value) {
        return value != null && SHA256_HEX.matcher(value).matches();
    }
}
