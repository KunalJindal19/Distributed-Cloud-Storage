package com.objectstore.client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;

/** Guesses the MIME type of a file from its name, so uploads carry a meaningful Content-Type. */
final class ContentTypes {

    static final String OCTET_STREAM = "application/octet-stream";

    private static final Map<String, String> BY_EXTENSION = Map.ofEntries(
            Map.entry("txt", "text/plain"), Map.entry("md", "text/markdown"), Map.entry("csv", "text/csv"),
            Map.entry("html", "text/html"), Map.entry("htm", "text/html"), Map.entry("css", "text/css"),
            Map.entry("js", "text/javascript"), Map.entry("json", "application/json"),
            Map.entry("xml", "application/xml"), Map.entry("pdf", "application/pdf"),
            Map.entry("doc", "application/msword"),
            Map.entry("docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
            Map.entry("xls", "application/vnd.ms-excel"),
            Map.entry("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            Map.entry("ppt", "application/vnd.ms-powerpoint"),
            Map.entry("pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation"),
            Map.entry("jpg", "image/jpeg"), Map.entry("jpeg", "image/jpeg"), Map.entry("png", "image/png"),
            Map.entry("gif", "image/gif"), Map.entry("webp", "image/webp"), Map.entry("svg", "image/svg+xml"),
            Map.entry("bmp", "image/bmp"), Map.entry("tif", "image/tiff"), Map.entry("tiff", "image/tiff"),
            Map.entry("mp3", "audio/mpeg"), Map.entry("wav", "audio/wav"), Map.entry("flac", "audio/flac"),
            Map.entry("ogg", "audio/ogg"), Map.entry("m4a", "audio/mp4"),
            Map.entry("mp4", "video/mp4"), Map.entry("mov", "video/quicktime"), Map.entry("mkv", "video/x-matroska"),
            Map.entry("webm", "video/webm"), Map.entry("avi", "video/x-msvideo"),
            Map.entry("zip", "application/zip"), Map.entry("gz", "application/gzip"),
            Map.entry("tgz", "application/gzip"), Map.entry("tar", "application/x-tar"),
            Map.entry("bz2", "application/x-bzip2"), Map.entry("xz", "application/x-xz"),
            Map.entry("7z", "application/x-7z-compressed"), Map.entry("rar", "application/vnd.rar"),
            Map.entry("jar", "application/java-archive"));

    private ContentTypes() {
    }

    static String guess(Path file) {
        String name = file.getFileName().toString();
        int dot = name.lastIndexOf('.');
        if (dot >= 0 && dot < name.length() - 1) {
            String known = BY_EXTENSION.get(name.substring(dot + 1).toLowerCase(Locale.ROOT));
            if (known != null) {
                return known;
            }
        }
        try {
            String probed = Files.probeContentType(file);
            if (probed != null && !probed.isBlank()) {
                return probed;
            }
        } catch (IOException ignored) {
            // fall through to the generic type
        }
        return OCTET_STREAM;
    }
}
