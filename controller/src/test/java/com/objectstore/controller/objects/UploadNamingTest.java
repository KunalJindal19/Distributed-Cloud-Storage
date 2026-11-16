package com.objectstore.controller.objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.objectstore.common.web.ApiException;
import org.junit.jupiter.api.Test;

class UploadNamingTest {

    @Test
    void theObjectNameIsTheFileNameWithoutAnyClientSideDirectory() {
        assertThat(UploadService.objectName("report.pdf")).isEqualTo("report.pdf");
        assertThat(UploadService.objectName("/home/me/report.pdf")).isEqualTo("report.pdf");
        assertThat(UploadService.objectName("C:\\Users\\me\\report.pdf")).isEqualTo("report.pdf");
        assertThat(UploadService.objectName("r\u00e9sum\u00e9 \"final\".pdf")).isEqualTo("r\u00e9sum\u00e9 \"final\".pdf");
    }

    @Test
    void aMissingOrBlankFileNameBecomesUnnamed() {
        assertThat(UploadService.objectName(null)).isEqualTo("unnamed");
        assertThat(UploadService.objectName("")).isEqualTo("unnamed");
        assertThat(UploadService.objectName("  ")).isEqualTo("unnamed");
        assertThat(UploadService.objectName("dir/")).isEqualTo("unnamed");
    }

    @Test
    void controlCharactersAreDroppedAndOverlongNamesRejected() {
        assertThat(UploadService.objectName("a\r\nb\u0000.txt")).isEqualTo("ab.txt");
        assertThat(UploadService.objectName("x".repeat(512))).hasSize(512);
        assertThatThrownBy(() -> UploadService.objectName("x".repeat(513))).isInstanceOf(ApiException.class);
    }

    @Test
    void aSpecificDeclaredContentTypeIsKept() {
        assertThat(UploadService.contentType("image/png", "whatever.bin")).isEqualTo("image/png");
        assertThat(UploadService.contentType("text/plain;charset=UTF-8", "notes")).isEqualTo("text/plain;charset=UTF-8");
    }

    @Test
    void aMissingOrGenericContentTypeIsInferredFromTheExtension() {
        assertThat(UploadService.contentType(null, "photo.jpg")).isEqualTo("image/jpeg");
        assertThat(UploadService.contentType("", "clip.mp4")).isEqualTo("video/mp4");
        assertThat(UploadService.contentType("application/octet-stream", "song.mp3")).isEqualTo("audio/mpeg");
        assertThat(UploadService.contentType("application/octet-stream", "archive.zip")).isEqualTo("application/zip");
        assertThat(UploadService.contentType("not a media type", "doc.pdf")).isEqualTo("application/pdf");
    }

    @Test
    void anythingUnrecognisedIsOctetStream() {
        assertThat(UploadService.contentType(null, "blob")).isEqualTo("application/octet-stream");
        assertThat(UploadService.contentType("application/octet-stream", "data.unknownext"))
                .isEqualTo("application/octet-stream");
    }
}
