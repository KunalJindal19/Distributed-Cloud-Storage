package com.objectstore.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ChunkIdsTest {

    private static final String OBJECT_ID = "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a";

    @Test
    void formatsIndexWithFiveDigitsSoIdsSortInChunkOrder() {
        assertThat(ChunkIds.of(OBJECT_ID, 0)).isEqualTo(OBJECT_ID + "-00000");
        assertThat(ChunkIds.of(OBJECT_ID, 3)).isEqualTo(OBJECT_ID + "-00003");
        assertThat(ChunkIds.of(OBJECT_ID, 99_999)).isEqualTo(OBJECT_ID + "-99999");
    }

    @Test
    void roundTripsObjectIdAndIndex() {
        String chunkId = ChunkIds.of(OBJECT_ID, 42);

        assertThat(ChunkIds.isValid(chunkId)).isTrue();
        assertThat(ChunkIds.objectIdOf(chunkId)).isEqualTo(OBJECT_ID);
        assertThat(ChunkIds.indexOf(chunkId)).isEqualTo(42);
    }

    @Test
    void rejectsIndexesThatDoNotFitTheSuffix() {
        assertThatThrownBy(() -> ChunkIds.of(OBJECT_ID, -1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ChunkIds.of(OBJECT_ID, 100_000)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "test-00000",
            "../../etc/passwd",
            "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a",
            "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a-0000",
            "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a-000000",
            "3F1C9A52-7D0E-4C1B-9A54-1F2E3D4C5B6A-00000",
            "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a-00000.tmp",
            "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a-00000/x",
            "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a-00000\n"
    })
    void rejectsAnythingThatIsNotExactlyAChunkId(String candidate) {
        assertThat(ChunkIds.isValid(candidate)).isFalse();
    }

    @Test
    void rejectsNull() {
        assertThat(ChunkIds.isValid(null)).isFalse();
    }
}
