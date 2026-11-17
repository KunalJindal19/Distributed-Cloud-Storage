package com.objectstore.controller.objects;

import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.Checksums;
import com.objectstore.controller.cluster.WorkerClient.FetchedChunk;
import com.objectstore.controller.domain.Chunk;
import com.objectstore.controller.objects.DownloadService.Outcome;
import org.junit.jupiter.api.Test;

/** How a worker's 200 answer is classified: only disk corruption may mark a copy corrupt. */
class DownloadClassificationTest {

    private final byte[] data = "the quick brown fox jumps over the lazy dog".getBytes();
    private final String sum = Checksums.sha256Hex(data);
    private final Chunk chunk = new Chunk("c", "o", 0, data.length, sum);

    @Test
    void intactBytesAreGood() {
        assertThat(DownloadService.classify(chunk, new FetchedChunk(data, sum))).isEqualTo(Outcome.GOOD);
    }

    @Test
    void bytesChangedInTransitAreTransientNotCorrupt() {
        byte[] flipped = data.clone();
        flipped[3] ^= 1;
        // The worker vouches for the right checksum, but the bytes we received do not match it.
        assertThat(DownloadService.classify(chunk, new FetchedChunk(flipped, sum))).isEqualTo(Outcome.IN_TRANSIT);
    }

    @Test
    void aWrongChecksumFromTheWorkerMeansItsDiskCopyIsCorrupt() {
        byte[] other = "something else entirely, same length as the data..".substring(0, data.length).getBytes();
        assertThat(DownloadService.classify(chunk, new FetchedChunk(other, Checksums.sha256Hex(other))))
                .isEqualTo(Outcome.CORRUPT);
    }

    @Test
    void aTruncatedTransferIsTransient() {
        byte[] cut = java.util.Arrays.copyOf(data, data.length - 5);
        assertThat(DownloadService.classify(chunk, new FetchedChunk(cut, sum))).isEqualTo(Outcome.TRANSIENT);
    }
}
