package com.objectstore.controller.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** Block 17 (v5 §3.2): even versions from term + seq, never reused across terms. */
class OpVersionsTest {

    @Test
    void versionsAreEvenIncreasingAndCurrentIsTheHighestIssued() {
        OpVersions versions = new OpVersions(3);
        assertThat(versions.current()).isEqualTo(3L << 40);
        long a = versions.next();
        long b = versions.next();
        assertThat(a & 1).isZero();
        assertThat(b).isGreaterThan(a);
        assertThat(versions.current()).isEqualTo(b);
        assertThat(OpVersions.termOf(b)).isEqualTo(3);
    }

    @Test
    void everyVersionOfALaterTermIsAboveEveryVersionOfAnEarlierOne() {
        OpVersions earlier = new OpVersions(1);
        earlier.setSeqForTests(OpVersions.SEQ_LIMIT - 2);
        long last = earlier.next();
        OpVersions later = new OpVersions(2);
        assertThat(later.next()).isGreaterThan(last);
        assertThat(later.current()).isGreaterThan(last);
    }

    @Test
    void anExhaustedSequenceThrowsAndStopsTheController() {
        OpVersions versions = new OpVersions(1);
        AtomicBoolean stopped = new AtomicBoolean();
        versions.setOnExhaustedForTests(() -> stopped.set(true));
        versions.setSeqForTests(OpVersions.SEQ_LIMIT - 1);

        assertThatThrownBy(versions::next).isInstanceOf(IllegalStateException.class);
        assertThat(stopped).isTrue();
    }

    @Test
    void noVersionIsIssuedBeforeTheTermIsSet() {
        assertThatThrownBy(() -> new OpVersions().next()).isInstanceOf(IllegalStateException.class);
    }
}
