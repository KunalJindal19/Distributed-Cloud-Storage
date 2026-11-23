package com.objectstore.controller.auth;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PasswordHasherTest {

    @Test
    void aHashVerifiesOnlyItsOwnPasswordAndIsSaltedEveryTime() {
        String first = PasswordHasher.hash("s3cret password");
        String second = PasswordHasher.hash("s3cret password");

        assertThat(first).startsWith("pbkdf2-sha256$" + PasswordHasher.ITERATIONS + "$").doesNotContain("s3cret");
        assertThat(first).isNotEqualTo(second);
        assertThat(PasswordHasher.verify("s3cret password", first)).isTrue();
        assertThat(PasswordHasher.verify("s3cret password", second)).isTrue();
        assertThat(PasswordHasher.verify("s3cret passworD", first)).isFalse();
        assertThat(PasswordHasher.verify("", first)).isFalse();
    }

    @Test
    void malformedStoredHashesNeverVerify() {
        assertThat(PasswordHasher.verify("x", null)).isFalse();
        assertThat(PasswordHasher.verify("x", "")).isFalse();
        assertThat(PasswordHasher.verify("x", "md5$1$abc$def")).isFalse();
        assertThat(PasswordHasher.verify("x", "pbkdf2-sha256$notanumber$abc$def")).isFalse();
        assertThat(PasswordHasher.verify("x", "pbkdf2-sha256$1000$!!!$def")).isFalse();
    }
}
