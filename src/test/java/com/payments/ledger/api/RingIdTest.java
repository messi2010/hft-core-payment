package com.payments.ledger.api;

import com.payments.ledger.domain.UInt128;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("RingId — wrap external id → UInt128(hi/lo) at the ring-buffer boundary")
class RingIdTest {

    private static final UUID UUID_SAMPLE = UUID.fromString("6f9619ff-8b86-d011-b42d-00cf4fc964ff");

    @Test
    @DisplayName("UUID → hi = mostSignificantBits, lo = leastSignificantBits")
    void uuidMapsToHiLo() {
        UInt128 id = RingId.of(UUID_SAMPLE);
        assertThat(id.hi()).isEqualTo(UUID_SAMPLE.getMostSignificantBits());
        assertThat(id.lo()).isEqualTo(UUID_SAMPLE.getLeastSignificantBits());
    }

    @Test
    @DisplayName("UUID round-trips: of(uuid).toUuid() == uuid")
    void roundTrip() {
        assertThat(RingId.of(UUID_SAMPLE).toUuid()).isEqualTo(UUID_SAMPLE);
    }

    @Test
    @DisplayName("UUID object and its string form produce the same id")
    void uuidObjectEqualsUuidString() {
        assertThat(RingId.of(UUID_SAMPLE)).isEqualTo(RingId.of(UUID_SAMPLE.toString()));
    }

    @Test
    @DisplayName("unsigned-decimal string still works (backward compatible)")
    void decimalStringStillWorks() {
        assertThat(RingId.of("1001")).isEqualTo(UInt128.of(1001));
    }

    @Test
    @DisplayName("null / empty → ZERO")
    void emptyIsZero() {
        assertThat(RingId.of((String) null)).isEqualTo(UInt128.ZERO);
        assertThat(RingId.of("")).isEqualTo(UInt128.ZERO);
    }
}
