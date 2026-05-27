package com.payments.tbjava.domain;

import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UInt128Test {

    @Test
    void zeroAndIsZero() {
        assertTrue(UInt128.ZERO.isZero());
        assertTrue(UInt128.of(0).isZero());
        assertFalse(UInt128.of(1).isZero());
    }

    @Test
    void decimalRoundTrip() {
        assertEquals(UInt128.of(1001), UInt128.parse("1001"));
        assertEquals("1001", UInt128.of(1001).toString());

        BigInteger big = BigInteger.ONE.shiftLeft(100).add(BigInteger.valueOf(12345));
        UInt128 v = UInt128.fromBigInteger(big);
        assertEquals(big, v.toBigInteger());
        assertEquals(v, UInt128.parse(big.toString()));
    }

    @Test
    void uuidRoundTrip() {
        UUID uuid = UUID.fromString("11111111-2222-3333-4444-555555555555");
        UInt128 v = UInt128.fromUuid(uuid);
        assertEquals(v, UInt128.parse(uuid.toString()));
    }

    @Test
    void unsignedOrdering() {
        // lo with the sign bit set must sort ABOVE a small positive lo.
        assertTrue(UInt128.of(0, -1L).compareTo(UInt128.of(0, 1L)) > 0);
        // hi dominates lo.
        assertTrue(UInt128.of(1, 0).compareTo(UInt128.of(0, -1L)) > 0);
        assertEquals(0, UInt128.of(5, 7).compareTo(UInt128.of(5, 7)));
    }

    @Test
    void worksAsMapKey() {
        var m = new java.util.HashMap<UInt128, String>();
        m.put(UInt128.of(7), "a");
        m.put(UInt128.fromUuid(UUID.fromString("11111111-2222-3333-4444-555555555555")), "b");
        assertEquals("a", m.get(UInt128.of(7)));
        assertEquals("b", m.get(UInt128.parse("11111111-2222-3333-4444-555555555555")));
    }
}
