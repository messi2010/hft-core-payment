package com.payments.ledger.domain;

import java.math.BigInteger;
import java.util.UUID;

/**
 * Unsigned 128-bit integer used for account and transfer ids. Storing ids as
 * 128-bit lets callers use
 * externally-generated identifiers (UUID/ULID) directly, with no central
 * sequence and a negligible collision probability, instead of coordinating u64
 * ids across upstream systems.
 *
 * <p>Amounts deliberately stay u64 ({@code long}); only identifiers are 128-bit.
 *
 * <p>Represented as two longs ({@code hi}, {@code lo}), each treated as an
 * unsigned 64-bit half. Immutable; usable as a map key (record equals/hashCode).
 */
public record UInt128(long hi, long lo) implements Comparable<UInt128> {

    public static final UInt128 ZERO = new UInt128(0, 0);

    /** From a single unsigned 64-bit value (high half zero). Handy for small ids/tests. */
    public static UInt128 of(long lo) {
        return new UInt128(0, lo);
    }

    public static UInt128 of(long hi, long lo) {
        return new UInt128(hi, lo);
    }

    public static UInt128 fromUuid(UUID uuid) {
        return new UInt128(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    /**
     * Parse a string id. Accepts a UUID (contains '-') or an unsigned decimal
     * integer (0 .. 2^128-1). Empty/null is treated as {@link #ZERO}.
     */
    public static UInt128 parse(String s) {
        if (s == null || s.isEmpty()) return ZERO;
        if (s.indexOf('-') >= 0) return fromUuid(UUID.fromString(s));
        return fromBigInteger(new BigInteger(s));
    }

    public static UInt128 fromBigInteger(BigInteger v) {
        if (v.signum() < 0 || v.bitLength() > 128) {
            throw new IllegalArgumentException("value out of unsigned 128-bit range: " + v);
        }
        BigInteger lo = v.and(LOW_MASK);
        BigInteger hi = v.shiftRight(64);
        return new UInt128(hi.longValue(), lo.longValue());
    }

    public boolean isZero() {
        return hi == 0 && lo == 0;
    }

    public BigInteger toBigInteger() {
        BigInteger hiB = unsigned(hi).shiftLeft(64);
        return hiB.or(unsigned(lo));
    }

    /** Unsigned ordering: compare the high half first, then the low half. */
    @Override
    public int compareTo(UInt128 o) {
        int c = Long.compareUnsigned(hi, o.hi);
        return c != 0 ? c : Long.compareUnsigned(lo, o.lo);
    }

    /** Canonical string form: unsigned decimal (round-trips through {@link #parse}). */
    @Override
    public String toString() {
        return toBigInteger().toString();
    }

    private static final BigInteger LOW_MASK = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    private static BigInteger unsigned(long v) {
        BigInteger b = BigInteger.valueOf(v & 0x7FFFFFFFFFFFFFFFL);
        return v < 0 ? b.setBit(63) : b;
    }
}
