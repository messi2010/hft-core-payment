package com.payments.ledger.api;

import com.payments.ledger.domain.UInt128;

import java.util.UUID;

/**
 * Boundary wrapper that turns an externally-supplied id into the engine's
 * internal {@link UInt128} (hi/lo) form — done at the API edge, <em>before</em>
 * the command is published onto the ring buffer. Centralizing the conversion
 * here keeps the controller declarative and gives one place to evolve the
 * accepted id formats.
 *
 * <p>Accepted inputs:
 * <ul>
 *   <li>a {@link UUID} (canonical for externally-generated ids) → {@code hi = MSB}, {@code lo = LSB};</li>
 *   <li>a UUID string (contains '-'), e.g. {@code "6f9619ff-8b86-d011-b42d-00cf4fc964ff"};</li>
 *   <li>an unsigned decimal string, e.g. {@code "1001"};</li>
 *   <li>{@code null}/empty → {@link UInt128#ZERO}.</li>
 * </ul>
 */
public final class RingId {

    private RingId() {}

    /** UUID → {@link UInt128}(hi = most-significant bits, lo = least-significant bits). */
    public static UInt128 of(UUID uuid) {
        return UInt128.fromUuid(uuid);
    }

    /** UUID string or unsigned-decimal string → {@link UInt128}; {@code null}/empty → ZERO. */
    public static UInt128 of(String idOrUuid) {
        return UInt128.parse(idOrUuid);
    }
}
