package com.payments.ledger.api;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.payments.ledger.domain.UInt128;

import java.io.IOException;

/**
 * Serializes {@link UInt128} ids as a single JSON string — friendly for clients
 * and round-trippable through {@link RingId#of(String)}:
 * <ul>
 *   <li>{@code hi == 0} → unsigned decimal (e.g. {@code "1001"}), best for small ids;</li>
 *   <li>{@code hi != 0} → canonical UUID string, best for externally-generated 128-bit ids.</li>
 * </ul>
 * Without this, Jackson would serialize the record as
 * {@code {"hi":0,"lo":1001,"zero":false}} — internal layout leaking out.
 */
public final class UInt128JsonSerializer extends JsonSerializer<UInt128> {
    @Override
    public void serialize(UInt128 value, JsonGenerator gen, SerializerProvider sp) throws IOException {
        if (value == null) { gen.writeNull(); return; }
        if (value.hi() == 0) {
            gen.writeString(Long.toUnsignedString(value.lo()));
        } else {
            gen.writeString(value.toUuid().toString());
        }
    }
}
