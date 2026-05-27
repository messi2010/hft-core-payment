package com.payments.tbjava.lsm;

import com.payments.tbjava.domain.UInt128;
import com.payments.tbjava.util.Bytes;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BloomFilterTest {

    @Test
    void neverProducesFalseNegatives() {
        BloomFilter bf = BloomFilter.create(10_000, 0.01);
        for (long k = 0; k < 10_000; k++) bf.add(UInt128.of(k));
        for (long k = 0; k < 10_000; k++) {
            assertTrue(bf.mightContain(UInt128.of(k)), "false negative at " + k);
        }
    }

    @Test
    void falsePositiveRateIsRoughlyAsConfigured() {
        int n = 50_000;
        BloomFilter bf = BloomFilter.create(n, 0.01);
        for (long k = 0; k < n; k++) bf.add(UInt128.of(k));

        int falsePositives = 0;
        int probes = 100_000;
        Random rnd = new Random(7);
        for (int i = 0; i < probes; i++) {
            long candidate = n + 1 + Math.floorMod(rnd.nextLong(), 10_000_000L);
            if (bf.mightContain(UInt128.of(candidate))) falsePositives++;
        }
        double rate = (double) falsePositives / probes;
        // Allow generous headroom; we only care that it's in the right ballpark.
        assertTrue(rate < 0.05, "false positive rate too high: " + rate);
    }

    @Test
    void survivesSerializationRoundTrip() {
        BloomFilter bf = BloomFilter.create(1000, 0.01);
        for (long k = 0; k < 1000; k++) bf.add(UInt128.of(k * 7));

        ByteBuffer buf = Bytes.allocate(bf.serializedSize());
        bf.writeTo(buf);
        buf.flip();
        BloomFilter restored = BloomFilter.readFrom(buf);

        for (long k = 0; k < 1000; k++) {
            assertTrue(restored.mightContain(UInt128.of(k * 7)), "lost membership for " + (k * 7));
        }
        assertEquals(bf.serializedSize(), Bytes.allocate(bf.serializedSize()).capacity());
    }
}
