package com.payments.ledger.lsm;

import com.payments.ledger.domain.UInt128;
import com.payments.ledger.util.Bytes;

import java.nio.ByteBuffer;

/**
 * Fixed-size Bloom filter keyed by {@code long}, used per {@link SsTable} to
 * avoid touching disk for keys that are definitely absent.
 *
 * <p>A read in an LSM tree must, in the worst case, check every SSTable from
 * newest to oldest. Without a negative-lookup filter that means one disk seek
 * per table per miss. The Bloom filter answers "definitely not present" with
 * zero false negatives (so it never hides a real key) and a tunable false
 * positive rate, turning most of those seeks into an in-memory bit test.
 *
 * <p>Keys are 64-bit, so we derive two independent hashes with a finalizer mix
 * and combine them via double hashing ({@code h1 + i*h2}) to synthesize the
 * {@code k} probe positions. This is the standard Kirsch-Mitzenmacher trick.
 */
final class BloomFilter {

    private final long[] words;     // bitset, 64 bits per word
    private final int bitCount;
    private final int numHashes;

    private BloomFilter(long[] words, int bitCount, int numHashes) {
        this.words = words;
        this.bitCount = bitCount;
        this.numHashes = numHashes;
    }

    /**
     * Size a filter for {@code expectedKeys} at roughly {@code falsePositiveRate}.
     * Uses the optimal bit count m = -n*ln(p)/(ln2)^2 and hash count k = (m/n)*ln2.
     */
    static BloomFilter create(int expectedKeys, double falsePositiveRate) {
        int n = Math.max(1, expectedKeys);
        double ln2 = Math.log(2);
        int bits = (int) Math.ceil(-(n * Math.log(falsePositiveRate)) / (ln2 * ln2));
        bits = Math.max(64, bits);
        int k = Math.max(1, (int) Math.round((double) bits / n * ln2));
        int wordCount = (bits + 63) >>> 6;
        return new BloomFilter(new long[wordCount], wordCount << 6, k);
    }

    void add(UInt128 key) {
        long h1 = mix(key.hi() * 0xD6E8FEB86659FD93L ^ key.lo());
        long h2 = mix(key.lo() ^ Long.rotateLeft(key.hi(), 32) ^ 0x9E3779B97F4A7C15L);
        for (int i = 0; i < numHashes; i++) {
            int bit = bitIndex(h1, h2, i);
            words[bit >>> 6] |= (1L << (bit & 63));
        }
    }

    boolean mightContain(UInt128 key) {
        long h1 = mix(key.hi() * 0xD6E8FEB86659FD93L ^ key.lo());
        long h2 = mix(key.lo() ^ Long.rotateLeft(key.hi(), 32) ^ 0x9E3779B97F4A7C15L);
        for (int i = 0; i < numHashes; i++) {
            int bit = bitIndex(h1, h2, i);
            if ((words[bit >>> 6] & (1L << (bit & 63))) == 0) return false;
        }
        return true;
    }

    private int bitIndex(long h1, long h2, int i) {
        long combined = h1 + (long) i * h2;
        // Mask to non-negative, then modulo the bit count.
        return (int) Long.remainderUnsigned(combined, bitCount);
    }

    /** SplitMix64-style finalizer: cheap, good avalanche for sequential ids. */
    private static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    // --- serialization: bitCount(4) | numHashes(4) | wordCount(4) | words[] ---

    int serializedSize() {
        return 12 + words.length * Long.BYTES;
    }

    void writeTo(ByteBuffer buf) {
        buf.putInt(bitCount);
        buf.putInt(numHashes);
        buf.putInt(words.length);
        for (long w : words) buf.putLong(w);
    }

    static BloomFilter readFrom(ByteBuffer buf) {
        int bitCount = buf.getInt();
        int numHashes = buf.getInt();
        int wordCount = buf.getInt();
        long[] words = new long[wordCount];
        for (int i = 0; i < wordCount; i++) words[i] = buf.getLong();
        return new BloomFilter(words, bitCount, numHashes);
    }

    static BloomFilter readFrom(byte[] bytes) {
        return readFrom(Bytes.wrap(bytes));
    }
}
