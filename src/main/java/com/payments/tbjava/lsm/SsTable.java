package com.payments.tbjava.lsm;

import com.payments.tbjava.domain.UInt128;
import com.payments.tbjava.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.zip.CRC32C;

/**
 * Immutable, sorted, on-disk table — the persistent unit of an LSM tree.
 *
 * <p>Once written, an SSTable is never modified; updates and deletes are
 * expressed by newer tables (or the memtable) shadowing it. Compaction
 * eventually merges and discards superseded tables.
 *
 * <p>Keys are {@link UInt128} (16 bytes each, stored hi then lo). File layout
 * (little-endian), built so a point lookup costs at most one Bloom probe plus
 * one seek:
 * <pre>
 *   [DATA]   sorted entries: key(16) | flags(1) | valueLen(4) | value(valueLen)
 *            flags bit0 = tombstone (valueLen is 0 for a tombstone)
 *   [INDEX]  entryCount * { key(16) | offset(8) }   -- offset into DATA
 *   [BLOOM]  serialized {@link BloomFilter}
 *   [FOOTER] indexOffset(8) | bloomOffset(8) | entryCount(4) | fileCrc(4) | magic(8)
 * </pre>
 * {@code fileCrc} is CRC32C over every byte before the footer; it is verified
 * once on {@link #open} so corruption is caught before the table serves reads.
 */
final class SsTable implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SsTable.class);

    private static final long MAGIC = 0x54424C534D5F5353L; // "TBLSM_SS"
    static final int FOOTER_SIZE = 32;
    private static final int ENTRY_HEADER = 21;  // key(16) + flags(1) + valueLen(4)
    private static final int INDEX_ENTRY = 24;   // key(16) + offset(8)
    private static final int TOMBSTONE_FLAG = 0x1;
    private static final double BLOOM_FALSE_POSITIVE_RATE = 0.01;

    private final long seq;
    private final Path path;
    private final FileChannel channel;
    private final long[] keysHi;        // sorted ascending (with keysLo)
    private final long[] keysLo;
    private final long[] indexOffsets;  // parallel to keys
    private final BloomFilter bloom;

    private SsTable(long seq, Path path, FileChannel channel,
                    long[] keysHi, long[] keysLo, long[] indexOffsets, BloomFilter bloom) {
        this.seq = seq;
        this.path = path;
        this.channel = channel;
        this.keysHi = keysHi;
        this.keysLo = keysLo;
        this.indexOffsets = indexOffsets;
        this.bloom = bloom;
    }

    long seq() { return seq; }
    Path path() { return path; }
    int entryCount() { return keysHi.length; }

    // ----------------------------------------------------------------------
    // Write
    // ----------------------------------------------------------------------

    /**
     * Write {@code entries} (which MUST already be sorted ascending by key, with
     * at most one entry per key) into a new SSTable file and fsync it.
     *
     * @param expectedKeys upper bound on distinct keys, used to size the Bloom filter
     */
    static void write(Path file, List<Entry> entries, int expectedKeys) throws IOException {
        BloomFilter bloom = BloomFilter.create(Math.max(1, expectedKeys), BLOOM_FALSE_POSITIVE_RATE);
        long[] hi = new long[entries.size()];
        long[] lo = new long[entries.size()];
        long[] offsets = new long[entries.size()];

        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            CRC32C crc = new CRC32C();
            long offset = 0;
            UInt128 prevKey = null;

            // --- DATA ---
            for (int i = 0; i < entries.size(); i++) {
                Entry e = entries.get(i);
                if (prevKey != null && e.key().compareTo(prevKey) <= 0) {
                    throw new IllegalArgumentException("SSTable entries must be strictly ascending by key");
                }
                prevKey = e.key();

                byte[] value = e.tombstone() ? new byte[0] : e.value();
                ByteBuffer rec = Bytes.allocate(ENTRY_HEADER + value.length);
                rec.putLong(e.key().hi());
                rec.putLong(e.key().lo());
                rec.put((byte) (e.tombstone() ? TOMBSTONE_FLAG : 0));
                rec.putInt(value.length);
                rec.put(value);
                rec.flip();
                crc.update(rec.duplicate());
                writeFully(ch, rec);

                hi[i] = e.key().hi();
                lo[i] = e.key().lo();
                offsets[i] = offset;
                offset += ENTRY_HEADER + value.length;
                bloom.add(e.key());
            }
            long indexOffset = offset;

            // --- INDEX ---
            ByteBuffer idx = Bytes.allocate(entries.size() * INDEX_ENTRY);
            for (int i = 0; i < entries.size(); i++) {
                idx.putLong(hi[i]);
                idx.putLong(lo[i]);
                idx.putLong(offsets[i]);
            }
            idx.flip();
            crc.update(idx.duplicate());
            writeFully(ch, idx);
            long bloomOffset = indexOffset + (long) entries.size() * INDEX_ENTRY;

            // --- BLOOM ---
            ByteBuffer bloomBuf = Bytes.allocate(bloom.serializedSize());
            bloom.writeTo(bloomBuf);
            bloomBuf.flip();
            crc.update(bloomBuf.duplicate());
            writeFully(ch, bloomBuf);

            // --- FOOTER ---
            ByteBuffer footer = Bytes.allocate(FOOTER_SIZE);
            footer.putLong(indexOffset);
            footer.putLong(bloomOffset);
            footer.putInt(entries.size());
            footer.putInt((int) crc.getValue());
            footer.putLong(MAGIC);
            footer.flip();
            writeFully(ch, footer);

            ch.force(true);
        }
        // Atomic publish: the table is only visible under its real name once fully written.
        Files.move(tmp, file, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    }

    // ----------------------------------------------------------------------
    // Open / read
    // ----------------------------------------------------------------------

    static SsTable open(long seq, Path file) throws IOException {
        FileChannel ch = FileChannel.open(file, StandardOpenOption.READ);
        try {
            long size = ch.size();
            if (size < FOOTER_SIZE) {
                throw new IOException("SSTable too small to contain a footer: " + file);
            }
            ByteBuffer footer = Bytes.allocate(FOOTER_SIZE);
            readFully(ch, footer, size - FOOTER_SIZE);
            footer.flip();
            long indexOffset = footer.getLong();
            long bloomOffset = footer.getLong();
            int entryCount = footer.getInt();
            int expectedCrc = footer.getInt();
            long magic = footer.getLong();
            if (magic != MAGIC) {
                throw new IOException("Bad SSTable magic in " + file);
            }

            long footerStart = size - FOOTER_SIZE;
            verifyCrc(ch, footerStart, expectedCrc, file);

            // Load index fully into memory: exact lookups via binary search.
            ByteBuffer idx = Bytes.allocate((int) (bloomOffset - indexOffset));
            readFully(ch, idx, indexOffset);
            idx.flip();
            long[] hi = new long[entryCount];
            long[] lo = new long[entryCount];
            long[] offsets = new long[entryCount];
            for (int i = 0; i < entryCount; i++) {
                hi[i] = idx.getLong();
                lo[i] = idx.getLong();
                offsets[i] = idx.getLong();
            }

            ByteBuffer bloomBuf = Bytes.allocate((int) (footerStart - bloomOffset));
            readFully(ch, bloomBuf, bloomOffset);
            bloomBuf.flip();
            BloomFilter bloom = BloomFilter.readFrom(bloomBuf);

            SsTable table = new SsTable(seq, file, ch, hi, lo, offsets, bloom);
            ch = null; // ownership transferred; don't close in finally
            return table;
        } finally {
            if (ch != null) ch.close();
        }
    }

    /** Bloom pre-check used by the read path to skip tables that can't hold the key. */
    boolean mightContain(UInt128 key) {
        int n = keysHi.length;
        if (n == 0) return false;
        if (cmp(key, keysHi[0], keysLo[0]) < 0 || cmp(key, keysHi[n - 1], keysLo[n - 1]) > 0) return false;
        return bloom.mightContain(key);
    }

    /** Returns the entry for {@code key} (possibly a tombstone), or {@code null} if absent. */
    Entry get(UInt128 key) throws IOException {
        int idx = binarySearch(key);
        if (idx < 0) return null;
        return readEntryAt(indexOffsets[idx]);
    }

    private Entry readEntryAt(long offset) throws IOException {
        ByteBuffer head = Bytes.allocate(ENTRY_HEADER);
        readFully(channel, head, offset);
        head.flip();
        UInt128 key = new UInt128(head.getLong(), head.getLong());
        byte flags = head.get();
        int valueLen = head.getInt();
        if ((flags & TOMBSTONE_FLAG) != 0) {
            return Entry.tombstone(key);
        }
        ByteBuffer val = Bytes.allocate(valueLen);
        readFully(channel, val, offset + ENTRY_HEADER);
        val.flip();
        byte[] value = new byte[valueLen];
        val.get(value);
        return Entry.put(key, value);
    }

    /**
     * Sequential scan over all entries in key order. Used by compaction's
     * merge. Reads the whole data block streaming, so it does not rely on the
     * in-memory index beyond knowing where DATA ends.
     */
    Iterator<Entry> iterator() {
        final long limit = indexOffsetValue();
        return new Iterator<>() {
            private long pos = 0;
            @Override public boolean hasNext() { return pos < limit; }
            @Override public Entry next() {
                if (pos >= limit) throw new NoSuchElementException();
                try {
                    Entry e = readEntryAt(pos);
                    int valueLen = e.tombstone() ? 0 : e.value().length;
                    pos += ENTRY_HEADER + valueLen;
                    return e;
                } catch (IOException ex) {
                    throw new RuntimeException("SSTable scan failed: " + path, ex);
                }
            }
        };
    }

    private long indexOffsetValue() {
        // The data block ends where the index begins. We recompute it from the
        // last entry rather than storing it, keeping the in-memory footprint small.
        if (keysHi.length == 0) return 0;
        try {
            long lastStart = indexOffsets[keysHi.length - 1];
            Entry last = readEntryAt(lastStart);
            int valueLen = last.tombstone() ? 0 : last.value().length;
            return lastStart + ENTRY_HEADER + valueLen;
        } catch (IOException e) {
            throw new RuntimeException("Failed to compute SSTable data extent: " + path, e);
        }
    }

    /** Binary search over the sorted index; returns slot or -1. */
    private int binarySearch(UInt128 key) {
        int lo = 0, hi = keysHi.length - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int c = cmp(key, keysHi[mid], keysLo[mid]);
            if (c > 0) lo = mid + 1;
            else if (c < 0) hi = mid - 1;
            else return mid;
        }
        return -1;
    }

    /** Compare {@code key} against an index slot's (hi,lo); unsigned, hi first. */
    private static int cmp(UInt128 key, long hi, long lo) {
        int c = Long.compareUnsigned(key.hi(), hi);
        return c != 0 ? c : Long.compareUnsigned(key.lo(), lo);
    }

    void deleteFile() throws IOException {
        close();
        Files.deleteIfExists(path);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    private static void verifyCrc(FileChannel ch, long lengthToCheck, int expectedCrc, Path file) throws IOException {
        CRC32C crc = new CRC32C();
        ByteBuffer buf = Bytes.allocate(1 << 16);
        long pos = 0;
        while (pos < lengthToCheck) {
            buf.clear();
            int want = (int) Math.min(buf.capacity(), lengthToCheck - pos);
            buf.limit(want);
            int n = ch.read(buf, pos);
            if (n < 0) break;
            buf.flip();
            crc.update(buf);
            pos += n;
        }
        int actual = (int) crc.getValue();
        if (actual != expectedCrc) {
            throw new IOException("SSTable CRC mismatch in " + file
                    + " (expected=" + Integer.toUnsignedString(expectedCrc)
                    + " actual=" + Integer.toUnsignedString(actual) + ")");
        }
    }

    private static void writeFully(FileChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) ch.write(buf);
    }

    private static void readFully(FileChannel ch, ByteBuffer buf, long position) throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            int n = ch.read(buf, pos);
            if (n < 0) throw new IOException("Unexpected EOF reading SSTable at " + pos);
            pos += n;
        }
    }
}
