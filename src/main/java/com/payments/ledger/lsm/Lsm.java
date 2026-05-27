package com.payments.ledger.lsm;

import com.payments.ledger.domain.UInt128;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A single-tree log-structured merge store: {@code long key -> byte[] value}.
 *
 * <p>This is the storage primitive the README lists as "phase 2" — the path off
 * the all-in-RAM ceiling. The hot set of recent writes lives in a {@link Memtable}
 * (guarded by an {@link LsmWal}); when it fills it is flushed to an immutable,
 * sorted {@link SsTable} on disk. Reads consult the memtable, then each SSTable
 * newest-to-oldest, short-circuiting on the first hit (a tombstone reads back as
 * absent). {@link Manifest} records the live set of tables so flush and
 * compaction are crash-atomic.
 *
 * <p>Write amplification is bounded by size-tiered compaction: once too many
 * tables accumulate they are k-way merged into one, keeping the newest value per
 * key and physically dropping tombstones (safe in a full merge, since no older
 * table survives to resurrect the key).
 *
 * <p>Concurrency model matches the rest of the engine: a single owner thread
 * (the Disruptor writer) serializes all access. This class is NOT thread-safe.
 */
public final class Lsm implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Lsm.class);
    private static final Pattern SST_NAME = Pattern.compile("sst-(\\d{16})\\.sst");
    private static final String WAL_NAME = "lsm.wal";
    private static final String MANIFEST_NAME = "MANIFEST";

    /** Tuning knobs. Defaults aim at correctness-under-test, not production sizing. */
    public record Config(long memtableFlushBytes, int maxTablesBeforeCompaction) {
        public static Config defaults() {
            return new Config(4L * 1024 * 1024, 4);
        }
    }

    private final Path dir;
    private final Config config;
    private final Manifest manifest;

    private Memtable memtable = new Memtable();
    private LsmWal wal;
    /** Live tables, newest first (index 0 wins on key conflict). */
    private final List<SsTable> tables = new ArrayList<>();
    private long nextSeq;

    private Lsm(Path dir, Config config) {
        this.dir = dir;
        this.config = config;
        this.manifest = new Manifest(dir.resolve(MANIFEST_NAME));
    }

    // ----------------------------------------------------------------------
    // Lifecycle
    // ----------------------------------------------------------------------

    public static Lsm open(Path dir, Config config) throws IOException {
        Files.createDirectories(dir);
        Lsm lsm = new Lsm(dir, config);
        lsm.recover();
        return lsm;
    }

    private void recover() throws IOException {
        long maxFileSeq = -1;
        try (Stream<Path> files = Files.list(dir)) {
            for (Path p : (Iterable<Path>) files::iterator) {
                Matcher m = SST_NAME.matcher(p.getFileName().toString());
                if (m.matches()) {
                    maxFileSeq = Math.max(maxFileSeq, Long.parseLong(m.group(1)));
                }
            }
        }
        nextSeq = maxFileSeq + 1;

        // Only tables named by the manifest are live; everything else is an
        // orphan from a flush/compaction that crashed before committing.
        if (manifest.exists()) {
            for (long seq : manifest.read()) {
                Path file = tablePath(seq);
                if (Files.exists(file)) {
                    tables.add(SsTable.open(seq, file));
                } else {
                    log.warn("Manifest references missing SSTable seq={} -- skipping", seq);
                }
            }
        }

        // Replay the WAL into a fresh memtable. Entries already captured by a
        // flushed SSTable are simply re-shadowed in the memtable (idempotent).
        wal = new LsmWal(dir.resolve(WAL_NAME));
        wal.replay(memtable::put);

        log.info("LSM opened at {}: {} live tables, {} memtable entries, nextSeq={}",
                dir, tables.size(), memtable.size(), nextSeq);
    }

    // ----------------------------------------------------------------------
    // Read / write
    // ----------------------------------------------------------------------

    public void put(UInt128 key, byte[] value) throws IOException {
        if (value == null) throw new IllegalArgumentException("value must not be null; use delete()");
        write(Entry.put(key, value));
    }

    public void delete(UInt128 key) throws IOException {
        write(Entry.tombstone(key));
    }

    private void write(Entry entry) throws IOException {
        wal.append(entry);
        memtable.put(entry);
        if (memtable.approxBytes() >= config.memtableFlushBytes()) {
            flush();
        }
    }

    /** Returns the current value for {@code key}, or {@code null} if absent or deleted. */
    public byte[] get(UInt128 key) throws IOException {
        Entry m = memtable.get(key);
        if (m != null) {
            return m.tombstone() ? null : m.value();
        }
        for (SsTable table : tables) { // newest first
            if (!table.mightContain(key)) continue;
            Entry e = table.get(key);
            if (e != null) {
                return e.tombstone() ? null : e.value();
            }
        }
        return null;
    }

    /** Durably persist buffered writes: fsync the WAL (memtable stays in RAM). */
    public void sync() throws IOException {
        wal.flush();
    }

    // ----------------------------------------------------------------------
    // Flush + compaction
    // ----------------------------------------------------------------------

    /** Force the memtable out to a new SSTable, even if it is below the threshold. */
    public void flush() throws IOException {
        if (memtable.isEmpty()) return;

        long seq = nextSeq++;
        Path file = tablePath(seq);
        List<Entry> entries = memtable.sortedEntries();
        SsTable.write(file, entries, entries.size());
        SsTable table = SsTable.open(seq, file);

        // Publish: the new table becomes newest, then the manifest records it,
        // and only then is the WAL safe to discard.
        tables.add(0, table);
        manifest.write(liveSeqs());
        wal.rotate();
        memtable = new Memtable();

        log.info("LSM flush: wrote SSTable seq={} ({} entries)", seq, entries.size());
        maybeCompact();
    }

    private void maybeCompact() throws IOException {
        if (tables.size() <= config.maxTablesBeforeCompaction()) return;
        compact();
    }

    /**
     * Full size-tiered compaction: k-way merge every live table into one,
     * keeping the newest value per key and dropping tombstones.
     */
    void compact() throws IOException {
        if (tables.size() < 2) return;
        List<SsTable> inputs = new ArrayList<>(tables); // newest-first
        long seq = nextSeq++;
        Path file = tablePath(seq);

        List<Entry> merged = new ArrayList<>();
        List<Iterator<Entry>> sources = new ArrayList<>(inputs.size());
        for (SsTable t : inputs) sources.add(t.iterator());
        mergeSources(sources, e -> { if (!e.tombstone()) merged.add(e); });
        SsTable.write(file, merged, merged.size());
        SsTable newTable = SsTable.open(seq, file);

        // Swap in the merged table, commit the manifest, then drop old files.
        tables.clear();
        tables.add(newTable);
        manifest.write(liveSeqs());
        for (SsTable old : inputs) {
            old.deleteFile();
        }
        log.info("LSM compaction: merged {} tables into seq={} ({} live entries)",
                inputs.size(), seq, merged.size());
    }

    /** One position within an input table during the merge. */
    private static final class Cursor {
        Entry entry;
        final int precedence; // lower = newer table
        final Iterator<Entry> it;

        Cursor(Entry entry, int precedence, Iterator<Entry> it) {
            this.entry = entry;
            this.precedence = precedence;
            this.it = it;
        }

        /** Re-arm with the next entry; returns false when the table is exhausted. */
        boolean advance() {
            if (it.hasNext()) {
                entry = it.next();
                return true;
            }
            return false;
        }
    }

    @FunctionalInterface
    private interface EntrySink {
        void accept(Entry e) throws IOException;
    }

    /**
     * K-way merge of pre-sorted {@code sources} ordered newest-first (index 0 =
     * newest). Emits exactly one winning entry per key -- the newest -- to
     * {@code sink}, in ascending key order. Tombstones are passed through; the
     * caller decides whether to keep or drop them.
     */
    private void mergeSources(List<Iterator<Entry>> sources, EntrySink sink) throws IOException {
        Comparator<Cursor> cmp = (a, b) -> {
            int k = a.entry.key().compareTo(b.entry.key());
            return k != 0 ? k : Integer.compare(a.precedence, b.precedence);
        };
        PriorityQueue<Cursor> pq = new PriorityQueue<>(cmp);
        for (int i = 0; i < sources.size(); i++) {
            Iterator<Entry> it = sources.get(i);
            if (it.hasNext()) pq.add(new Cursor(it.next(), i, it));
        }
        while (!pq.isEmpty()) {
            Cursor head = pq.poll();
            UInt128 key = head.entry.key();
            Entry winner = head.entry; // newest, because precedence broke the tie
            if (head.advance()) pq.add(head);
            while (!pq.isEmpty() && pq.peek().entry.key().equals(key)) {
                Cursor dup = pq.poll();
                if (dup.advance()) pq.add(dup);
            }
            sink.accept(winner);
        }
    }

    /**
     * Visit every live key/value in ascending key order (newest value per key,
     * deleted keys skipped). Used by recovery to load a full checkpoint back
     * into memory.
     */
    public void scan(EntryConsumer consumer) throws IOException {
        List<Iterator<Entry>> sources = new ArrayList<>(1 + tables.size());
        sources.add(memtable.sortedEntries().iterator()); // newest
        for (SsTable t : tables) sources.add(t.iterator());
        mergeSources(sources, e -> {
            if (!e.tombstone()) consumer.accept(e.key(), e.value());
        });
    }

    @FunctionalInterface
    public interface EntryConsumer {
        void accept(UInt128 key, byte[] value) throws IOException;
    }

    // ----------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------

    private List<Long> liveSeqs() {
        List<Long> seqs = new ArrayList<>(tables.size());
        for (SsTable t : tables) seqs.add(t.seq());
        return seqs;
    }

    private Path tablePath(long seq) {
        return dir.resolve(String.format("sst-%016d.sst", seq));
    }

    int liveTableCount() {
        return tables.size();
    }

    @Override
    public void close() throws IOException {
        wal.close();
        for (SsTable t : tables) t.close();
    }
}
