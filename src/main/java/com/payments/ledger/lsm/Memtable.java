package com.payments.ledger.lsm;

import com.payments.ledger.domain.UInt128;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * The in-memory write buffer at the top of the LSM tree.
 *
 * <p>Writes land here first (after the WAL append) and are served from here
 * before any on-disk table. It is kept sorted by key (a {@link TreeMap} ordered
 * by {@link UInt128}'s unsigned comparator) so a flush can stream entries
 * straight into an {@link SsTable} without an extra sort pass.
 *
 * <p>Deletes are stored as tombstone entries rather than removals, because the
 * key may still exist in a lower SSTable; the tombstone must win on read and be
 * propagated on flush. NOT thread-safe: the owning writer thread serializes all
 * access, mirroring the rest of this engine's single-writer model.
 */
final class Memtable {

    private final TreeMap<UInt128, Entry> map = new TreeMap<>();
    private long approxBytes;

    /** Rough per-entry overhead (key + object headers + map node) for sizing flushes. */
    private static final long ENTRY_OVERHEAD = 64;

    void put(Entry entry) {
        Entry prev = map.put(entry.key(), entry);
        if (prev != null) approxBytes -= sizeOf(prev);
        approxBytes += sizeOf(entry);
    }

    /** Returns the entry (possibly a tombstone) or {@code null} if this memtable has never seen the key. */
    Entry get(UInt128 key) {
        return map.get(key);
    }

    boolean isEmpty() {
        return map.isEmpty();
    }

    int size() {
        return map.size();
    }

    long approxBytes() {
        return approxBytes;
    }

    /** Entries in ascending key order, ready to stream into an SSTable. */
    List<Entry> sortedEntries() {
        return new ArrayList<>(map.values());
    }

    private static long sizeOf(Entry e) {
        return ENTRY_OVERHEAD + (e.value() == null ? 0 : e.value().length);
    }
}
