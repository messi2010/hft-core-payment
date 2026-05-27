package com.payments.ledger.lsm;

import com.payments.ledger.domain.UInt128;

/**
 * A single key/value record flowing through the LSM tree.
 *
 * <p>A tombstone is a deletion marker: it must be carried through flushes and
 * compaction so it can shadow an older value for the same key living in a
 * lower SSTable. It is only safe to physically drop a tombstone during a full
 * compaction, when no older table that could still hold the key remains.
 */
record Entry(UInt128 key, byte[] value, boolean tombstone) {

    static Entry put(UInt128 key, byte[] value) {
        return new Entry(key, value, false);
    }

    static Entry tombstone(UInt128 key) {
        return new Entry(key, null, true);
    }
}
