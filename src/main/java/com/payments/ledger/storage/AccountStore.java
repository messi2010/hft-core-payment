package com.payments.ledger.storage;

import com.payments.ledger.domain.Account;
import com.payments.ledger.domain.AccountFlags;
import com.payments.ledger.domain.AccountSnapshot;
import com.payments.ledger.domain.UInt128;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.Iterator;

/**
 * Pre-allocated, fixed-capacity account store.
 *
 * <p>Implementation: an {@code Object2IntOpenHashMap<UInt128>} maps
 * {@code accountId -> slot index}, plus parallel primitive arrays for each field.
 * This layout avoids materializing a per-account record object (only the
 * {@link UInt128} id is held); the hot balance counters stay in primitive
 * {@code long[]}, all allocated upfront -- no GC pressure during steady state.
 *
 * <p>This class is NOT thread-safe. The single writer thread owns it.
 * Read-only callers must go through the engine, which serializes reads onto
 * the same thread.
 */
public final class AccountStore {

    private final int capacity;
    private final Object2IntOpenHashMap<UInt128> idToSlot;
    private int nextSlot = 0;

    // Parallel arrays -- index by slot.
    private final long[] ledgerCodeFlags;   // packed: ledger(32) | code(16) | flags(16)
    private final long[] userData64;
    private final int[]  userData32;
    private final long[] debitsPending;
    private final long[] debitsPosted;
    private final long[] creditsPending;
    private final long[] creditsPosted;
    private final long[] timestamps;
    private final UInt128[] ids;            // reverse map slot -> id, for iteration

    public AccountStore(int capacity) {
        this.capacity = capacity;
        this.idToSlot = new Object2IntOpenHashMap<>(capacity);
        this.idToSlot.defaultReturnValue(-1);
        this.ledgerCodeFlags = new long[capacity];
        this.userData64 = new long[capacity];
        this.userData32 = new int[capacity];
        this.debitsPending = new long[capacity];
        this.debitsPosted = new long[capacity];
        this.creditsPending = new long[capacity];
        this.creditsPosted = new long[capacity];
        this.timestamps = new long[capacity];
        this.ids = new UInt128[capacity];
    }

    public int size() { return nextSlot; }
    public int capacity() { return capacity; }

    public boolean exists(UInt128 id) {
        return idToSlot.getInt(id) != -1;
    }

    /** Returns slot index or -1 if not found. */
    public int slotOf(UInt128 id) {
        return idToSlot.getInt(id);
    }

    /** Insert. Caller must check {@link #exists} first. Returns slot index. */
    public int insert(UInt128 id, int ledger, short code, short flags,
                      long userData64, int userData32, long timestamp) {
        if (nextSlot >= capacity) {
            throw new IllegalStateException("AccountStore full: capacity=" + capacity);
        }
        int slot = nextSlot++;
        ids[slot] = id;
        ledgerCodeFlags[slot] = packLedgerCodeFlags(ledger, code, flags);
        this.userData64[slot] = userData64;
        this.userData32[slot] = userData32;
        timestamps[slot] = timestamp;
        // balance counters start at zero (Java default)
        idToSlot.put(id, slot);
        return slot;
    }

    // --- field accessors by slot (fast path for the state machine) ---

    public UInt128 id(int slot) { return ids[slot]; }
    public int ledger(int slot) { return (int) (ledgerCodeFlags[slot] >>> 32); }
    public short code(int slot) { return (short) ((ledgerCodeFlags[slot] >>> 16) & 0xFFFF); }
    public short flags(int slot) { return (short) (ledgerCodeFlags[slot] & 0xFFFF); }
    public long userData64(int slot) { return userData64[slot]; }
    public int userData32(int slot) { return userData32[slot]; }
    public long timestamp(int slot) { return timestamps[slot]; }

    public long debitsPending(int slot) { return debitsPending[slot]; }
    public long debitsPosted(int slot) { return debitsPosted[slot]; }
    public long creditsPending(int slot) { return creditsPending[slot]; }
    public long creditsPosted(int slot) { return creditsPosted[slot]; }

    public void addDebitsPending(int slot, long delta) { debitsPending[slot] += delta; }
    public void addDebitsPosted(int slot, long delta)  { debitsPosted[slot] += delta; }
    public void addCreditsPending(int slot, long delta) { creditsPending[slot] += delta; }
    public void addCreditsPosted(int slot, long delta)  { creditsPosted[slot] += delta; }

    public boolean hasFlag(int slot, int flag) { return (flags(slot) & flag) != 0; }

    public boolean isClosed(int slot) { return hasFlag(slot, AccountFlags.CLOSED); }

    /** Set CLOSED flag (used by closing transfers). */
    public void close(int slot) {
        ledgerCodeFlags[slot] |= AccountFlags.CLOSED;
    }

    public boolean wouldExceedDebitLimit(int slot, long extra) {
        if (!hasFlag(slot, AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS)) return false;
        long posted  = debitsPosted[slot];
        long pending = debitsPending[slot];
        // Check (posted + pending) for unsigned overflow before adding extra.
        long sum1 = posted + pending;
        if (Long.compareUnsigned(sum1, posted) < 0) return true;   // sum1 wrapped → definitely exceeds
        long total = sum1 + extra;
        if (Long.compareUnsigned(total, sum1) < 0) return true;    // total wrapped → definitely exceeds
        return Long.compareUnsigned(total, creditsPosted[slot]) > 0;
    }

    public boolean wouldExceedCreditLimit(int slot, long extra) {
        if (!hasFlag(slot, AccountFlags.CREDITS_MUST_NOT_EXCEED_DEBITS)) return false;
        long posted  = creditsPosted[slot];
        long pending = creditsPending[slot];
        long sum1 = posted + pending;
        if (Long.compareUnsigned(sum1, posted) < 0) return true;
        long total = sum1 + extra;
        if (Long.compareUnsigned(total, sum1) < 0) return true;
        return Long.compareUnsigned(total, debitsPosted[slot]) > 0;
    }

    /** Immutable snapshot for external consumers. */
    public AccountSnapshot snapshot(int slot) {
        return new AccountSnapshot(
                ids[slot], ledger(slot), code(slot), flags(slot),
                userData64[slot], userData32[slot],
                debitsPending[slot], debitsPosted[slot],
                creditsPending[slot], creditsPosted[slot],
                timestamps[slot]);
    }

    /** For snapshot persistence. Iterates in insertion order. */
    public Iterator<AccountSnapshot> iterator() {
        return new Iterator<>() {
            private int i = 0;
            @Override public boolean hasNext() { return i < nextSlot; }
            @Override public AccountSnapshot next() { return snapshot(i++); }
        };
    }

    /** Repopulate from a snapshot during recovery. */
    public void restoreEntry(UInt128 id, int ledger, short code, short flags,
                             long userData64, int userData32, long timestamp,
                             long debitsPending, long debitsPosted,
                             long creditsPending, long creditsPosted) {
        int slot = insert(id, ledger, code, flags, userData64, userData32, timestamp);
        this.debitsPending[slot] = debitsPending;
        this.debitsPosted[slot] = debitsPosted;
        this.creditsPending[slot] = creditsPending;
        this.creditsPosted[slot] = creditsPosted;
    }

    private static long packLedgerCodeFlags(int ledger, short code, short flags) {
        return ((long) ledger << 32) | ((Short.toUnsignedLong(code) & 0xFFFFL) << 16)
                | (Short.toUnsignedLong(flags) & 0xFFFFL);
    }
}
