package com.payments.tbjava.storage;

import com.payments.tbjava.domain.Transfer;
import com.payments.tbjava.domain.UInt128;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Pre-allocated transfer store. Same primitive-array layout as AccountStore.
 *
 * <p>Transfers are append-only — once inserted, never modified. Pending
 * transfers' status (posted/voided) is tracked separately in
 * {@link PendingTransferIndex}.
 */
public final class TransferStore {

    private final int capacity;
    private final Object2IntOpenHashMap<UInt128> idToSlot;
    private int nextSlot = 0;

    private final UInt128[] ids;
    private final UInt128[] debitAccountIds;
    private final UInt128[] creditAccountIds;
    private final long[] amounts;
    private final UInt128[] pendingIds;
    private final long[] userData64;
    private final int[]  userData32;
    private final int[]  timeoutSeconds;
    private final long[] ledgerCodeFlags;   // ledger(32) | code(16) | flags(16)
    private final long[] timestamps;

    public TransferStore(int capacity) {
        this.capacity = capacity;
        this.idToSlot = new Object2IntOpenHashMap<>(capacity);
        this.idToSlot.defaultReturnValue(-1);
        this.ids = new UInt128[capacity];
        this.debitAccountIds = new UInt128[capacity];
        this.creditAccountIds = new UInt128[capacity];
        this.amounts = new long[capacity];
        this.pendingIds = new UInt128[capacity];
        this.userData64 = new long[capacity];
        this.userData32 = new int[capacity];
        this.timeoutSeconds = new int[capacity];
        this.ledgerCodeFlags = new long[capacity];
        this.timestamps = new long[capacity];
    }

    public int size() { return nextSlot; }

    public boolean exists(UInt128 id) { return idToSlot.getInt(id) != -1; }
    public int slotOf(UInt128 id) { return idToSlot.getInt(id); }

    /**
     * Roll back to a previously recorded size, discarding every transfer inserted
     * since. Used when a linked chain fails: the transfers it inserted must be
     * fully removed (id mapping included) so the IDs are free to be retried and
     * so a later snapshot/replay never observes a transfer that was rolled back.
     */
    public void truncateTo(int mark) {
        if (mark < 0 || mark > nextSlot) {
            throw new IllegalArgumentException("invalid truncate mark " + mark + " (size=" + nextSlot + ")");
        }
        for (int slot = mark; slot < nextSlot; slot++) {
            idToSlot.removeInt(ids[slot]);
        }
        nextSlot = mark;
    }

    public int insert(Transfer t) {
        if (nextSlot >= capacity) {
            throw new IllegalStateException("TransferStore full: capacity=" + capacity);
        }
        int slot = nextSlot++;
        ids[slot] = t.id();
        debitAccountIds[slot] = t.debitAccountId();
        creditAccountIds[slot] = t.creditAccountId();
        amounts[slot] = t.amount();
        pendingIds[slot] = t.pendingId();
        userData64[slot] = t.userData64();
        userData32[slot] = t.userData32();
        timeoutSeconds[slot] = t.timeoutSeconds();
        ledgerCodeFlags[slot] = ((long) t.ledger() << 32)
                | ((Short.toUnsignedLong(t.code()) & 0xFFFFL) << 16)
                | (Short.toUnsignedLong(t.flags()) & 0xFFFFL);
        timestamps[slot] = t.timestamp();
        idToSlot.put(t.id(), slot);
        return slot;
    }

    public Transfer load(UInt128 id) {
        int slot = idToSlot.getInt(id);
        if (slot == -1) throw new NoSuchElementException("Transfer not found: " + id);
        return loadFromSlot(slot);
    }

    public Transfer loadFromSlot(int slot) {
        long lcf = ledgerCodeFlags[slot];
        return new Transfer(
                ids[slot],
                debitAccountIds[slot],
                creditAccountIds[slot],
                amounts[slot],
                pendingIds[slot],
                userData64[slot],
                userData32[slot],
                timeoutSeconds[slot],
                (int) (lcf >>> 32),
                (short) ((lcf >>> 16) & 0xFFFF),
                (short) (lcf & 0xFFFF),
                timestamps[slot]);
    }

    /** Iterate all transfers in insertion order (for snapshot persistence). */
    public Iterator<Transfer> iterator() {
        return new Iterator<>() {
            private int i = 0;
            @Override public boolean hasNext() { return i < nextSlot; }
            @Override public Transfer next() { return loadFromSlot(i++); }
        };
    }
}
