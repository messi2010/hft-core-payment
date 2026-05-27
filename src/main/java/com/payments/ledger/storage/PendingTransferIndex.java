package com.payments.ledger.storage;

import com.payments.ledger.domain.UInt128;
import it.unimi.dsi.fastutil.objects.Object2ByteOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import java.util.ArrayList;
import java.util.List;

/**
 * Tracks the status of pending (two-phase) transfers.
 *
 * <p>When a PENDING transfer is created, it's added here with status
 * {@code OPEN}. A subsequent POST_PENDING or VOID_PENDING transition flips
 * status to {@code POSTED} or {@code VOIDED}. Expired transfers are checked
 * lazily by {@link #isExpired} and swept actively by {@link #openExpired}.
 */
public final class PendingTransferIndex {

    public static final byte STATUS_OPEN   = 0;
    public static final byte STATUS_POSTED = 1;
    public static final byte STATUS_VOIDED = 2;

    /** transferId -> status byte */
    private final Object2ByteOpenHashMap<UInt128> status = new Object2ByteOpenHashMap<>();
    /** transferId -> expiry timestamp (nanos since epoch). 0 = no timeout. */
    private final Object2LongOpenHashMap<UInt128> expiry = new Object2LongOpenHashMap<>();

    public PendingTransferIndex() {
        status.defaultReturnValue((byte) -1);
        expiry.defaultReturnValue(0L);
    }

    /** Register a new pending transfer. */
    public void add(UInt128 transferId, long expiryNanos) {
        status.put(transferId, STATUS_OPEN);
        if (expiryNanos > 0) expiry.put(transferId, expiryNanos);
    }

    public boolean exists(UInt128 pendingId) {
        return status.getByte(pendingId) != -1;
    }

    public byte statusOf(UInt128 pendingId) {
        return status.getByte(pendingId);
    }

    public boolean isOpen(UInt128 pendingId) {
        return statusOf(pendingId) == STATUS_OPEN;
    }

    public boolean isExpired(UInt128 pendingId, long nowNanos) {
        long exp = expiry.getLong(pendingId);
        return exp != 0 && nowNanos >= exp;
    }

    public void markPosted(UInt128 pendingId) {
        status.put(pendingId, STATUS_POSTED);
        expiry.removeLong(pendingId);
    }

    public void markVoided(UInt128 pendingId) {
        status.put(pendingId, STATUS_VOIDED);
        expiry.removeLong(pendingId);
    }

    /** Expiry timestamp for a pending transfer, or 0 if none. */
    public long expiryOf(UInt128 pendingId) {
        return expiry.getLong(pendingId);
    }

    /**
     * Ids of pending transfers that are still OPEN and whose timeout has passed
     * as of {@code now}. Order is unspecified (the caller voids each independently,
     * so order does not affect the result).
     */
    public UInt128[] openExpired(long now) {
        List<UInt128> out = new ArrayList<>();
        for (var e : expiry.object2LongEntrySet()) {
            long exp = e.getLongValue();
            if (exp > 0 && now >= exp && status.getByte(e.getKey()) == STATUS_OPEN) {
                out.add(e.getKey());
            }
        }
        return out.toArray(new UInt128[0]);
    }

    /** Remove all trace of a pending transfer (chain rollback of a PENDING create). */
    public void remove(UInt128 pendingId) {
        status.removeByte(pendingId);
        expiry.removeLong(pendingId);
    }

    /**
     * Restore a pending transfer's prior status and expiry (chain rollback of a
     * POST/VOID that had already flipped the status).
     */
    public void restore(UInt128 pendingId, byte prevStatus, long prevExpiry) {
        status.put(pendingId, prevStatus);
        if (prevExpiry > 0) {
            expiry.put(pendingId, prevExpiry);
        } else {
            expiry.removeLong(pendingId);
        }
    }

    public int size() { return status.size(); }
}
