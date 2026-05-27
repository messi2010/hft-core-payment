package com.payments.tbjava.domain;

public final class TransferFlags {
    private TransferFlags() {}

    /** Link this transfer to the next one in the batch (atomic chain). */
    public static final int LINKED                  = 1 << 0;
    /** This is a pending (two-phase) transfer that holds funds. */
    public static final int PENDING                 = 1 << 1;
    /** This transfer settles (commits) an existing pending transfer. */
    public static final int POST_PENDING_TRANSFER   = 1 << 2;
    /** This transfer rolls back an existing pending transfer. */
    public static final int VOID_PENDING_TRANSFER   = 1 << 3;
    /** Auto-derive amount from a pending transfer (post for full pending amount). */
    public static final int BALANCING_DEBIT         = 1 << 4;
    public static final int BALANCING_CREDIT        = 1 << 5;
    /** Allow this transfer to close the debit account (sets CLOSED flag on it). */
    public static final int CLOSING_DEBIT           = 1 << 6;
    public static final int CLOSING_CREDIT          = 1 << 7;
    /** Mark transfer for inclusion in CDC stream. */
    public static final int HISTORY                 = 1 << 8;
}
