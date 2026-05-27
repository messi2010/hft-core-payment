package com.payments.ledger.domain;

/**
 * Transfer record: a double-entry value movement (u128 ids, u64 amounts).
 *
 * <p>A single transfer moves {@code amount} units of {@code ledger} from
 * {@code debitAccountId} to {@code creditAccountId}. Double-entry is enforced
 * by construction -- there are always exactly two legs.
 *
 * <p>For two-phase semantics, {@code pendingId} is non-zero on POST_PENDING /
 * VOID_PENDING transfers and refers back to the original PENDING transfer.
 *
 * <p>For atomic batches, the {@code LINKED} flag on a transfer says "this
 * transfer is linked to the next one in the batch" -- if any in the chain
 * fails, all roll back.
 */
public record Transfer(
        UInt128 id,
        UInt128 debitAccountId,
        UInt128 creditAccountId,
        long amount,
        UInt128 pendingId,     // UInt128.ZERO if not a post/void of a pending
        long userData64,
        int userData32,
        int timeoutSeconds,    // 0 means no timeout; only meaningful on PENDING
        int ledger,
        short code,
        short flags,
        long timestamp
) {
    public boolean hasFlag(int flag) {
        return (flags & flag) != 0;
    }

    public boolean isPending() { return hasFlag(TransferFlags.PENDING); }
    public boolean isPostPending() { return hasFlag(TransferFlags.POST_PENDING_TRANSFER); }
    public boolean isVoidPending() { return hasFlag(TransferFlags.VOID_PENDING_TRANSFER); }
    public boolean isLinked() { return hasFlag(TransferFlags.LINKED); }
}
