package com.payments.ledger.domain;

/**
 * Immutable point-in-time snapshot of an account, safe to hand out to any
 * thread. Produced by {@link Account#snapshot()}.
 */
public record AccountSnapshot(
        UInt128 id,
        int ledger,
        short code,
        short flags,
        long userData64,
        int userData32,
        long debitsPending,
        long debitsPosted,
        long creditsPending,
        long creditsPosted,
        long timestamp
) {
    /**
     * Net position from the credit side ({@code creditsPosted - debitsPosted}).
     * Natural for credit-normal accounts (liability/income/equity). For
     * debit-normal accounts (asset/expense) use {@link #debitBalance()} instead —
     * a single convention cannot suit both normal sides, so prefer the explicit
     * helpers and let the chart of accounts decide which one applies.
     */
    public long balance() {
        return creditBalance();
    }

    /** Net position for a debit-normal account (asset/expense): debits minus credits. */
    public long debitBalance() {
        return debitsPosted - creditsPosted;
    }

    /** Net position for a credit-normal account (liability/income/equity): credits minus debits. */
    public long creditBalance() {
        return creditsPosted - debitsPosted;
    }
}
