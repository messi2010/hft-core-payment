package com.payments.ledger.domain;

/**
 * Trial-balance row for one ledger: the summed counters across all of its
 * accounts. In a correct double-entry ledger {@code debitsPosted} must equal
 * {@code creditsPosted} (and likewise for pending) — {@link #balanced()} exposes
 * that invariant for auditing/reconciliation.
 */
public record LedgerBalance(
        int ledger,
        int accounts,
        long debitsPosted,
        long creditsPosted,
        long debitsPending,
        long creditsPending
) {
    /** True iff posted debits equal posted credits and pending debits equal pending credits. */
    public boolean balanced() {
        return debitsPosted == creditsPosted && debitsPending == creditsPending;
    }
}
