package com.payments.tbjava.domain;

/**
 * In-memory account record. Mirrors TigerBeetle's Account semantics with these
 * deliberate simplifications:
 * <ul>
 *   <li>{@code id} is u64 (long) instead of u128.</li>
 *   <li>Amounts are u64 (long) — signed in Java but always treated as unsigned;
 *       max value is 9.2e18 which is more than enough for any currency in cents.</li>
 *   <li>No reserved bytes — Java doesn't care about on-disk layout for in-memory state.</li>
 * </ul>
 *
 * <p>This class is mutable and is only mutated by the single writer thread.
 * Read-only callers must use {@link #snapshot()} to get a stable copy.
 */
public final class Account {

    private final UInt128 id;
    private final int ledger;
    private final short code;
    private final short flags;
    private final long userData64;
    private final int userData32;

    // Mutable balance counters — only updated by the writer thread.
    private long debitsPending;
    private long debitsPosted;
    private long creditsPending;
    private long creditsPosted;

    private final long timestamp;  // nanoseconds since cluster epoch, assigned at create time

    public Account(UInt128 id, int ledger, short code, short flags,
                   long userData64, int userData32, long timestamp) {
        this.id = id;
        this.ledger = ledger;
        this.code = code;
        this.flags = flags;
        this.userData64 = userData64;
        this.userData32 = userData32;
        this.timestamp = timestamp;
    }

    public UInt128 id() { return id; }
    public int ledger() { return ledger; }
    public short code() { return code; }
    public short flags() { return flags; }
    public long userData64() { return userData64; }
    public int userData32() { return userData32; }
    public long timestamp() { return timestamp; }

    public long debitsPending() { return debitsPending; }
    public long debitsPosted() { return debitsPosted; }
    public long creditsPending() { return creditsPending; }
    public long creditsPosted() { return creditsPosted; }

    public void addDebitsPending(long delta)  { this.debitsPending += delta; }
    public void addDebitsPosted(long delta)   { this.debitsPosted += delta; }
    public void addCreditsPending(long delta) { this.creditsPending += delta; }
    public void addCreditsPosted(long delta)  { this.creditsPosted += delta; }

    /** Returns true if {@code (flags & flag) != 0}. */
    public boolean hasFlag(int flag) {
        return (flags & flag) != 0;
    }

    /**
     * Returns true if posting this debit amount would violate a balance limit
     * (e.g. an asset account flagged debits_must_not_exceed_credits going negative).
     */
    public boolean wouldExceedDebitLimit(long extraDebit) {
        if (!hasFlag(AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS)) return false;
        long newDebits = debitsPosted + debitsPending + extraDebit;
        return Long.compareUnsigned(newDebits, creditsPosted) > 0;
    }

    public boolean wouldExceedCreditLimit(long extraCredit) {
        if (!hasFlag(AccountFlags.CREDITS_MUST_NOT_EXCEED_DEBITS)) return false;
        long newCredits = creditsPosted + creditsPending + extraCredit;
        return Long.compareUnsigned(newCredits, debitsPosted) > 0;
    }

    /** Returns an immutable snapshot for read-only consumers. */
    public AccountSnapshot snapshot() {
        return new AccountSnapshot(id, ledger, code, flags,
                userData64, userData32,
                debitsPending, debitsPosted, creditsPending, creditsPosted,
                timestamp);
    }
}
