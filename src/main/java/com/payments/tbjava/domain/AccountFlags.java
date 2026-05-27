package com.payments.tbjava.domain;

/**
 * Account flag bit positions. Matches TigerBeetle's account flag conventions
 * so callers familiar with TB can switch with minimal mental load.
 */
public final class AccountFlags {
    private AccountFlags() {}

    /** Reserved -- ignored by engine but persisted. */
    public static final int LINKED                          = 1 << 0;
    /** Asset/expense accounts: refuse postings that would push debits over credits. */
    public static final int DEBITS_MUST_NOT_EXCEED_CREDITS  = 1 << 1;
    /** Liability/income accounts: refuse postings that would push credits over debits. */
    public static final int CREDITS_MUST_NOT_EXCEED_DEBITS  = 1 << 2;
    /** Visible to CDC stream. Accounts without this flag are still consistent
     *  but their transfers won't be replicated downstream. */
    public static final int HISTORY                         = 1 << 3;
    /** Account is closed -- no new transfers may debit or credit it. */
    public static final int CLOSED                          = 1 << 4;
}
