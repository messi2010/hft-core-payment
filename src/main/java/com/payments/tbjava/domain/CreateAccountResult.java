package com.payments.tbjava.domain;

/**
 * Per-account result code from a batch {@code createAccounts} call.
 * Mirrors TigerBeetle's CreateAccountResult values for client portability.
 */
public enum CreateAccountResult {
    OK,
    /** Same id already exists with identical fields -- safe idempotent retry. */
    EXISTS,
    /** Same id exists but with different fields -- caller bug. */
    EXISTS_WITH_DIFFERENT_FIELDS,
    ID_MUST_NOT_BE_ZERO,
    LEDGER_MUST_NOT_BE_ZERO,
    CODE_MUST_NOT_BE_ZERO,
    FLAGS_ARE_MUTUALLY_EXCLUSIVE,
    /** Initial balance fields must be zero -- TB assigns them, you don't set them. */
    DEBITS_PENDING_MUST_BE_ZERO,
    DEBITS_POSTED_MUST_BE_ZERO,
    CREDITS_PENDING_MUST_BE_ZERO,
    CREDITS_POSTED_MUST_BE_ZERO,
    /** Used to signal that a LINKED account earlier in the batch failed. */
    LINKED_EVENT_FAILED,
}
