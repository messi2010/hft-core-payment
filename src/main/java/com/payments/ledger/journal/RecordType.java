package com.payments.ledger.journal;

/** Journal record type codes. Persisted as a single byte. */
public final class RecordType {
    private RecordType() {}

    public static final byte CREATE_ACCOUNTS  = 1;  // one command = a batch of accounts
    public static final byte CREATE_TRANSFERS = 2;  // one command = a batch of transfers
    public static final byte CHECKPOINT       = 3;  // marker -- snapshot id taken up to this offset
    public static final byte EXPIRE_PENDING   = 4;  // void OPEN pendings with timeout <= header timestamp
}
