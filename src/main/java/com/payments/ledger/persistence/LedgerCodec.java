package com.payments.ledger.persistence;

import com.payments.ledger.domain.AccountSnapshot;
import com.payments.ledger.domain.Transfer;
import com.payments.ledger.domain.UInt128;
import com.payments.ledger.util.Bytes;

import java.nio.ByteBuffer;

/**
 * Fixed-width little-endian codecs for the values stored in the snapshot LSMs.
 *
 * <p>Unlike the journal (which stores only command <em>inputs</em>), a snapshot
 * must capture full mutated state -- including the four balance counters of an
 * account -- so recovery can skip replaying the transfers that produced them.
 */
public final class LedgerCodec {

    private LedgerCodec() {}

    public static final int ACCOUNT_BYTES = 76;
    public static final int TRANSFER_BYTES = 104;

    public static byte[] encodeAccount(AccountSnapshot a) {
        ByteBuffer b = Bytes.allocate(ACCOUNT_BYTES);
        b.putLong(a.id().hi());
        b.putLong(a.id().lo());
        b.putInt(a.ledger());
        b.putShort(a.code());
        b.putShort(a.flags());
        b.putLong(a.userData64());
        b.putInt(a.userData32());
        b.putLong(a.debitsPending());
        b.putLong(a.debitsPosted());
        b.putLong(a.creditsPending());
        b.putLong(a.creditsPosted());
        b.putLong(a.timestamp());
        return b.array();
    }

    public static AccountSnapshot decodeAccount(byte[] bytes) {
        ByteBuffer b = Bytes.wrap(bytes);
        UInt128 id = new UInt128(b.getLong(), b.getLong());
        int ledger = b.getInt();
        short code = b.getShort();
        short flags = b.getShort();
        long userData64 = b.getLong();
        int userData32 = b.getInt();
        long debitsPending = b.getLong();
        long debitsPosted = b.getLong();
        long creditsPending = b.getLong();
        long creditsPosted = b.getLong();
        long timestamp = b.getLong();
        return new AccountSnapshot(id, ledger, code, flags, userData64, userData32,
                debitsPending, debitsPosted, creditsPending, creditsPosted, timestamp);
    }

    public static byte[] encodeTransfer(Transfer t) {
        ByteBuffer b = Bytes.allocate(TRANSFER_BYTES);
        b.putLong(t.id().hi());
        b.putLong(t.id().lo());
        b.putLong(t.debitAccountId().hi());
        b.putLong(t.debitAccountId().lo());
        b.putLong(t.creditAccountId().hi());
        b.putLong(t.creditAccountId().lo());
        b.putLong(t.amount());
        b.putLong(t.pendingId().hi());
        b.putLong(t.pendingId().lo());
        b.putLong(t.userData64());
        b.putInt(t.userData32());
        b.putInt(t.timeoutSeconds());
        b.putInt(t.ledger());
        b.putShort(t.code());
        b.putShort(t.flags());
        b.putLong(t.timestamp());
        return b.array();
    }

    public static Transfer decodeTransfer(byte[] bytes) {
        ByteBuffer b = Bytes.wrap(bytes);
        UInt128 id = new UInt128(b.getLong(), b.getLong());
        UInt128 debit = new UInt128(b.getLong(), b.getLong());
        UInt128 credit = new UInt128(b.getLong(), b.getLong());
        long amount = b.getLong();
        UInt128 pendingId = new UInt128(b.getLong(), b.getLong());
        long userData64 = b.getLong();
        int userData32 = b.getInt();
        int timeout = b.getInt();
        int ledger = b.getInt();
        short code = b.getShort();
        short flags = b.getShort();
        long timestamp = b.getLong();
        return new Transfer(id, debit, credit, amount, pendingId, userData64, userData32,
                timeout, ledger, code, flags, timestamp);
    }
}
