package com.payments.ledger.raft;

import com.payments.ledger.domain.Account;
import com.payments.ledger.domain.CreateAccountResult;
import com.payments.ledger.domain.CreateTransferResult;
import com.payments.ledger.domain.LedgerBalance;
import com.payments.ledger.domain.Transfer;
import com.payments.ledger.domain.UInt128;
import com.payments.ledger.engine.LedgerStateMachine;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Wire codec for commands replicated through the Raft log, and for read queries.
 *
 * <p>A <b>write command</b> is the unit appended to the Raft log and applied
 * identically on every node:
 * <pre>
 *   [op:1][baseTimestamp:8][payload...]
 * </pre>
 * The client sends it with {@code baseTimestamp = 0}; the leader assigns the real
 * base timestamp in {@code startTransaction} (via {@link #withBaseTimestamp}) so
 * the committed entry carries it and followers reproduce identical state. The
 * payload layout mirrors the journal format so it stays consistent with the
 * single-node WAL.
 *
 * <p>Read queries never enter the log; they go through the state machine's
 * {@code query} path and use a separate {@code [qop:1][...]} framing.
 */
public final class LedgerCommandCodec {

    private LedgerCommandCodec() {}

    // --- write op codes ---
    public static final byte OP_ACCOUNTS  = 1;
    public static final byte OP_TRANSFERS = 2;
    public static final byte OP_EXPIRE    = 3;

    // --- read op codes ---
    public static final byte Q_LOOKUP_ACCOUNT = 1;
    public static final byte Q_TRIAL_BALANCE  = 2;

    private static final int CMD_HEADER = 1 + 8; // op + baseTimestamp

    // ----------------------------------------------------------------------
    // Encode write commands (client side; baseTimestamp left 0)
    // ----------------------------------------------------------------------

    public static byte[] encodeAccounts(List<Account> batch) {
        ByteBuffer b = ByteBuffer.allocate(CMD_HEADER + 4 + batch.size() * 36);
        b.put(OP_ACCOUNTS).putLong(0L).putInt(batch.size());
        for (Account a : batch) {
            b.putLong(a.id().hi()).putLong(a.id().lo());
            b.putLong(a.userData64()).putInt(a.userData32());
            b.putInt(a.ledger()).putShort(a.code()).putShort(a.flags());
        }
        return b.array();
    }

    public static byte[] encodeTransfers(List<Transfer> batch) {
        ByteBuffer b = ByteBuffer.allocate(CMD_HEADER + 4 + batch.size() * 96);
        b.put(OP_TRANSFERS).putLong(0L).putInt(batch.size());
        for (Transfer t : batch) {
            b.putLong(t.id().hi()).putLong(t.id().lo());
            b.putLong(t.debitAccountId().hi()).putLong(t.debitAccountId().lo());
            b.putLong(t.creditAccountId().hi()).putLong(t.creditAccountId().lo());
            b.putLong(t.amount());
            b.putLong(t.pendingId().hi()).putLong(t.pendingId().lo());
            b.putLong(t.userData64()).putInt(t.userData32()).putInt(t.timeoutSeconds());
            b.putInt(t.ledger()).putShort(t.code()).putShort(t.flags());
        }
        return b.array();
    }

    public static byte[] encodeExpire() {
        return ByteBuffer.allocate(CMD_HEADER).put(OP_EXPIRE).putLong(0L).array();
    }

    // ----------------------------------------------------------------------
    // Leader-side: how many timestamps to reserve, and stamp the command
    // ----------------------------------------------------------------------

    public static byte op(byte[] cmd) { return cmd[0]; }

    /** The base timestamp stamped into the command (0 before the leader assigns it). */
    public static long baseTimestamp(byte[] cmd) { return ByteBuffer.wrap(cmd, 1, 8).getLong(); }

    /** Number of contiguous timestamps this command consumes (batch size; 1 for expire). */
    public static int reserveCount(byte[] cmd) {
        return cmd[0] == OP_EXPIRE ? 1 : ByteBuffer.wrap(cmd, CMD_HEADER, 4).getInt();
    }

    /** Return a copy of {@code cmd} with the base timestamp set (leader assigns it). */
    public static byte[] withBaseTimestamp(byte[] cmd, long baseTimestamp) {
        byte[] out = cmd.clone();
        ByteBuffer.wrap(out).put(0, cmd[0]).putLong(1, baseTimestamp);
        return out;
    }

    // ----------------------------------------------------------------------
    // Apply a committed command to the state machine; return encoded result
    // ----------------------------------------------------------------------

    public static byte[] apply(byte[] cmd, LedgerStateMachine sm) {
        ByteBuffer b = ByteBuffer.wrap(cmd);
        byte op = b.get();
        long base = b.getLong();
        switch (op) {
            case OP_ACCOUNTS -> {
                List<Account> batch = readAccounts(b);
                return encodeAccountResults(sm.createAccounts(batch, base));
            }
            case OP_TRANSFERS -> {
                List<Transfer> batch = readTransfers(b);
                return encodeTransferResults(sm.createTransfers(batch, base));
            }
            case OP_EXPIRE -> {
                int n = sm.expirePending(base);
                return ByteBuffer.allocate(4).putInt(n).array();
            }
            default -> throw new IllegalArgumentException("unknown op " + op);
        }
    }

    private static List<Account> readAccounts(ByteBuffer b) {
        int count = b.getInt();
        List<Account> batch = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            UInt128 id = new UInt128(b.getLong(), b.getLong());
            long ud64 = b.getLong();
            int ud32 = b.getInt();
            int ledger = b.getInt();
            short code = b.getShort();
            short flags = b.getShort();
            batch.add(new Account(id, ledger, code, flags, ud64, ud32, 0));
        }
        return batch;
    }

    private static List<Transfer> readTransfers(ByteBuffer b) {
        int count = b.getInt();
        List<Transfer> batch = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            UInt128 id = new UInt128(b.getLong(), b.getLong());
            UInt128 debit = new UInt128(b.getLong(), b.getLong());
            UInt128 credit = new UInt128(b.getLong(), b.getLong());
            long amount = b.getLong();
            UInt128 pendingId = new UInt128(b.getLong(), b.getLong());
            long ud64 = b.getLong();
            int ud32 = b.getInt();
            int timeout = b.getInt();
            int ledger = b.getInt();
            short code = b.getShort();
            short flags = b.getShort();
            batch.add(new Transfer(id, debit, credit, amount, pendingId, ud64, ud32, timeout, ledger, code, flags, 0));
        }
        return batch;
    }

    // ----------------------------------------------------------------------
    // Result encode/decode (per-record enum ordinals)
    // ----------------------------------------------------------------------

    private static byte[] encodeAccountResults(List<CreateAccountResult> rs) {
        ByteBuffer b = ByteBuffer.allocate(4 + rs.size());
        b.putInt(rs.size());
        for (CreateAccountResult r : rs) b.put((byte) r.ordinal());
        return b.array();
    }

    private static byte[] encodeTransferResults(List<CreateTransferResult> rs) {
        ByteBuffer b = ByteBuffer.allocate(4 + rs.size());
        b.putInt(rs.size());
        for (CreateTransferResult r : rs) b.put((byte) r.ordinal());
        return b.array();
    }

    public static List<CreateAccountResult> decodeAccountResults(byte[] data) {
        ByteBuffer b = ByteBuffer.wrap(data);
        int n = b.getInt();
        List<CreateAccountResult> out = new ArrayList<>(n);
        CreateAccountResult[] vs = CreateAccountResult.values();
        for (int i = 0; i < n; i++) out.add(vs[b.get() & 0xFF]);
        return out;
    }

    public static List<CreateTransferResult> decodeTransferResults(byte[] data) {
        ByteBuffer b = ByteBuffer.wrap(data);
        int n = b.getInt();
        List<CreateTransferResult> out = new ArrayList<>(n);
        CreateTransferResult[] vs = CreateTransferResult.values();
        for (int i = 0; i < n; i++) out.add(vs[b.get() & 0xFF]);
        return out;
    }

    // ----------------------------------------------------------------------
    // Read queries
    // ----------------------------------------------------------------------

    public static byte[] encodeLookupAccount(UInt128 id) {
        return ByteBuffer.allocate(1 + 16).put(Q_LOOKUP_ACCOUNT).putLong(id.hi()).putLong(id.lo()).array();
    }

    public static byte[] encodeTrialBalance() {
        return new byte[]{Q_TRIAL_BALANCE};
    }

    /** Snapshot of an account balance returned by a query, or {@code found=false}. */
    public record AccountView(boolean found, int ledger, long debitsPending, long debitsPosted,
                              long creditsPending, long creditsPosted) {}

    public static byte[] query(byte[] q, LedgerStateMachine sm) {
        ByteBuffer b = ByteBuffer.wrap(q);
        byte qop = b.get();
        switch (qop) {
            case Q_LOOKUP_ACCOUNT -> {
                UInt128 id = new UInt128(b.getLong(), b.getLong());
                var snap = sm.lookupAccount(id);
                ByteBuffer out = ByteBuffer.allocate(1 + (snap == null ? 0 : 4 + 8 * 4));
                if (snap == null) { out.put((byte) 0); return out.array(); }
                out.put((byte) 1).putInt(snap.ledger())
                   .putLong(snap.debitsPending()).putLong(snap.debitsPosted())
                   .putLong(snap.creditsPending()).putLong(snap.creditsPosted());
                return out.array();
            }
            case Q_TRIAL_BALANCE -> {
                List<LedgerBalance> tb = sm.trialBalance();
                ByteBuffer out = ByteBuffer.allocate(4 + tb.size() * (4 + 8 + 8));
                out.putInt(tb.size());
                for (LedgerBalance lb : tb) out.putInt(lb.ledger()).putLong(lb.debitsPosted()).putLong(lb.creditsPosted());
                return out.array();
            }
            default -> throw new IllegalArgumentException("unknown query op " + qop);
        }
    }

    public static AccountView decodeAccount(byte[] data) {
        ByteBuffer b = ByteBuffer.wrap(data);
        if (b.get() == 0) return new AccountView(false, 0, 0, 0, 0, 0);
        return new AccountView(true, b.getInt(), b.getLong(), b.getLong(), b.getLong(), b.getLong());
    }
}
