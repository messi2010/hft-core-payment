package com.payments.ledger.cluster;

import com.payments.ledger.domain.Account;
import com.payments.ledger.domain.CreateAccountResult;
import com.payments.ledger.domain.CreateTransferResult;
import com.payments.ledger.domain.LedgerBalance;
import com.payments.ledger.domain.Transfer;
import com.payments.ledger.domain.UInt128;
import com.payments.ledger.engine.LedgerStateMachine;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Wire codec for messages flowing through the Aeron Cluster log and for
 * read-query messages. Mirrors the Ratis-era {@code LedgerCommandCodec} but
 * works directly against Agrona {@link DirectBuffer}s to fit Aeron's zero-copy
 * ingress/egress model.
 *
 * <h2>Framing</h2>
 *
 * <p>All messages are little-endian. There is <b>no {@code baseTimestamp} field
 * on the wire</b> (unlike the Ratis codec): under Aeron Cluster every node
 * applies messages in the same committed order, so the deterministic monotonic
 * timestamp source ({@link com.payments.ledger.cluster.ClusterIdGenerator}) is
 * snapshot-resident state of the {@link LedgerClusteredService} — it doesn't
 * need to ride with each command.
 *
 * <pre>
 *   writes
 *     OP_ACCOUNTS    : [op:1][count:4][account*count]     account = 36 B
 *     OP_TRANSFERS   : [op:1][count:4][transfer*count]    transfer = 96 B
 *     OP_EXPIRE      : [op:1]
 *
 *   queries
 *     Q_LOOKUP_ACCOUNT : [qop:1][idHi:8][idLo:8]
 *     Q_TRIAL_BALANCE  : [qop:1]
 *
 *   responses (matching op)
 *     ACCOUNT_RESULTS  : [count:4][ordinal*count]
 *     TRANSFER_RESULTS : [count:4][ordinal*count]
 *     EXPIRE_RESULT    : [expiredCount:4]
 *     LOOKUP_RESULT    : [found:1] then if found: [ledger:4][debPend:8][debPost:8][crPend:8][crPost:8]
 *     TRIAL_BALANCE    : [count:4][(ledger:4,debPost:8,crPost:8)*count]
 * </pre>
 */
public final class ClusterCommandCodec {

    private ClusterCommandCodec() {}

    // ---- op codes (writes) ----
    public static final byte OP_ACCOUNTS  = 1;
    public static final byte OP_TRANSFERS = 2;
    public static final byte OP_EXPIRE    = 3;

    // ---- op codes (queries) — disjoint from writes so a dispatcher can branch on first byte ----
    public static final byte Q_LOOKUP_ACCOUNT = (byte) 0x81;
    public static final byte Q_TRIAL_BALANCE  = (byte) 0x82;

    // ---- record sizes ----
    public static final int ACCOUNT_BYTES  = 36;   // 16+8+4+4+2+2
    public static final int TRANSFER_BYTES = 96;   // 16+16+16+8+16+8+4+4+4+2+2

    public static int accountsBytes(int count)  { return 1 + 4 + count * ACCOUNT_BYTES; }
    public static int transfersBytes(int count) { return 1 + 4 + count * TRANSFER_BYTES; }

    // =====================================================================
    // Encode (writes) — into a caller-provided MutableDirectBuffer
    // =====================================================================

    /** @return total bytes written. */
    public static int encodeAccounts(List<Account> batch, MutableDirectBuffer dst, int offset) {
        int p = offset;
        dst.putByte(p, OP_ACCOUNTS); p += 1;
        dst.putInt(p, batch.size(), ByteOrder.LITTLE_ENDIAN); p += 4;
        for (Account a : batch) {
            dst.putLong(p, a.id().hi(), ByteOrder.LITTLE_ENDIAN); p += 8;
            dst.putLong(p, a.id().lo(), ByteOrder.LITTLE_ENDIAN); p += 8;
            dst.putLong(p, a.userData64(), ByteOrder.LITTLE_ENDIAN); p += 8;
            dst.putInt(p, a.userData32(), ByteOrder.LITTLE_ENDIAN); p += 4;
            dst.putInt(p, a.ledger(), ByteOrder.LITTLE_ENDIAN); p += 4;
            dst.putShort(p, a.code(), ByteOrder.LITTLE_ENDIAN); p += 2;
            dst.putShort(p, a.flags(), ByteOrder.LITTLE_ENDIAN); p += 2;
        }
        return p - offset;
    }

    /** @return total bytes written. */
    public static int encodeTransfers(List<Transfer> batch, MutableDirectBuffer dst, int offset) {
        int p = offset;
        dst.putByte(p, OP_TRANSFERS); p += 1;
        dst.putInt(p, batch.size(), ByteOrder.LITTLE_ENDIAN); p += 4;
        for (Transfer t : batch) {
            dst.putLong(p, t.id().hi(), ByteOrder.LITTLE_ENDIAN); p += 8;
            dst.putLong(p, t.id().lo(), ByteOrder.LITTLE_ENDIAN); p += 8;
            dst.putLong(p, t.debitAccountId().hi(), ByteOrder.LITTLE_ENDIAN); p += 8;
            dst.putLong(p, t.debitAccountId().lo(), ByteOrder.LITTLE_ENDIAN); p += 8;
            dst.putLong(p, t.creditAccountId().hi(), ByteOrder.LITTLE_ENDIAN); p += 8;
            dst.putLong(p, t.creditAccountId().lo(), ByteOrder.LITTLE_ENDIAN); p += 8;
            dst.putLong(p, t.amount(), ByteOrder.LITTLE_ENDIAN); p += 8;
            dst.putLong(p, t.pendingId().hi(), ByteOrder.LITTLE_ENDIAN); p += 8;
            dst.putLong(p, t.pendingId().lo(), ByteOrder.LITTLE_ENDIAN); p += 8;
            dst.putLong(p, t.userData64(), ByteOrder.LITTLE_ENDIAN); p += 8;
            dst.putInt(p, t.userData32(), ByteOrder.LITTLE_ENDIAN); p += 4;
            dst.putInt(p, t.timeoutSeconds(), ByteOrder.LITTLE_ENDIAN); p += 4;
            dst.putInt(p, t.ledger(), ByteOrder.LITTLE_ENDIAN); p += 4;
            dst.putShort(p, t.code(), ByteOrder.LITTLE_ENDIAN); p += 2;
            dst.putShort(p, t.flags(), ByteOrder.LITTLE_ENDIAN); p += 2;
        }
        return p - offset;
    }

    public static int encodeExpire(MutableDirectBuffer dst, int offset) {
        dst.putByte(offset, OP_EXPIRE);
        return 1;
    }

    public static int encodeLookupAccount(UInt128 id, MutableDirectBuffer dst, int offset) {
        dst.putByte(offset, Q_LOOKUP_ACCOUNT);
        dst.putLong(offset + 1, id.hi(), ByteOrder.LITTLE_ENDIAN);
        dst.putLong(offset + 9, id.lo(), ByteOrder.LITTLE_ENDIAN);
        return 17;
    }

    public static int encodeTrialBalance(MutableDirectBuffer dst, int offset) {
        dst.putByte(offset, Q_TRIAL_BALANCE);
        return 1;
    }

    // Convenience for clients that want a fresh heap buffer.
    public static MutableDirectBuffer allocate(int bytes) {
        return new UnsafeBuffer(ByteBuffer.allocate(bytes));
    }

    public static byte op(DirectBuffer src, int offset) { return src.getByte(offset); }

    // =====================================================================
    // Apply / Query — called inside ClusteredService.onSessionMessage
    // =====================================================================

    /**
     * Apply a committed write or run a query, writing the response into
     * {@code dst} at {@code dstOffset}.
     *
     * @param base monotonic timestamp base reserved by the state machine for
     *             this command (ignored for queries).
     * @return response length in bytes.
     */
    public static int applyOrQuery(DirectBuffer src, int srcOffset, int length,
                                   LedgerStateMachine sm,
                                   long base,
                                   MutableDirectBuffer dst, int dstOffset) {
        if (length < 1) throw new IllegalArgumentException("empty message");
        byte op = src.getByte(srcOffset);
        int p = srcOffset + 1;
        switch (op) {
            case OP_ACCOUNTS -> {
                int count = src.getInt(p, ByteOrder.LITTLE_ENDIAN); p += 4;
                List<Account> batch = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    UInt128 id = new UInt128(src.getLong(p, ByteOrder.LITTLE_ENDIAN),
                                             src.getLong(p + 8, ByteOrder.LITTLE_ENDIAN));
                    long ud64 = src.getLong(p + 16, ByteOrder.LITTLE_ENDIAN);
                    int ud32 = src.getInt(p + 24, ByteOrder.LITTLE_ENDIAN);
                    int ledger = src.getInt(p + 28, ByteOrder.LITTLE_ENDIAN);
                    short code = src.getShort(p + 32, ByteOrder.LITTLE_ENDIAN);
                    short flags = src.getShort(p + 34, ByteOrder.LITTLE_ENDIAN);
                    batch.add(new Account(id, ledger, code, flags, ud64, ud32, 0));
                    p += ACCOUNT_BYTES;
                }
                return encodeAccountResults(sm.createAccounts(batch, base), dst, dstOffset);
            }
            case OP_TRANSFERS -> {
                int count = src.getInt(p, ByteOrder.LITTLE_ENDIAN); p += 4;
                List<Transfer> batch = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    UInt128 id     = new UInt128(src.getLong(p,      ByteOrder.LITTLE_ENDIAN),
                                                 src.getLong(p + 8,  ByteOrder.LITTLE_ENDIAN));
                    UInt128 debit  = new UInt128(src.getLong(p + 16, ByteOrder.LITTLE_ENDIAN),
                                                 src.getLong(p + 24, ByteOrder.LITTLE_ENDIAN));
                    UInt128 credit = new UInt128(src.getLong(p + 32, ByteOrder.LITTLE_ENDIAN),
                                                 src.getLong(p + 40, ByteOrder.LITTLE_ENDIAN));
                    long amount    = src.getLong(p + 48, ByteOrder.LITTLE_ENDIAN);
                    UInt128 pend   = new UInt128(src.getLong(p + 56, ByteOrder.LITTLE_ENDIAN),
                                                 src.getLong(p + 64, ByteOrder.LITTLE_ENDIAN));
                    long ud64      = src.getLong(p + 72, ByteOrder.LITTLE_ENDIAN);
                    int ud32       = src.getInt(p + 80, ByteOrder.LITTLE_ENDIAN);
                    int timeout    = src.getInt(p + 84, ByteOrder.LITTLE_ENDIAN);
                    int ledger     = src.getInt(p + 88, ByteOrder.LITTLE_ENDIAN);
                    short code     = src.getShort(p + 92, ByteOrder.LITTLE_ENDIAN);
                    short flags    = src.getShort(p + 94, ByteOrder.LITTLE_ENDIAN);
                    batch.add(new Transfer(id, debit, credit, amount, pend, ud64, ud32, timeout,
                                           ledger, code, flags, 0));
                    p += TRANSFER_BYTES;
                }
                return encodeTransferResults(sm.createTransfers(batch, base), dst, dstOffset);
            }
            case OP_EXPIRE -> {
                int n = sm.expirePending(base);
                dst.putInt(dstOffset, n, ByteOrder.LITTLE_ENDIAN);
                return 4;
            }
            case Q_LOOKUP_ACCOUNT -> {
                UInt128 id = new UInt128(src.getLong(p, ByteOrder.LITTLE_ENDIAN),
                                         src.getLong(p + 8, ByteOrder.LITTLE_ENDIAN));
                var snap = sm.lookupAccount(id);
                if (snap == null) { dst.putByte(dstOffset, (byte) 0); return 1; }
                int o = dstOffset;
                dst.putByte(o, (byte) 1); o += 1;
                dst.putInt(o, snap.ledger(), ByteOrder.LITTLE_ENDIAN); o += 4;
                dst.putLong(o, snap.debitsPending(), ByteOrder.LITTLE_ENDIAN); o += 8;
                dst.putLong(o, snap.debitsPosted(),  ByteOrder.LITTLE_ENDIAN); o += 8;
                dst.putLong(o, snap.creditsPending(),ByteOrder.LITTLE_ENDIAN); o += 8;
                dst.putLong(o, snap.creditsPosted(), ByteOrder.LITTLE_ENDIAN); o += 8;
                return o - dstOffset;
            }
            case Q_TRIAL_BALANCE -> {
                List<LedgerBalance> tb = sm.trialBalance();
                int o = dstOffset;
                dst.putInt(o, tb.size(), ByteOrder.LITTLE_ENDIAN); o += 4;
                for (LedgerBalance lb : tb) {
                    dst.putInt(o,  lb.ledger(),          ByteOrder.LITTLE_ENDIAN); o += 4;
                    dst.putInt(o,  lb.accounts(),        ByteOrder.LITTLE_ENDIAN); o += 4;
                    dst.putLong(o, lb.debitsPosted(),    ByteOrder.LITTLE_ENDIAN); o += 8;
                    dst.putLong(o, lb.creditsPosted(),   ByteOrder.LITTLE_ENDIAN); o += 8;
                    dst.putLong(o, lb.debitsPending(),   ByteOrder.LITTLE_ENDIAN); o += 8;
                    dst.putLong(o, lb.creditsPending(),  ByteOrder.LITTLE_ENDIAN); o += 8;
                }
                return o - dstOffset;
            }
            default -> throw new IllegalArgumentException("unknown op: 0x" + Integer.toHexString(op & 0xFF));
        }
    }

    /** Number of monotonic timestamps a command reserves (0 for queries). */
    public static int reserveCount(DirectBuffer src, int offset, int length) {
        if (length < 1) return 0;
        byte op = src.getByte(offset);
        return switch (op) {
            case OP_ACCOUNTS, OP_TRANSFERS -> src.getInt(offset + 1, ByteOrder.LITTLE_ENDIAN);
            case OP_EXPIRE -> 1;
            default -> 0;   // queries
        };
    }

    public static boolean isQuery(byte op) {
        return (op & 0x80) != 0;
    }

    // =====================================================================
    // Response encoders / decoders
    // =====================================================================

    public static int encodeAccountResults(List<CreateAccountResult> rs, MutableDirectBuffer dst, int offset) {
        dst.putInt(offset, rs.size(), ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < rs.size(); i++) dst.putByte(offset + 4 + i, (byte) rs.get(i).ordinal());
        return 4 + rs.size();
    }

    public static int encodeTransferResults(List<CreateTransferResult> rs, MutableDirectBuffer dst, int offset) {
        dst.putInt(offset, rs.size(), ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < rs.size(); i++) dst.putByte(offset + 4 + i, (byte) rs.get(i).ordinal());
        return 4 + rs.size();
    }

    public static List<CreateAccountResult> decodeAccountResults(DirectBuffer src, int offset) {
        int n = src.getInt(offset, ByteOrder.LITTLE_ENDIAN);
        List<CreateAccountResult> out = new ArrayList<>(n);
        CreateAccountResult[] vs = CreateAccountResult.values();
        for (int i = 0; i < n; i++) out.add(vs[src.getByte(offset + 4 + i) & 0xFF]);
        return out;
    }

    public static List<CreateTransferResult> decodeTransferResults(DirectBuffer src, int offset) {
        int n = src.getInt(offset, ByteOrder.LITTLE_ENDIAN);
        List<CreateTransferResult> out = new ArrayList<>(n);
        CreateTransferResult[] vs = CreateTransferResult.values();
        for (int i = 0; i < n; i++) out.add(vs[src.getByte(offset + 4 + i) & 0xFF]);
        return out;
    }

    /** Account snapshot returned by {@link #Q_LOOKUP_ACCOUNT}. */
    public record AccountView(boolean found, int ledger,
                              long debitsPending, long debitsPosted,
                              long creditsPending, long creditsPosted) {}

    public static AccountView decodeAccount(DirectBuffer src, int offset) {
        if (src.getByte(offset) == 0) return new AccountView(false, 0, 0, 0, 0, 0);
        return new AccountView(true,
                src.getInt (offset + 1,  ByteOrder.LITTLE_ENDIAN),
                src.getLong(offset + 5,  ByteOrder.LITTLE_ENDIAN),
                src.getLong(offset + 13, ByteOrder.LITTLE_ENDIAN),
                src.getLong(offset + 21, ByteOrder.LITTLE_ENDIAN),
                src.getLong(offset + 29, ByteOrder.LITTLE_ENDIAN));
    }

    public static List<LedgerBalance> decodeTrialBalance(DirectBuffer src, int offset) {
        int n = src.getInt(offset, ByteOrder.LITTLE_ENDIAN);
        List<LedgerBalance> out = new ArrayList<>(n);
        int p = offset + 4;
        for (int i = 0; i < n; i++) {
            int    ledger     = src.getInt(p,       ByteOrder.LITTLE_ENDIAN);
            int    accounts   = src.getInt(p + 4,   ByteOrder.LITTLE_ENDIAN);
            long   debPosted  = src.getLong(p + 8,  ByteOrder.LITTLE_ENDIAN);
            long   crPosted   = src.getLong(p + 16, ByteOrder.LITTLE_ENDIAN);
            long   debPending = src.getLong(p + 24, ByteOrder.LITTLE_ENDIAN);
            long   crPending  = src.getLong(p + 32, ByteOrder.LITTLE_ENDIAN);
            out.add(new LedgerBalance(ledger, accounts, debPosted, crPosted, debPending, crPending));
            p += 40;
        }
        return out;
    }
}
