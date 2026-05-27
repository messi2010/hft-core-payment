package com.payments.tbjava.engine;

import com.payments.tbjava.domain.*;
import com.payments.tbjava.storage.AccountStore;
import com.payments.tbjava.storage.AccountTransferIndex;
import com.payments.tbjava.storage.PendingTransferIndex;
import com.payments.tbjava.storage.TransferStore;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Single-writer state machine. All ledger mutations flow through here on the
 * dedicated writer thread. Public methods are NOT thread-safe -- they're invoked
 * exclusively by the Disruptor's event handler (live path) or by recovery
 * (replay path).
 *
 * <p><b>Deterministic by construction.</b> This class performs no I/O and reads
 * no wall clock: every record's timestamp is derived from a {@code baseTimestamp}
 * passed in by the caller ({@code ts = baseTimestamp + indexInBatch}). The same
 * command applied with the same base therefore produces byte-identical state.
 * That is what lets recovery rebuild state by replaying the journaled input
 * commands through this same code -- including linked-chain rollback -- rather
 * than journaling and re-loading mutated balances. Durability (journaling the
 * command, fsync) is the caller's responsibility; keeping it out of here is what
 * makes replay safe.
 *
 * <p>Invariants enforced:
 * <ul>
 *   <li>Every transfer is balanced by construction (one debit leg + one credit leg).</li>
 *   <li>Both accounts referenced must share the same {@code ledger}.</li>
 *   <li>Transfer/account ID collision returns {@code EXISTS} -- idempotent retry.</li>
 *   <li>Account balance flags ({@code DEBITS_MUST_NOT_EXCEED_CREDITS}, etc.) are checked.</li>
 *   <li>Pending transfers: PENDING reserves funds; POST_PENDING settles; VOID_PENDING releases.</li>
 *   <li>Linked transfers: all-or-nothing within a batch chain, with full rollback.</li>
 * </ul>
 */
public final class LedgerStateMachine {

    private static final Logger log = LoggerFactory.getLogger(LedgerStateMachine.class);

    private final AccountStore accounts;
    private final TransferStore transfers;
    private final PendingTransferIndex pending;
    private final AccountTransferIndex acctIndex = new AccountTransferIndex();

    public LedgerStateMachine(AccountStore accounts, TransferStore transfers,
                              PendingTransferIndex pending) {
        this.accounts = accounts;
        this.transfers = transfers;
        this.pending = pending;
    }

    public AccountStore accounts() { return accounts; }
    public TransferStore transfers() { return transfers; }

    /**
     * Rebuild the derived in-memory indexes (pending status + account→transfers)
     * from the transfer store. Called during recovery after a snapshot loads
     * accounts + transfers (whose balances are already correct) but not these
     * indexes. Both are fully derivable from the transfer set:
     * <ul>
     *   <li>Pending: a PENDING transfer is OPEN unless a later POST/VOID references it.</li>
     *   <li>Account index: each transfer is listed under its debit and credit account.</li>
     * </ul>
     * Transfers are processed in timestamp order so the account index reflects
     * chronological history regardless of the order they were loaded in.
     */
    public void rebuildDerivedIndexes() {
        var it = transfers.iterator();
        List<Transfer> all = new ArrayList<>();
        while (it.hasNext()) all.add(it.next());
        all.sort(Comparator.comparingLong(Transfer::timestamp));

        for (Transfer t : all) {
            if (t.isPending()) {
                long expiry = t.timeoutSeconds() > 0
                        ? t.timestamp() + TimeUnit.SECONDS.toNanos(t.timeoutSeconds()) : 0;
                pending.add(t.id(), expiry);
            }
            acctIndex.add(t.debitAccountId(), t.id());
            acctIndex.add(t.creditAccountId(), t.id());
        }
        for (Transfer t : all) {
            if (t.isPostPending()) pending.markPosted(t.pendingId());
            else if (t.isVoidPending()) pending.markVoided(t.pendingId());
        }
    }

    /** Most recent transfers (newest first, capped at {@code limit}) touching an account. */
    public List<Transfer> getAccountTransfers(UInt128 accountId, int limit) {
        UInt128[] ids = acctIndex.recent(accountId, limit);
        List<Transfer> out = new ArrayList<>(ids.length);
        for (UInt128 id : ids) out.add(transfers.load(id));
        return out;
    }

    // ----------------------------------------------------------------------
    // Account creation
    // ----------------------------------------------------------------------

    /**
     * @param baseTimestamp first timestamp of the reserved block; account {@code i}
     *                      is stamped {@code baseTimestamp + i}
     */
    public List<CreateAccountResult> createAccounts(List<Account> batch, long baseTimestamp) {
        List<CreateAccountResult> results = new ArrayList<>(batch.size());
        for (int i = 0; i < batch.size(); i++) {
            results.add(validateAndInsertAccount(batch.get(i), baseTimestamp + i));
        }
        return results;
    }

    private CreateAccountResult validateAndInsertAccount(Account a, long ts) {
        if (a.id().isZero()) return CreateAccountResult.ID_MUST_NOT_BE_ZERO;
        if (a.ledger() == 0) return CreateAccountResult.LEDGER_MUST_NOT_BE_ZERO;
        if (a.code() == 0) return CreateAccountResult.CODE_MUST_NOT_BE_ZERO;

        int debitCreditExclusive = AccountFlags.DEBITS_MUST_NOT_EXCEED_CREDITS
                                 | AccountFlags.CREDITS_MUST_NOT_EXCEED_DEBITS;
        if ((a.flags() & debitCreditExclusive) == debitCreditExclusive) {
            return CreateAccountResult.FLAGS_ARE_MUTUALLY_EXCLUSIVE;
        }

        if (a.debitsPending() != 0) return CreateAccountResult.DEBITS_PENDING_MUST_BE_ZERO;
        if (a.debitsPosted()  != 0) return CreateAccountResult.DEBITS_POSTED_MUST_BE_ZERO;
        if (a.creditsPending() != 0) return CreateAccountResult.CREDITS_PENDING_MUST_BE_ZERO;
        if (a.creditsPosted()  != 0) return CreateAccountResult.CREDITS_POSTED_MUST_BE_ZERO;

        int existingSlot = accounts.slotOf(a.id());
        if (existingSlot != -1) {
            if (accounts.ledger(existingSlot) == a.ledger()
                    && accounts.code(existingSlot) == a.code()
                    && accounts.flags(existingSlot) == a.flags()
                    && accounts.userData64(existingSlot) == a.userData64()
                    && accounts.userData32(existingSlot) == a.userData32()) {
                return CreateAccountResult.EXISTS;
            }
            return CreateAccountResult.EXISTS_WITH_DIFFERENT_FIELDS;
        }

        accounts.insert(a.id(), a.ledger(), a.code(), a.flags(),
                a.userData64(), a.userData32(), ts);
        return CreateAccountResult.OK;
    }

    // ----------------------------------------------------------------------
    // Transfer creation (the centerpiece)
    // ----------------------------------------------------------------------

    /**
     * Process a batch of transfers. Linked transfers are processed as atomic
     * chains -- if any in a chain fails, all of its mutations roll back.
     *
     * @param baseTimestamp first timestamp of the reserved block; transfer {@code i}
     *                      is stamped {@code baseTimestamp + i}
     */
    public List<CreateTransferResult> createTransfers(List<Transfer> batch, long baseTimestamp) {
        List<CreateTransferResult> results = new ArrayList<>(batch.size());
        for (int i = 0; i < batch.size(); i++) results.add(null);

        int i = 0;
        while (i < batch.size()) {
            int chainEnd = i;
            while (chainEnd < batch.size() && batch.get(chainEnd).isLinked()) {
                chainEnd++;
            }
            if (chainEnd >= batch.size()) chainEnd = batch.size() - 1;

            processChain(batch, results, i, chainEnd, baseTimestamp);
            i = chainEnd + 1;
        }
        return results;
    }

    /** Records enough state to undo every mutation a chain makes. */
    private static final class ChainUndo {
        final int transferMark;
        final Int2ObjectOpenHashMap<long[]> balanceUndo = new Int2ObjectOpenHashMap<>();
        final List<PendingUndo> pendingUndo = new ArrayList<>();
        final List<UInt128[]> indexUndo = new ArrayList<>(); // {accountId, transferId}

        ChainUndo(int transferMark) { this.transferMark = transferMark; }
    }

    /** A reversible change to the pending index made within a chain. */
    private record PendingUndo(UInt128 id, boolean added, byte prevStatus, long prevExpiry) {
        static PendingUndo added(UInt128 id) { return new PendingUndo(id, true, (byte) 0, 0); }
        static PendingUndo statusChange(UInt128 id, byte prevStatus, long prevExpiry) {
            return new PendingUndo(id, false, prevStatus, prevExpiry);
        }
    }

    private void processChain(List<Transfer> batch, List<CreateTransferResult> results,
                              int start, int end, long baseTimestamp) {
        ChainUndo undo = new ChainUndo(transfers.size());
        int chainSize = end - start + 1;
        List<CreateTransferResult> chainResults = new ArrayList<>(chainSize);

        for (int i = start; i <= end; i++) {
            Transfer t = batch.get(i);
            CreateTransferResult r = tryApplyTransfer(t, baseTimestamp + i, undo);
            chainResults.add(r);
            if (r != CreateTransferResult.OK && r != CreateTransferResult.EXISTS) {
                rollback(undo);
                for (int j = start; j <= end; j++) {
                    results.set(j, j == i ? r : CreateTransferResult.LINKED_EVENT_FAILED);
                }
                return;
            }
        }
        for (int i = start, k = 0; i <= end; i++, k++) {
            results.set(i, chainResults.get(k));
        }
    }

    private CreateTransferResult tryApplyTransfer(Transfer t, long ts, ChainUndo undo) {
        if (t.id().isZero()) return CreateTransferResult.ID_MUST_NOT_BE_ZERO;
        if (t.debitAccountId().isZero()) return CreateTransferResult.DEBIT_ACCOUNT_ID_MUST_NOT_BE_ZERO;
        if (t.creditAccountId().isZero()) return CreateTransferResult.CREDIT_ACCOUNT_ID_MUST_NOT_BE_ZERO;
        if (t.debitAccountId().equals(t.creditAccountId()))
            return CreateTransferResult.ACCOUNTS_MUST_BE_DIFFERENT;

        boolean isPost = t.isPostPending();
        boolean isVoid = t.isVoidPending();
        boolean isPending = t.isPending();

        int mutuallyExclusive = TransferFlags.PENDING | TransferFlags.POST_PENDING_TRANSFER | TransferFlags.VOID_PENDING_TRANSFER;
        if (Integer.bitCount(t.flags() & mutuallyExclusive) > 1)
            return CreateTransferResult.FLAGS_ARE_MUTUALLY_EXCLUSIVE;

        int existingTslot = transfers.slotOf(t.id());
        if (existingTslot != -1) {
            Transfer existing = transfers.loadFromSlot(existingTslot);
            if (existing.debitAccountId() == t.debitAccountId()
                    && existing.creditAccountId() == t.creditAccountId()
                    && existing.amount() == t.amount()
                    && existing.ledger() == t.ledger()
                    && existing.code() == t.code()
                    && existing.flags() == t.flags()
                    && existing.pendingId() == t.pendingId()) {
                return CreateTransferResult.EXISTS;
            }
            return CreateTransferResult.EXISTS_WITH_DIFFERENT_FIELDS;
        }

        if (isPost || isVoid) {
            return applyPostOrVoid(t, ts, undo);
        }

        if (t.amount() == 0) return CreateTransferResult.AMOUNT_MUST_NOT_BE_ZERO;
        if (t.ledger() == 0) return CreateTransferResult.LEDGER_MUST_NOT_BE_ZERO;
        if (t.code() == 0) return CreateTransferResult.CODE_MUST_NOT_BE_ZERO;

        int debitSlot = accounts.slotOf(t.debitAccountId());
        int creditSlot = accounts.slotOf(t.creditAccountId());
        if (debitSlot == -1) return CreateTransferResult.DEBIT_ACCOUNT_NOT_FOUND;
        if (creditSlot == -1) return CreateTransferResult.CREDIT_ACCOUNT_NOT_FOUND;

        if (accounts.ledger(debitSlot) != accounts.ledger(creditSlot))
            return CreateTransferResult.ACCOUNTS_MUST_HAVE_SAME_LEDGER;
        if (t.ledger() != accounts.ledger(debitSlot))
            return CreateTransferResult.TRANSFER_MUST_HAVE_SAME_LEDGER_AS_ACCOUNTS;

        if (accounts.isClosed(debitSlot)) return CreateTransferResult.DEBIT_ACCOUNT_CLOSED;
        if (accounts.isClosed(creditSlot)) return CreateTransferResult.CREDIT_ACCOUNT_CLOSED;

        if (accounts.wouldExceedDebitLimit(debitSlot, t.amount()))
            return CreateTransferResult.EXCEEDS_DEBITS_LIMIT;
        if (accounts.wouldExceedCreditLimit(creditSlot, t.amount()))
            return CreateTransferResult.EXCEEDS_CREDITS_LIMIT;

        // Overflow guard: u64 counters must not wrap, or per-account balances corrupt.
        if (isPending) {
            if (addOverflows(accounts.debitsPending(debitSlot), t.amount()))
                return CreateTransferResult.OVERFLOWS_DEBITS_PENDING;
            if (addOverflows(accounts.creditsPending(creditSlot), t.amount()))
                return CreateTransferResult.OVERFLOWS_CREDITS_PENDING;
        } else {
            if (addOverflows(accounts.debitsPosted(debitSlot), t.amount()))
                return CreateTransferResult.OVERFLOWS_DEBITS_POSTED;
            if (addOverflows(accounts.creditsPosted(creditSlot), t.amount()))
                return CreateTransferResult.OVERFLOWS_CREDITS_POSTED;
        }

        captureBalance(undo, debitSlot);
        captureBalance(undo, creditSlot);
        if (isPending) {
            accounts.addDebitsPending(debitSlot, t.amount());
            accounts.addCreditsPending(creditSlot, t.amount());
            long expiry = t.timeoutSeconds() > 0 ? ts + TimeUnit.SECONDS.toNanos(t.timeoutSeconds()) : 0;
            pending.add(t.id(), expiry);
            undo.pendingUndo.add(PendingUndo.added(t.id()));
        } else {
            accounts.addDebitsPosted(debitSlot, t.amount());
            accounts.addCreditsPosted(creditSlot, t.amount());
        }

        Transfer stored = withTimestamp(t, ts);
        transfers.insert(stored);
        indexInsert(undo, stored);
        return CreateTransferResult.OK;
    }

    private CreateTransferResult applyPostOrVoid(Transfer t, long ts, ChainUndo undo) {
        if (t.pendingId().isZero()) return CreateTransferResult.PENDING_TRANSFER_NOT_FOUND;

        int origSlot = transfers.slotOf(t.pendingId());
        if (origSlot == -1) return CreateTransferResult.PENDING_TRANSFER_NOT_FOUND;

        Transfer orig = transfers.loadFromSlot(origSlot);
        if (!orig.isPending()) return CreateTransferResult.PENDING_TRANSFER_NOT_PENDING;
        if (!pending.isOpen(orig.id())) return CreateTransferResult.PENDING_TRANSFER_NOT_PENDING;
        if (pending.isExpired(orig.id(), ts)) return CreateTransferResult.PENDING_TRANSFER_EXPIRED;

        boolean isPost = t.isPostPending();
        long amount = t.amount() != 0 ? t.amount() : orig.amount();
        if (isPost && Long.compareUnsigned(amount, orig.amount()) > 0) {
            return CreateTransferResult.EXCEEDS_PENDING_TRANSFER_AMOUNT;
        }

        int debitSlot = accounts.slotOf(orig.debitAccountId());
        int creditSlot = accounts.slotOf(orig.creditAccountId());

        // Overflow guard on the posted counters before we touch any balance.
        if (isPost) {
            if (addOverflows(accounts.debitsPosted(debitSlot), amount))
                return CreateTransferResult.OVERFLOWS_DEBITS_POSTED;
            if (addOverflows(accounts.creditsPosted(creditSlot), amount))
                return CreateTransferResult.OVERFLOWS_CREDITS_POSTED;
        }

        captureBalance(undo, debitSlot);
        captureBalance(undo, creditSlot);
        accounts.addDebitsPending(debitSlot, -orig.amount());
        accounts.addCreditsPending(creditSlot, -orig.amount());

        undo.pendingUndo.add(PendingUndo.statusChange(
                orig.id(), pending.statusOf(orig.id()), pending.expiryOf(orig.id())));
        if (isPost) {
            accounts.addDebitsPosted(debitSlot, amount);
            accounts.addCreditsPosted(creditSlot, amount);
            pending.markPosted(orig.id());
        } else {
            pending.markVoided(orig.id());
        }

        Transfer stored = new Transfer(t.id(),
                orig.debitAccountId(), orig.creditAccountId(),
                amount, orig.id(), t.userData64(), t.userData32(),
                0, orig.ledger(), t.code(), t.flags(), ts);
        transfers.insert(stored);
        indexInsert(undo, stored);
        return CreateTransferResult.OK;
    }

    /** True if {@code current + delta} wraps the unsigned 64-bit range. */
    private static boolean addOverflows(long current, long delta) {
        return Long.compareUnsigned(current + delta, current) < 0;
    }

    /** Record a stored transfer in the account→transfers index (both legs). */
    private void indexInsert(ChainUndo undo, Transfer stored) {
        acctIndex.add(stored.debitAccountId(), stored.id());
        acctIndex.add(stored.creditAccountId(), stored.id());
        undo.indexUndo.add(new UInt128[]{stored.debitAccountId(), stored.id()});
        undo.indexUndo.add(new UInt128[]{stored.creditAccountId(), stored.id()});
    }

    private static Transfer withTimestamp(Transfer t, long ts) {
        return new Transfer(t.id(), t.debitAccountId(), t.creditAccountId(),
                t.amount(), t.pendingId(), t.userData64(), t.userData32(),
                t.timeoutSeconds(), t.ledger(), t.code(), t.flags(), ts);
    }

    // ----------------------------------------------------------------------
    // Rollback for linked chains
    // ----------------------------------------------------------------------

    private void captureBalance(ChainUndo undo, int slot) {
        if (!undo.balanceUndo.containsKey(slot)) {
            undo.balanceUndo.put(slot, new long[]{
                    accounts.debitsPending(slot), accounts.debitsPosted(slot),
                    accounts.creditsPending(slot), accounts.creditsPosted(slot)});
        }
    }

    private void rollback(ChainUndo undo) {
        // Restore touched account balances to their pre-chain values.
        for (var e : undo.balanceUndo.int2ObjectEntrySet()) {
            int slot = e.getIntKey();
            long[] v = e.getValue();
            accounts.addDebitsPending(slot,  v[0] - accounts.debitsPending(slot));
            accounts.addDebitsPosted(slot,   v[1] - accounts.debitsPosted(slot));
            accounts.addCreditsPending(slot, v[2] - accounts.creditsPending(slot));
            accounts.addCreditsPosted(slot,  v[3] - accounts.creditsPosted(slot));
        }
        // Physically drop transfers this chain inserted.
        transfers.truncateTo(undo.transferMark);
        // Undo account-index entries in reverse order (LIFO).
        for (int i = undo.indexUndo.size() - 1; i >= 0; i--) {
            UInt128[] e = undo.indexUndo.get(i);
            acctIndex.remove(e[0], e[1]);
        }
        // Undo pending-index changes in reverse order.
        for (int i = undo.pendingUndo.size() - 1; i >= 0; i--) {
            PendingUndo u = undo.pendingUndo.get(i);
            if (u.added()) {
                pending.remove(u.id());
            } else {
                pending.restore(u.id(), u.prevStatus(), u.prevExpiry());
            }
        }
    }

    // ----------------------------------------------------------------------
    // Lookups (read-only, run on writer thread to serialize)
    // ----------------------------------------------------------------------

    public AccountSnapshot lookupAccount(UInt128 id) {
        int slot = accounts.slotOf(id);
        return slot == -1 ? null : accounts.snapshot(slot);
    }

    public Transfer lookupTransfer(UInt128 id) {
        int slot = transfers.slotOf(id);
        return slot == -1 ? null : transfers.loadFromSlot(slot);
    }

    /**
     * Trial balance per ledger: sums every account's counters. The double-entry
     * invariant guarantees posted debits == posted credits per ledger; this makes
     * that observable for reconciliation. Read-only; runs on the writer thread.
     */
    public List<LedgerBalance> trialBalance() {
        // ledger -> {debitsPosted, creditsPosted, debitsPending, creditsPending, accounts}
        Int2ObjectOpenHashMap<long[]> byLedger = new Int2ObjectOpenHashMap<>();
        var it = accounts.iterator();
        while (it.hasNext()) {
            AccountSnapshot a = it.next();
            long[] sums = byLedger.computeIfAbsent(a.ledger(), k -> new long[5]);
            sums[0] += a.debitsPosted();
            sums[1] += a.creditsPosted();
            sums[2] += a.debitsPending();
            sums[3] += a.creditsPending();
            sums[4] += 1;
        }
        List<LedgerBalance> out = new ArrayList<>(byLedger.size());
        for (var e : byLedger.int2ObjectEntrySet()) {
            long[] s = e.getValue();
            out.add(new LedgerBalance(e.getIntKey(), (int) s[4], s[0], s[1], s[2], s[3]));
        }
        out.sort(Comparator.comparingInt(LedgerBalance::ledger));
        return out;
    }

    /**
     * Deterministically void every OPEN pending transfer whose timeout has passed
     * as of {@code asOfTimestamp}, releasing the funds it reserved. Driven by a
     * journaled EXPIRE command so replay reproduces the same voids at the same
     * logical time. Returns the number of pendings expired.
     */
    public int expirePending(long asOfTimestamp) {
        UInt128[] expired = pending.openExpired(asOfTimestamp);
        for (UInt128 pendingId : expired) {
            int slot = transfers.slotOf(pendingId);
            if (slot == -1) continue;
            Transfer orig = transfers.loadFromSlot(slot);
            int debitSlot = accounts.slotOf(orig.debitAccountId());
            int creditSlot = accounts.slotOf(orig.creditAccountId());
            if (debitSlot != -1) accounts.addDebitsPending(debitSlot, -orig.amount());
            if (creditSlot != -1) accounts.addCreditsPending(creditSlot, -orig.amount());
            pending.markVoided(pendingId);
        }
        return expired.length;
    }
}
