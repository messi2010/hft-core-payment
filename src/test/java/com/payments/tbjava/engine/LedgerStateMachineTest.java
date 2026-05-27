package com.payments.tbjava.engine;

import com.payments.tbjava.domain.Account;
import com.payments.tbjava.domain.CreateTransferResult;
import com.payments.tbjava.domain.Transfer;
import com.payments.tbjava.domain.TransferFlags;
import com.payments.tbjava.domain.UInt128;
import com.payments.tbjava.storage.AccountStore;
import com.payments.tbjava.storage.PendingTransferIndex;
import com.payments.tbjava.storage.TransferStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class LedgerStateMachineTest {

    private AccountStore accounts;
    private TransferStore transfers;
    private PendingTransferIndex pending;
    private LedgerStateMachine sm;

    private static UInt128 u(long v) { return UInt128.of(v); }

    private static Account acct(long id) {
        return new Account(u(id), 700, (short) 10, (short) 0, 0, 0, 0);
    }

    private static Transfer transfer(long id, long debit, long credit, long amount, int flags) {
        return new Transfer(u(id), u(debit), u(credit), amount, UInt128.ZERO, 0, 0, 0, 700, (short) 1, (short) flags, 0);
    }

    @BeforeEach
    void setUp() {
        accounts = new AccountStore(100);
        transfers = new TransferStore(100);
        pending = new PendingTransferIndex();
        sm = new LedgerStateMachine(accounts, transfers, pending);
        sm.createAccounts(List.of(acct(1), acct(2)), 1000);
    }

    @Test
    void simpleTransferPostsBalancesDeterministically() {
        List<CreateTransferResult> r = sm.createTransfers(
                List.of(transfer(5, 1, 2, 100, (short) 0)), 2000);
        assertEquals(List.of(CreateTransferResult.OK), r);
        assertEquals(100, sm.lookupAccount(u(1)).debitsPosted());
        assertEquals(100, sm.lookupAccount(u(2)).creditsPosted());
        // ts = base + index
        assertEquals(2000, sm.lookupTransfer(u(5)).timestamp());
    }

    @Test
    void idempotentRetryOfIdenticalTransferReturnsExists() {
        // A transfer re-submitted with the same id and identical fields must be
        // recognized as a duplicate (EXISTS), not EXISTS_WITH_DIFFERENT_FIELDS.
        Transfer t = transfer(5, 1, 2, 100, (short) 0);
        assertEquals(List.of(CreateTransferResult.OK), sm.createTransfers(List.of(t), 2000));
        assertEquals(List.of(CreateTransferResult.EXISTS), sm.createTransfers(List.of(t), 3000));
        // The balance must not have been applied twice.
        assertEquals(100, sm.lookupAccount(u(1)).debitsPosted());
    }

    @Test
    void failedLinkedChainRollsBackEverything() {
        // Chain: transfer 10 (linked, valid) -> transfer 11 (terminator, invalid: credit acct missing).
        Transfer a = transfer(10, 1, 2, 100, TransferFlags.LINKED);
        Transfer b = transfer(11, 1, 999, 50, (short) 0);

        List<CreateTransferResult> r = sm.createTransfers(List.of(a, b), 2000);

        assertEquals(CreateTransferResult.LINKED_EVENT_FAILED, r.get(0));
        assertEquals(CreateTransferResult.CREDIT_ACCOUNT_NOT_FOUND, r.get(1));

        // The valid first leg must be fully undone: not stored, balances untouched.
        assertNull(sm.lookupTransfer(u(10)));
        assertFalse(transfers.exists(u(10)));
        assertEquals(0, transfers.size());
        assertEquals(0, sm.lookupAccount(u(1)).debitsPosted());
        assertEquals(0, sm.lookupAccount(u(2)).creditsPosted());
    }

    @Test
    void failedChainWithPendingLegRemovesPendingIndexEntry() {
        // Chain: pending transfer 20 (linked) -> transfer 21 (terminator, invalid).
        Transfer pendingLeg = transfer(20, 1, 2, 100, TransferFlags.LINKED | TransferFlags.PENDING);
        Transfer bad = transfer(21, 1, 1, 50, (short) 0); // same debit/credit -> invalid

        List<CreateTransferResult> r = sm.createTransfers(List.of(pendingLeg, bad), 2000);

        assertEquals(CreateTransferResult.LINKED_EVENT_FAILED, r.get(0));
        assertEquals(CreateTransferResult.ACCOUNTS_MUST_BE_DIFFERENT, r.get(1));

        assertFalse(pending.exists(u(20)));
        assertFalse(transfers.exists(u(20)));
        assertEquals(0, sm.lookupAccount(u(1)).debitsPending());
        assertEquals(0, sm.lookupAccount(u(2)).creditsPending());
    }

    @Test
    void postingThatOverflowsIsRejectedAndLeavesBalanceIntact() {
        // Seed account 1 with debitsPosted at the unsigned u64 max.
        AccountStore seeded = new AccountStore(10);
        seeded.restoreEntry(u(1), 700, (short) 10, (short) 0, 0, 0, 1,
                0, -1L /* u64 max */, 0, 0);
        seeded.restoreEntry(u(2), 700, (short) 10, (short) 0, 0, 0, 1, 0, 0, 0, 0);
        LedgerStateMachine sm2 = new LedgerStateMachine(seeded, new TransferStore(10), new PendingTransferIndex());

        List<CreateTransferResult> r = sm2.createTransfers(
                List.of(transfer(5, 1, 2, 1, (short) 0)), 2000);

        assertEquals(CreateTransferResult.OVERFLOWS_DEBITS_POSTED, r.get(0));
        assertEquals(-1L, sm2.lookupAccount(u(1)).debitsPosted()); // unchanged, did not wrap to 0
        assertEquals(0, sm2.lookupAccount(u(2)).creditsPosted());
    }

    @Test
    void expirePendingVoidsTimedOutReservationsAndReleasesFunds() {
        // Pending transfer at ts=2000 with a 1s timeout -> expiry = 2000 + 1e9.
        Transfer pendingT = new Transfer(u(20), u(1), u(2), 50, UInt128.ZERO, 0, 0,
                1 /* timeoutSeconds */, 700, (short) 1, (short) TransferFlags.PENDING, 0);
        sm.createTransfers(List.of(pendingT), 2000);
        assertEquals(50, sm.lookupAccount(u(1)).debitsPending());

        // Before expiry: nothing happens.
        assertEquals(0, sm.expirePending(2000 + 500_000_000L));
        assertEquals(50, sm.lookupAccount(u(1)).debitsPending());

        // At/after expiry: voided, funds released.
        assertEquals(1, sm.expirePending(2000 + 1_000_000_000L));
        assertEquals(0, sm.lookupAccount(u(1)).debitsPending());
        assertEquals(0, sm.lookupAccount(u(2)).creditsPending());
    }

    @Test
    void rolledBackTransferIdCanBeReused() {
        // After a chain rolls back transfer id 10, the same id must be insertable again.
        sm.createTransfers(List.of(
                transfer(10, 1, 2, 100, TransferFlags.LINKED),
                transfer(11, 1, 999, 50, (short) 0)), 2000);
        assertFalse(transfers.exists(u(10)));

        List<CreateTransferResult> r = sm.createTransfers(
                List.of(transfer(10, 1, 2, 30, (short) 0)), 3000);
        assertEquals(List.of(CreateTransferResult.OK), r);
        assertEquals(30, sm.lookupAccount(u(1)).debitsPosted());
    }
}
