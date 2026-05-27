package com.payments.ledger.client;

import com.payments.ledger.domain.Account;
import com.payments.ledger.domain.AccountSnapshot;
import com.payments.ledger.domain.CreateAccountResult;
import com.payments.ledger.domain.CreateTransferResult;
import com.payments.ledger.domain.Transfer;
import com.payments.ledger.domain.UInt128;
import com.payments.ledger.engine.LedgerEngine;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Thin client API for in-process callers (e.g. saga orchestrator running in
 * the same JVM). For out-of-process callers, use the HTTP API in
 * {@code LedgerController} or write a remote client over your protocol of choice.
 */
public final class LedgerClient {

    private final LedgerEngine engine;

    public LedgerClient(LedgerEngine engine) {
        this.engine = engine;
    }

    public CompletableFuture<List<CreateAccountResult>> createAccounts(List<Account> accounts) {
        return engine.createAccounts(accounts);
    }

    public CompletableFuture<List<CreateTransferResult>> createTransfers(List<Transfer> transfers) {
        return engine.createTransfers(transfers);
    }

    public CompletableFuture<AccountSnapshot> lookupAccount(UInt128 accountId) {
        return engine.lookupAccount(accountId);
    }

    public CompletableFuture<Transfer> lookupTransfer(UInt128 transferId) {
        return engine.lookupTransfer(transferId);
    }

    /** Convenience: build a one-shot transfer. */
    public static Transfer transfer(UInt128 id, UInt128 debit, UInt128 credit, long amount,
                                    int ledger, short code) {
        return new Transfer(id, debit, credit, amount, UInt128.ZERO, 0, 0, 0, ledger, code, (short) 0, 0);
    }

    /** Convenience: build a pending (2-phase) transfer with timeout. */
    public static Transfer pending(UInt128 id, UInt128 debit, UInt128 credit, long amount,
                                   int ledger, short code, int timeoutSeconds) {
        return new Transfer(id, debit, credit, amount, UInt128.ZERO, 0, 0, timeoutSeconds,
                ledger, code, (short) com.payments.ledger.domain.TransferFlags.PENDING, 0);
    }
}
