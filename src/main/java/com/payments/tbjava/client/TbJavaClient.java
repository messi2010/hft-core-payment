package com.payments.tbjava.client;

import com.payments.tbjava.domain.Account;
import com.payments.tbjava.domain.AccountSnapshot;
import com.payments.tbjava.domain.CreateAccountResult;
import com.payments.tbjava.domain.CreateTransferResult;
import com.payments.tbjava.domain.Transfer;
import com.payments.tbjava.domain.UInt128;
import com.payments.tbjava.engine.LedgerEngine;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Thin client API for in-process callers (e.g. saga orchestrator running in
 * the same JVM). For out-of-process callers, use the HTTP API in
 * {@code TbController} or write a remote client over your protocol of choice.
 */
public final class TbJavaClient {

    private final LedgerEngine engine;

    public TbJavaClient(LedgerEngine engine) {
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
                ledger, code, (short) com.payments.tbjava.domain.TransferFlags.PENDING, 0);
    }
}
