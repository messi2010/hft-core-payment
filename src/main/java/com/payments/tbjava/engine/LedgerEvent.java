package com.payments.tbjava.engine;

import com.payments.tbjava.domain.Account;
import com.payments.tbjava.domain.AccountSnapshot;
import com.payments.tbjava.domain.CreateAccountResult;
import com.payments.tbjava.domain.CreateTransferResult;
import com.payments.tbjava.domain.LedgerBalance;
import com.payments.tbjava.domain.Transfer;
import com.payments.tbjava.domain.UInt128;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Pre-allocated event passed through the Disruptor ring buffer. One instance
 * per slot; fields are recycled across requests. Producers fill in {@code op}
 * and the corresponding payload, then publish; the consumer fills in the
 * result and completes {@code future}.
 *
 * <p>This is the only place where API threads and the writer thread share
 * state -- and the ring buffer protocol (sequence number CAS) ensures safe
 * handoff without locks.
 */
public final class LedgerEvent {

    public enum Op {
        CREATE_ACCOUNTS,
        CREATE_TRANSFERS,
        LOOKUP_ACCOUNT,
        LOOKUP_TRANSFER,
        GET_ACCOUNT_TRANSFERS,
        TRIAL_BALANCE,
        EXPIRE_PENDING,
        CHECKPOINT,
    }

    public Op op;

    // Inputs (populated by producer)
    public List<Account> accountsToCreate;
    public List<Transfer> transfersToCreate;
    public UInt128 lookupId;
    public int limit;

    // Outputs (populated by consumer)
    public List<CreateAccountResult> accountResults;
    public List<CreateTransferResult> transferResults;
    public AccountSnapshot accountResult;
    public Transfer transferResult;
    public List<Transfer> transferListResult;
    public List<LedgerBalance> trialBalanceResult;
    public Throwable error;

    // Completion handle for the API thread to await
    public CompletableFuture<LedgerEvent> future;

    /** Clear all references so the GC can reclaim payloads after consumption. */
    public void reset() {
        op = null;
        accountsToCreate = null;
        transfersToCreate = null;
        lookupId = null;
        limit = 0;
        accountResults = null;
        transferResults = null;
        accountResult = null;
        transferResult = null;
        transferListResult = null;
        trialBalanceResult = null;
        error = null;
        future = null;
    }
}
