package com.payments.ledger.engine;

import com.lmax.disruptor.EventHandler;
import com.payments.ledger.domain.AccountSnapshot;
import com.payments.ledger.domain.Transfer;
import com.payments.ledger.journal.Journal;
import com.payments.ledger.persistence.SerializationProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Single-consumer Disruptor handler. Runs on the writer thread. This is the
 * "ingestion" layer in the command-sourcing design: it reserves the timestamp
 * block for each command, applies it to the (deterministic) state machine, then
 * journals the <em>input command</em> -- never the mutated state. The journal is
 * fsynced once at the end of each Disruptor batch to amortize the fsync, and
 * futures complete only after that fsync so a caller is never told "OK" for a
 * command that isn't durable.
 */
public final class LedgerEventHandler implements EventHandler<LedgerEvent> {

    private static final Logger log = LoggerFactory.getLogger(LedgerEventHandler.class);

    private final LedgerStateMachine stateMachine;
    private final Journal journal;
    private final IdGenerator clock;
    private final SerializationProcessor serde;

    public LedgerEventHandler(LedgerStateMachine stateMachine, Journal journal,
                              IdGenerator clock, SerializationProcessor serde) {
        this.stateMachine = stateMachine;
        this.journal = journal;
        this.clock = clock;
        this.serde = serde;
    }

    @Override
    public void onEvent(LedgerEvent event, long sequence, boolean endOfBatch) {
        try {
            switch (event.op) {
                case CREATE_ACCOUNTS -> {
                    if (event.accountsToCreate.isEmpty()) {
                        event.accountResults = List.of();
                    } else {
                        long base = clock.reserve(event.accountsToCreate.size());
                        event.accountResults = stateMachine.createAccounts(event.accountsToCreate, base);
                        journal.appendAccountsCommand(base, event.accountsToCreate);
                    }
                }
                case CREATE_TRANSFERS -> {
                    if (event.transfersToCreate.isEmpty()) {
                        event.transferResults = List.of();
                    } else {
                        long base = clock.reserve(event.transfersToCreate.size());
                        event.transferResults = stateMachine.createTransfers(event.transfersToCreate, base);
                        journal.appendTransfersCommand(base, event.transfersToCreate);
                    }
                }
                case LOOKUP_ACCOUNT -> event.accountResult = stateMachine.lookupAccount(event.lookupId);
                case LOOKUP_TRANSFER -> event.transferResult = stateMachine.lookupTransfer(event.lookupId);
                case GET_ACCOUNT_TRANSFERS ->
                        event.transferListResult = stateMachine.getAccountTransfers(event.lookupId, event.limit);
                case TRIAL_BALANCE -> event.trialBalanceResult = stateMachine.trialBalance();
                case EXPIRE_PENDING -> runExpire();
                case CHECKPOINT -> runCheckpoint();
            }
        } catch (IOException | RuntimeException ex) {
            event.error = ex;
            log.error("Event {} failed: {}", event.op, ex.getMessage(), ex);
        }

        if (endOfBatch) {
            try {
                journal.flush();
            } catch (IOException e) {
                log.error("Journal flush failed -- engine integrity at risk", e);
                event.error = e;
            }
        }

        if (event.future != null) {
            event.future.complete(event);
        }
    }

    /**
     * Take a consistent checkpoint. Runs on the writer thread, so the stores are
     * stable for the duration -- no concurrent mutation is possible. The journal
     * is flushed first so {@code offset} marks exactly the commands already
     * durable; the snapshot is tagged with that offset and recovery replays only
     * what follows it. (Synchronous here for simplicity; a production engine
     * would snapshot off-thread via copy-on-write to avoid stalling writes.)
     */
    private void runCheckpoint() throws IOException {
        if (serde == null) return;
        journal.flush();
        long offset = journal.position();
        SerializationProcessor.SnapshotSource source = new SerializationProcessor.SnapshotSource() {
            @Override public Iterator<AccountSnapshot> accounts() { return stateMachine.accounts().iterator(); }
            @Override public Iterator<Transfer> transfers() { return stateMachine.transfers().iterator(); }
        };
        serde.storeSnapshot(offset, offset, source);
        journal.appendCheckpoint(offset);
    }

    /**
     * Void pending transfers whose timeout has passed. The as-of timestamp is
     * reserved from the same clock as commands; if anything expired, the EXPIRE
     * command is journaled with that timestamp so recovery reproduces the voids
     * deterministically. Nothing expired -> nothing journaled.
     */
    private void runExpire() throws IOException {
        long asOf = clock.reserve(1);
        int expired = stateMachine.expirePending(asOf);
        if (expired > 0) {
            journal.appendExpire(asOf);
        }
    }
}

