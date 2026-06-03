package com.payments.ledger.engine;

import com.payments.ledger.domain.UInt128;
import com.payments.ledger.journal.Journal;
import com.payments.ledger.storage.AccountStore;
import com.payments.ledger.storage.PendingTransferIndex;
import com.payments.ledger.storage.TransferStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class LedgerEventHandlerDurabilityTest {

    @TempDir
    Path tempDir;

    @Test
    void futuresInBatch_completedOnlyAfterEndOfBatchFlush() throws IOException {
        AccountStore accounts = new AccountStore(10);
        TransferStore transfers = new TransferStore(10);
        PendingTransferIndex pending = new PendingTransferIndex();
        LedgerStateMachine sm = new LedgerStateMachine(accounts, transfers, pending);
        Journal journal = new Journal(tempDir.resolve("journal.log"), 64 * 1024);
        IdGenerator clock = new IdGenerator();

        // serde=null is safe: runCheckpoint() guards for it and we don't trigger it here
        LedgerEventHandler handler = new LedgerEventHandler(sm, journal, clock, null);

        // First event in a batch (endOfBatch=false)
        LedgerEvent e1 = new LedgerEvent();
        e1.op = LedgerEvent.Op.LOOKUP_ACCOUNT;
        e1.lookupId = UInt128.of(1);
        e1.future = new CompletableFuture<>();

        handler.onEvent(e1, 0L, false);

        // After the first event, future must NOT be done (flush hasn't happened yet)
        assertThat(e1.future.isDone())
                .as("e1 future must NOT complete before endOfBatch flush")
                .isFalse();

        // Second event, last in batch (endOfBatch=true) — triggers flush then completion
        LedgerEvent e2 = new LedgerEvent();
        e2.op = LedgerEvent.Op.LOOKUP_ACCOUNT;
        e2.lookupId = UInt128.of(2);
        e2.future = new CompletableFuture<>();

        handler.onEvent(e2, 1L, true);

        // After endOfBatch, both futures must be done
        assertThat(e1.future.isDone()).as("e1 future must complete after endOfBatch").isTrue();
        assertThat(e2.future.isDone()).as("e2 future must complete after endOfBatch").isTrue();

        journal.close();
    }

    @Test
    void singleEventBatch_completedAfterFlush() throws IOException {
        AccountStore accounts = new AccountStore(10);
        TransferStore transfers = new TransferStore(10);
        PendingTransferIndex pending = new PendingTransferIndex();
        LedgerStateMachine sm = new LedgerStateMachine(accounts, transfers, pending);
        Journal journal = new Journal(tempDir.resolve("journal2.log"), 64 * 1024);
        IdGenerator clock = new IdGenerator();

        LedgerEventHandler handler = new LedgerEventHandler(sm, journal, clock, null);

        LedgerEvent e = new LedgerEvent();
        e.op = LedgerEvent.Op.LOOKUP_ACCOUNT;
        e.lookupId = UInt128.of(99);
        e.future = new CompletableFuture<>();

        // A single event IS the last in its batch
        handler.onEvent(e, 0L, true);

        assertThat(e.future.isDone()).isTrue();

        journal.close();
    }
}
