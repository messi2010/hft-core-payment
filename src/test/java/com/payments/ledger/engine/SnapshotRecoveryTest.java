package com.payments.ledger.engine;

import com.payments.ledger.config.EngineConfig;
import com.payments.ledger.domain.Account;
import com.payments.ledger.domain.AccountSnapshot;
import com.payments.ledger.domain.Transfer;
import com.payments.ledger.domain.TransferFlags;
import com.payments.ledger.domain.UInt128;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the checkpoint path: state is snapshotted into the LSM, and on
 * restart recovery loads the snapshot and replays only the journal commands
 * written after it -- including pending transfers whose status is rebuilt from
 * the snapshotted transfer set.
 */
class SnapshotRecoveryTest {

    private static EngineConfig config(Path dir) {
        EngineConfig c = new EngineConfig();
        c.setDataDir(dir.toString());
        c.setRingBufferSize(1024);
        c.setMaxAccounts(100);
        c.setMaxTransfers(100);
        c.getSnapshot().setIntervalSeconds(3600); // don't let the scheduler fire mid-test
        return c;
    }

    private static UInt128 u(long v) { return UInt128.of(v); }

    private static Account acct(long id) {
        return new Account(u(id), 700, (short) 10, (short) 0, 0, 0, 0);
    }

    private static Transfer transfer(long id, long debit, long credit, long amount, long pendingId, int flags) {
        return new Transfer(u(id), u(debit), u(credit), amount, u(pendingId), 0, 0, 0, 700, (short) 1, (short) flags, 0);
    }

    @Test
    void snapshotPlusJournalTailReplayReconstructsState(@TempDir Path dir)
            throws IOException, ExecutionException, InterruptedException {
        LedgerEngine engine = new LedgerEngine(config(dir));
        engine.createAccounts(List.of(acct(1), acct(2))).get();
        engine.createTransfers(List.of(transfer(5, 1, 2, 100, 0, 0))).get();   // posted, pre-snapshot

        engine.checkpoint().get();                                              // snapshot @ debits=100

        // A snapshot file must now exist.
        assertTrue(Files.exists(dir.resolve("snapshot").resolve("meta")));

        engine.createTransfers(List.of(transfer(6, 1, 2, 30, 0, 0))).get();    // post-snapshot, journal-tail only
        assertEquals(130, engine.lookupAccount(u(1)).get().debitsPosted());

        engine.simulateCrash(); // no graceful final checkpoint

        LedgerEngine recovered = new LedgerEngine(config(dir));
        try {
            // 100 from the snapshot + 30 replayed from the journal tail.
            assertEquals(130, recovered.lookupAccount(u(1)).get().debitsPosted());
            assertEquals(130, recovered.lookupAccount(u(2)).get().creditsPosted());
            assertNotNull(recovered.lookupTransfer(u(5)).get());
            assertNotNull(recovered.lookupTransfer(u(6)).get());
        } finally {
            recovered.shutdown();
        }
    }

    @Test
    void pendingCreatedBeforeSnapshotCanBePostedAfterRecovery(@TempDir Path dir)
            throws IOException, ExecutionException, InterruptedException {
        LedgerEngine engine = new LedgerEngine(config(dir));
        engine.createAccounts(List.of(acct(1), acct(2))).get();
        // Pending transfer reserves 50.
        engine.createTransfers(List.of(transfer(7, 1, 2, 50, 0, TransferFlags.PENDING))).get();
        AccountSnapshot beforeSnap = engine.lookupAccount(u(1)).get();
        assertEquals(50, beforeSnap.debitsPending());

        engine.checkpoint().get();                  // snapshot has an OPEN pending
        engine.simulateCrash();

        // Recover: pending status is rebuilt from the snapshotted transfer set.
        LedgerEngine recovered = new LedgerEngine(config(dir));
        try {
            assertEquals(50, recovered.lookupAccount(u(1)).get().debitsPending());
            // Post the snapshot-era pending after recovery.
            recovered.createTransfers(List.of(
                    transfer(8, 1, 2, 50, 7, TransferFlags.POST_PENDING_TRANSFER))).get();
            AccountSnapshot a = recovered.lookupAccount(u(1)).get();
            assertEquals(0, a.debitsPending());
            assertEquals(50, a.debitsPosted());
        } finally {
            recovered.shutdown();
        }
    }
}
