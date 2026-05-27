package com.payments.tbjava.engine;

import com.payments.tbjava.config.EngineConfig;
import com.payments.tbjava.domain.Account;
import com.payments.tbjava.domain.CreateTransferResult;
import com.payments.tbjava.domain.Transfer;
import com.payments.tbjava.domain.TransferFlags;
import com.payments.tbjava.domain.UInt128;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * End-to-end recovery test. The key assertion is that a failed linked chain,
 * after rollback, does NOT reappear when the journal is replayed on restart --
 * the divergence that per-record state journaling used to cause.
 */
class LedgerEngineRecoveryTest {

    private static EngineConfig config(Path dir) {
        EngineConfig c = new EngineConfig();
        c.setDataDir(dir.toString());
        c.setRingBufferSize(1024);
        c.setMaxAccounts(100);
        c.setMaxTransfers(100);
        return c;
    }

    private static UInt128 u(long v) { return UInt128.of(v); }

    private static Account acct(long id) {
        return new Account(u(id), 700, (short) 10, (short) 0, 0, 0, 0);
    }

    private static Transfer transfer(long id, long debit, long credit, long amount, int flags) {
        return new Transfer(u(id), u(debit), u(credit), amount, UInt128.ZERO, 0, 0, 0, 700, (short) 1, (short) flags, 0);
    }

    @Test
    void stateSurvivesRestartAndFailedLinkedChainIsNotResurrected(@TempDir Path dir)
            throws IOException, ExecutionException, InterruptedException {
        LedgerEngine engine = new LedgerEngine(config(dir));
        try {
            engine.createAccounts(List.of(acct(1), acct(2))).get();

            // A committed transfer.
            assertEquals(List.of(CreateTransferResult.OK),
                    engine.createTransfers(List.of(transfer(5, 1, 2, 100, (short) 0))).get());

            // A linked chain that must fail and roll back fully.
            List<CreateTransferResult> chain = engine.createTransfers(List.of(
                    transfer(6, 1, 2, 50, TransferFlags.LINKED),
                    transfer(7, 1, 999, 10, (short) 0))).get();
            assertEquals(CreateTransferResult.LINKED_EVENT_FAILED, chain.get(0));
            assertEquals(CreateTransferResult.CREDIT_ACCOUNT_NOT_FOUND, chain.get(1));

            assertEquals(100, engine.lookupAccount(u(1)).get().debitsPosted());
            assertNull(engine.lookupTransfer(u(6)).get());
        } finally {
            engine.shutdown();
        }

        // Restart: replay the journal and assert identical state.
        LedgerEngine recovered = new LedgerEngine(config(dir));
        try {
            assertEquals(100, recovered.lookupAccount(u(1)).get().debitsPosted());
            assertEquals(100, recovered.lookupAccount(u(2)).get().creditsPosted());
            assertNotNull(recovered.lookupTransfer(u(5)).get());
            // The rolled-back linked-chain transfer must NOT come back to life.
            assertNull(recovered.lookupTransfer(u(6)).get());
            assertNull(recovered.lookupTransfer(u(7)).get());
        } finally {
            recovered.shutdown();
        }
    }
}
