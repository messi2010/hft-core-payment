package com.payments.tbjava.engine;

import com.payments.tbjava.config.EngineConfig;
import com.payments.tbjava.domain.Account;
import com.payments.tbjava.domain.LedgerBalance;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

class LedgerHardeningTest {

    private static EngineConfig config(Path dir) {
        EngineConfig c = new EngineConfig();
        c.setDataDir(dir.toString());
        c.setRingBufferSize(1024);
        c.setMaxAccounts(100);
        c.setMaxTransfers(100);
        c.getSnapshot().setIntervalSeconds(3600);
        c.setExpirySweepSeconds(3600); // don't let the background sweeper fire mid-test
        return c;
    }

    private static UInt128 u(long v) { return UInt128.of(v); }

    private static Account acct(long id, int ledger) {
        return new Account(u(id), ledger, (short) 10, (short) 0, 0, 0, 0);
    }

    private static Transfer std(long id, long debit, long credit, long amount, int ledger) {
        return new Transfer(u(id), u(debit), u(credit), amount, UInt128.ZERO, 0, 0, 0, ledger, (short) 1, (short) 0, 0);
    }

    @Test
    void trialBalanceIsBalancedPerLedger(@TempDir Path dir)
            throws IOException, ExecutionException, InterruptedException {
        LedgerEngine engine = new LedgerEngine(config(dir));
        try {
            engine.createAccounts(List.of(acct(1, 700), acct(2, 700), acct(3, 800), acct(4, 800))).get();
            engine.createTransfers(List.of(std(5, 1, 2, 1000, 700))).get();
            engine.createTransfers(List.of(std(6, 3, 4, 250, 800))).get();

            List<LedgerBalance> tb = engine.trialBalance().get();
            assertEquals(2, tb.size());
            for (LedgerBalance lb : tb) {
                assertTrue(lb.balanced(), "ledger " + lb.ledger() + " not balanced");
                assertEquals(lb.debitsPosted(), lb.creditsPosted());
            }
            LedgerBalance l700 = tb.stream().filter(b -> b.ledger() == 700).findFirst().orElseThrow();
            assertEquals(1000, l700.debitsPosted());
            assertEquals(1000, l700.creditsPosted());
            assertEquals(2, l700.accounts());
        } finally {
            engine.shutdown();
        }
    }

    @Test
    void expiredPendingIsVoidedAndSurvivesRecovery(@TempDir Path dir)
            throws IOException, ExecutionException, InterruptedException {
        LedgerEngine engine = new LedgerEngine(config(dir));
        engine.createAccounts(List.of(acct(1, 700), acct(2, 700))).get();
        // Pending with a 1s timeout reserves 50.
        Transfer pending = new Transfer(u(7), u(1), u(2), 50, UInt128.ZERO, 0, 0, 1, 700, (short) 1,
                (short) TransferFlags.PENDING, 0);
        engine.createTransfers(List.of(pending)).get();
        assertEquals(50, engine.lookupAccount(u(1)).get().debitsPending());

        // Let the timeout pass, then sweep (journals an EXPIRE command).
        Thread.sleep(1200);
        engine.expirePending().get();
        assertEquals(0, engine.lookupAccount(u(1)).get().debitsPending());
        engine.simulateCrash();

        // The EXPIRE command must replay deterministically: funds stay released.
        LedgerEngine recovered = new LedgerEngine(config(dir));
        try {
            assertEquals(0, recovered.lookupAccount(u(1)).get().debitsPending());
            assertEquals(0, recovered.lookupAccount(u(2)).get().creditsPending());
        } finally {
            recovered.shutdown();
        }
    }
}
