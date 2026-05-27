package com.payments.tbjava.engine;

import com.payments.tbjava.config.EngineConfig;
import com.payments.tbjava.domain.Account;
import com.payments.tbjava.domain.Transfer;
import com.payments.tbjava.domain.TransferFlags;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GetAccountTransfersTest {

    private static EngineConfig config(Path dir) {
        EngineConfig c = new EngineConfig();
        c.setDataDir(dir.toString());
        c.setRingBufferSize(1024);
        c.setMaxAccounts(100);
        c.setMaxTransfers(100);
        c.getSnapshot().setIntervalSeconds(3600);
        return c;
    }

    private static Account acct(long id) {
        return new Account(id, 700, (short) 10, (short) 0, 0, 0, 0);
    }

    private static Transfer transfer(long id, long debit, long credit, long amount, int flags) {
        return new Transfer(id, debit, credit, amount, 0, 0, 0, 0, 700, (short) 1, (short) flags, 0);
    }

    private static long[] ids(List<Transfer> transfers) {
        return transfers.stream().mapToLong(Transfer::id).toArray();
    }

    @Test
    void historyIsNewestFirstAndRespectsLimit(@TempDir Path dir)
            throws IOException, ExecutionException, InterruptedException {
        LedgerEngine engine = new LedgerEngine(config(dir));
        try {
            engine.createAccounts(List.of(acct(1), acct(2), acct(3))).get();
            engine.createTransfers(List.of(transfer(5, 1, 2, 100, 0))).get();
            engine.createTransfers(List.of(transfer(6, 1, 3, 50, 0))).get();
            engine.createTransfers(List.of(transfer(7, 2, 1, 20, 0))).get();

            // Account 1 is touched by all three (debit on 5,6; credit on 7). Newest first.
            assertEquals(List.of(7L, 6L, 5L),
                    List.of(box(engine.getAccountTransfers(1, 10).get())));
            // Limit caps to the most recent.
            assertEquals(List.of(7L, 6L),
                    List.of(box(engine.getAccountTransfers(1, 2).get())));
            // Account 3 only appears in transfer 6.
            assertEquals(List.of(6L),
                    List.of(box(engine.getAccountTransfers(3, 10).get())));
        } finally {
            engine.shutdown();
        }
    }

    @Test
    void rolledBackTransferDoesNotAppearInHistory(@TempDir Path dir)
            throws IOException, ExecutionException, InterruptedException {
        LedgerEngine engine = new LedgerEngine(config(dir));
        try {
            engine.createAccounts(List.of(acct(1), acct(2))).get();
            engine.createTransfers(List.of(transfer(5, 1, 2, 100, 0))).get();
            // Linked chain that fails: transfer 6 must be rolled back out of the index too.
            engine.createTransfers(List.of(
                    transfer(6, 1, 2, 50, TransferFlags.LINKED),
                    transfer(7, 1, 999, 10, 0))).get();

            assertEquals(1, engine.getAccountTransfers(1, 10).get().size());
            assertEquals(5L, engine.getAccountTransfers(1, 10).get().get(0).id());
        } finally {
            engine.shutdown();
        }
    }

    @Test
    void historySurvivesRecovery(@TempDir Path dir)
            throws IOException, ExecutionException, InterruptedException {
        LedgerEngine engine = new LedgerEngine(config(dir));
        engine.createAccounts(List.of(acct(1), acct(2))).get();
        engine.createTransfers(List.of(transfer(5, 1, 2, 100, 0))).get();
        engine.checkpoint().get();                                   // 5 goes into the snapshot
        engine.createTransfers(List.of(transfer(6, 1, 2, 50, 0))).get(); // 6 into the journal tail
        engine.simulateCrash();

        LedgerEngine recovered = new LedgerEngine(config(dir));
        try {
            // Index rebuilt from snapshot (5) + replayed tail (6), newest first.
            assertEquals(List.of(6L, 5L),
                    List.of(box(recovered.getAccountTransfers(1, 10).get())));
        } finally {
            recovered.shutdown();
        }
    }

    private static Long[] box(List<Transfer> transfers) {
        long[] a = ids(transfers);
        Long[] out = new Long[a.length];
        for (int i = 0; i < a.length; i++) out[i] = a[i];
        return out;
    }
}
