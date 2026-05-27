package com.payments.tbjava.engine;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.payments.tbjava.config.EngineConfig;
import com.payments.tbjava.domain.Account;
import com.payments.tbjava.domain.AccountSnapshot;
import com.payments.tbjava.domain.CreateAccountResult;
import com.payments.tbjava.domain.CreateTransferResult;
import com.payments.tbjava.domain.LedgerBalance;
import com.payments.tbjava.domain.Transfer;
import com.payments.tbjava.domain.UInt128;
import com.payments.tbjava.journal.Journal;
import com.payments.tbjava.journal.JournalReader;
import com.payments.tbjava.lsm.Lsm;
import com.payments.tbjava.persistence.LsmSerializationProcessor;
import com.payments.tbjava.persistence.SerializationProcessor;
import com.payments.tbjava.storage.AccountStore;
import com.payments.tbjava.storage.PendingTransferIndex;
import com.payments.tbjava.storage.TransferStore;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Public entry point. Wires up the Disruptor, state machine, and journal.
 * Callers (HTTP controller, in-process clients) submit batches through here
 * and await results via {@link CompletableFuture}.
 */
@Component
public final class LedgerEngine {

    private static final Logger log = LoggerFactory.getLogger(LedgerEngine.class);

    private final Disruptor<LedgerEvent> disruptor;
    private final RingBuffer<LedgerEvent> ringBuffer;
    private final Journal journal;
    private final LedgerStateMachine stateMachine;
    private final SerializationProcessor serde;
    private final ScheduledExecutorService checkpointScheduler;

    public LedgerEngine(EngineConfig config) throws IOException {
        Path dataDir = Path.of(config.getDataDir());
        Path journalPath = dataDir.resolve("journal.log");

        AccountStore accounts = new AccountStore(config.getMaxAccounts());
        TransferStore transfers = new TransferStore(config.getMaxTransfers());
        PendingTransferIndex pending = new PendingTransferIndex();
        IdGenerator clock = new IdGenerator();
        this.stateMachine = new LedgerStateMachine(accounts, transfers, pending);
        this.serde = new LsmSerializationProcessor(dataDir.resolve("snapshot"), Lsm.Config.defaults());

        // --- Recovery: load latest snapshot, then replay only the journal tail ---
        recover(journalPath, accounts, transfers, clock);

        this.journal = new Journal(journalPath, 64 * 1024 * 1024);  // 64 MiB write buffer

        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "tbjava-writer");
            t.setDaemon(false);
            return t;
        };

        // BlockingWaitStrategy: writer blocks on no events; trade µs latency for CPU savings.
        // For lower latency, switch to BusySpinWaitStrategy at the cost of pinning 1 core.
        this.disruptor = new Disruptor<>(
                LedgerEvent::new,
                config.getRingBufferSize(),
                tf,
                ProducerType.MULTI,
                new BlockingWaitStrategy()
        );
        this.disruptor.handleEventsWith(new LedgerEventHandler(stateMachine, journal, clock, serde));
        this.ringBuffer = this.disruptor.start();

        int intervalSeconds = config.getSnapshot().getIntervalSeconds();
        this.checkpointScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "tbjava-checkpoint");
            t.setDaemon(true);
            return t;
        });
        this.checkpointScheduler.scheduleWithFixedDelay(this::checkpointQuietly,
                intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        int expirySeconds = config.getExpirySweepSeconds();
        this.checkpointScheduler.scheduleWithFixedDelay(this::expireQuietly,
                expirySeconds, expirySeconds, TimeUnit.SECONDS);

        log.info("LedgerEngine ready: accounts={} transfers={} ringBufferSize={} checkpointInterval={}s",
                accounts.size(), transfers.size(), config.getRingBufferSize(), intervalSeconds);
    }

    // ----------------------------------------------------------------------
    // Public API -- callers should treat these as remote-ish calls.
    // ----------------------------------------------------------------------

    public CompletableFuture<List<CreateAccountResult>> createAccounts(List<Account> accounts) {
        CompletableFuture<LedgerEvent> fut = publish(e -> {
            e.op = LedgerEvent.Op.CREATE_ACCOUNTS;
            e.accountsToCreate = accounts;
        });
        return fut.thenApply(e -> {
            if (e.error != null) throw new RuntimeException(e.error);
            return e.accountResults;
        });
    }

    public CompletableFuture<List<CreateTransferResult>> createTransfers(List<Transfer> transfers) {
        CompletableFuture<LedgerEvent> fut = publish(e -> {
            e.op = LedgerEvent.Op.CREATE_TRANSFERS;
            e.transfersToCreate = transfers;
        });
        return fut.thenApply(e -> {
            if (e.error != null) throw new RuntimeException(e.error);
            return e.transferResults;
        });
    }

    public CompletableFuture<AccountSnapshot> lookupAccount(UInt128 id) {
        return publish(e -> { e.op = LedgerEvent.Op.LOOKUP_ACCOUNT; e.lookupId = id; })
                .thenApply(e -> e.accountResult);
    }

    public CompletableFuture<Transfer> lookupTransfer(UInt128 id) {
        return publish(e -> { e.op = LedgerEvent.Op.LOOKUP_TRANSFER; e.lookupId = id; })
                .thenApply(e -> e.transferResult);
    }

    /** Most recent transfers (newest first, capped at {@code limit}) touching an account. */
    public CompletableFuture<List<Transfer>> getAccountTransfers(UInt128 accountId, int limit) {
        return publish(e -> {
            e.op = LedgerEvent.Op.GET_ACCOUNT_TRANSFERS;
            e.lookupId = accountId;
            e.limit = limit;
        }).thenApply(e -> e.transferListResult);
    }

    /**
     * Take a consistent checkpoint now (snapshot current state + record the
     * journal offset to replay from). Completes after the snapshot is durable.
     */
    public CompletableFuture<Void> checkpoint() {
        return publish(e -> e.op = LedgerEvent.Op.CHECKPOINT)
                .thenApply(e -> {
                    if (e.error != null) throw new RuntimeException(e.error);
                    return null;
                });
    }

    private void checkpointQuietly() {
        try {
            checkpoint().get();
        } catch (Exception e) {
            log.warn("Scheduled checkpoint failed", e);
        }
    }

    /** Trial balance per ledger (posted debits should equal posted credits). */
    public CompletableFuture<List<LedgerBalance>> trialBalance() {
        return publish(e -> e.op = LedgerEvent.Op.TRIAL_BALANCE)
                .thenApply(e -> {
                    if (e.error != null) throw new RuntimeException(e.error);
                    return e.trialBalanceResult;
                });
    }

    /** Void all pending transfers whose timeout has passed. */
    public CompletableFuture<Void> expirePending() {
        return publish(e -> e.op = LedgerEvent.Op.EXPIRE_PENDING)
                .thenApply(e -> {
                    if (e.error != null) throw new RuntimeException(e.error);
                    return null;
                });
    }

    private void expireQuietly() {
        try {
            expirePending().get();
        } catch (Exception e) {
            log.warn("Scheduled pending-expiry sweep failed", e);
        }
    }

    /** Synchronous variant for callers that don't need async. Blocks the API thread. */
    public List<CreateTransferResult> createTransfersSync(List<Transfer> transfers) {
        try {
            return createTransfers(transfers).get();
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    // ----------------------------------------------------------------------
    // Internal
    // ----------------------------------------------------------------------

    @FunctionalInterface
    private interface EventFiller {
        void fill(LedgerEvent e);
    }

    private CompletableFuture<LedgerEvent> publish(EventFiller filler) {
        CompletableFuture<LedgerEvent> future = new CompletableFuture<>();
        long sequence;
        try {
            // Non-blocking claim: never park the caller (the API runs on a Netty
            // event loop). A full ring means the writer is behind -> fail fast.
            sequence = ringBuffer.tryNext();
        } catch (InsufficientCapacityException e) {
            future.completeExceptionally(new CapacityExceededException());
            return future;
        }
        try {
            LedgerEvent event = ringBuffer.get(sequence);
            event.reset();
            filler.fill(event);
            event.future = future;
        } finally {
            ringBuffer.publish(sequence);
        }
        return future;
    }

    /**
     * Recovery: load the latest snapshot into the stores, rebuild the pending
     * index from the loaded transfers, then replay only the journal commands
     * written after the snapshot's offset. With no snapshot, this degrades to a
     * full replay from offset 0.
     */
    private void recover(Path journalPath, AccountStore accounts, TransferStore transfers,
                         IdGenerator clock) throws IOException {
        long[] maxTimestamp = new long[1];

        long replayFrom = serde.loadLatestSnapshot(new SerializationProcessor.SnapshotSink() {
            @Override public void account(AccountSnapshot a) {
                accounts.restoreEntry(a.id(), a.ledger(), a.code(), a.flags(),
                        a.userData64(), a.userData32(), a.timestamp(),
                        a.debitsPending(), a.debitsPosted(), a.creditsPending(), a.creditsPosted());
                maxTimestamp[0] = Math.max(maxTimestamp[0], a.timestamp());
            }
            @Override public void transfer(Transfer t) {
                transfers.insert(t);
                maxTimestamp[0] = Math.max(maxTimestamp[0], t.timestamp());
            }
        });
        // Pending status + account index aren't snapshotted -- both derivable from transfers.
        stateMachine.rebuildDerivedIndexes();

        long[] counts = new long[2];   // accounts, transfers replayed from journal tail
        if (journalPath.toFile().exists()) {
            JournalReader reader = new JournalReader();
            JournalReader.ReplayResult result = reader.replayFrom(journalPath, replayFrom, new JournalReader.Visitor() {
                @Override public void onAccountsCommand(long base, List<Account> batch) {
                    stateMachine.createAccounts(batch, base);
                    maxTimestamp[0] = Math.max(maxTimestamp[0], base + batch.size() - 1);
                    counts[0] += batch.size();
                }
                @Override public void onTransfersCommand(long base, List<Transfer> batch) {
                    stateMachine.createTransfers(batch, base);
                    maxTimestamp[0] = Math.max(maxTimestamp[0], base + batch.size() - 1);
                    counts[1] += batch.size();
                }
                @Override public void onCheckpoint(long snapshotId) {
                    log.debug("Replay: checkpoint marker for snapshot {}", snapshotId);
                }
                @Override public void onExpire(long asOf) {
                    stateMachine.expirePending(asOf);
                    maxTimestamp[0] = Math.max(maxTimestamp[0], asOf);
                }
            });
            log.info("Recovery: snapshot + replayed {} account-creates and {} transfer-creates from offset {} (tailCorrupt={})",
                    counts[0], counts[1], replayFrom, result.tailCorrupted());
        }
        // Post-recovery writes must get strictly larger timestamps than anything recovered.
        clock.ensureAfter(maxTimestamp[0]);
    }

    /**
     * Test seam: stop the engine WITHOUT a final checkpoint, simulating power
     * loss. Recovery must then fall back to snapshot + journal-tail replay.
     */
    void simulateCrash() throws IOException {
        checkpointScheduler.shutdownNow();
        disruptor.shutdown();
        journal.close();
        serde.close();
    }

    @PreDestroy
    public void shutdown() throws IOException {
        log.info("Shutting down LedgerEngine...");
        checkpointScheduler.shutdownNow();
        try {
            // Final checkpoint so the next start recovers fast (writer thread still alive).
            checkpoint().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Final checkpoint on shutdown failed (continuing)", e);
        }
        disruptor.shutdown();
        journal.close();
        serde.close();
    }
}
