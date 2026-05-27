package com.payments.ledger.raft;

import com.payments.ledger.engine.IdGenerator;
import com.payments.ledger.engine.LedgerStateMachine;
import com.payments.ledger.storage.AccountStore;
import com.payments.ledger.storage.PendingTransferIndex;
import com.payments.ledger.storage.TransferStore;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Ratis state machine wrapping the deterministic {@link LedgerStateMachine}.
 *
 * <p><b>Single-writer preserved.</b> The Raft log is the (replicated) total order
 * of commands; Ratis applies them one at a time via {@link #applyTransaction},
 * which is the only place stores are mutated — exactly the single-writer model,
 * now replicated. Followers apply the same committed entries and reach identical
 * state.
 *
 * <p><b>Deterministic timestamps.</b> {@link #startTransaction} runs on the leader
 * only; it reserves the base timestamp block and stamps it into the log entry, so
 * the value is replicated and every node computes {@code ts = base + index}
 * identically. {@link #applyTransaction} never reads a clock.
 *
 * <p><b>Recovery (this version).</b> Relies on Ratis replaying the log through
 * {@code applyTransaction} on restart; no LSM snapshot is taken yet (log grows
 * unbounded — snapshot/log-purge is the documented next step, same as journal
 * truncation in the single-node engine).
 */
public final class LedgerRaftStateMachine extends BaseStateMachine {

    private static final Logger log = LoggerFactory.getLogger(LedgerRaftStateMachine.class);

    private final AccountStore accounts;
    private final TransferStore transfers;
    private final PendingTransferIndex pending;
    private final LedgerStateMachine sm;
    private final IdGenerator clock = new IdGenerator();

    public LedgerRaftStateMachine(int maxAccounts, int maxTransfers) {
        this.accounts = new AccountStore(maxAccounts);
        this.transfers = new TransferStore(maxTransfers);
        this.pending = new PendingTransferIndex();
        this.sm = new LedgerStateMachine(accounts, transfers, pending);
    }

    /** Test/inspection seam: the underlying deterministic state machine. */
    public LedgerStateMachine ledger() { return sm; }

    @Override
    public void initialize(RaftServer server, RaftGroupId groupId,
                           RaftStorage raftStorage) throws IOException {
        super.initialize(server, groupId, raftStorage);
        log.info("LedgerRaftStateMachine initialized for group {}", groupId);
    }

    /**
     * Leader-only: assign the deterministic base timestamp for this command and
     * embed it in the log entry that will be replicated.
     */
    @Override
    public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
        byte[] cmd = request.getMessage().getContent().toByteArray();
        long base = clock.reserve(LedgerCommandCodec.reserveCount(cmd));
        byte[] stamped = LedgerCommandCodec.withBaseTimestamp(cmd, base);
        return TransactionContext.newBuilder()
                .setStateMachine(this)
                .setClientRequest(request)
                .setLogData(ByteString.copyFrom(stamped))
                .build();
    }

    /** Applied on every node, in committed log order. The sole mutation point. */
    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final LogEntryProto entry = trx.getLogEntry();
        final byte[] cmd = entry.getStateMachineLogEntry().getLogData().toByteArray();
        byte[] result;
        try {
            result = LedgerCommandCodec.apply(cmd, sm);
        } catch (RuntimeException e) {
            log.error("applyTransaction failed at index {}: {}", entry.getIndex(), e.getMessage(), e);
            return CompletableFuture.failedFuture(e);
        }
        // Advance the clock past every applied timestamp on EVERY node, so a
        // follower promoted to leader assigns strictly larger timestamps.
        long base = LedgerCommandCodec.baseTimestamp(cmd);
        clock.ensureAfter(base + LedgerCommandCodec.reserveCount(cmd) - 1);
        updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
        return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(result)));
    }

    /** Read-only path: never touches the log. */
    @Override
    public CompletableFuture<Message> query(Message request) {
        byte[] q = request.getContent().toByteArray();
        byte[] result = LedgerCommandCodec.query(q, sm);
        return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(result)));
    }
}
