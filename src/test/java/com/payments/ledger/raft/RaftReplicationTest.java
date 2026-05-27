package com.payments.ledger.raft;

import com.payments.ledger.domain.Account;
import com.payments.ledger.domain.CreateAccountResult;
import com.payments.ledger.domain.CreateTransferResult;
import com.payments.ledger.domain.Transfer;
import com.payments.ledger.domain.UInt128;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Boots a single-node embedded Ratis cluster running the ledger state machine,
 * drives writes through the replicated log, reads them back, then restarts the
 * node on the same storage to prove state is rebuilt by replaying the Raft log.
 */
class RaftReplicationTest {

    private static final RaftGroupId GROUP_ID =
            RaftGroupId.valueOf(UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1"));

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) { return s.getLocalPort(); }
    }

    private static UInt128 u(long v) { return UInt128.of(v); }

    private static Account acct(long id) {
        return new Account(u(id), 704, (short) 10, (short) 0, 0, 0, 0);
    }

    private static Transfer transfer(long id, long debit, long credit, long amount) {
        return new Transfer(u(id), u(debit), u(credit), amount, UInt128.ZERO, 0, 0, 0, 704, (short) 1, (short) 0, 0);
    }

    @Test
    void writesReplicateAndSurviveRestartViaLogReplay(@TempDir Path dir) throws Exception {
        int port = freePort();
        RaftPeerId peerId = RaftPeerId.valueOf("n1");
        RaftPeer self = RaftPeer.newBuilder().setId(peerId).setAddress("127.0.0.1:" + port).build();
        RaftGroup group = RaftGroup.valueOf(GROUP_ID, self);
        File storage = dir.resolve("n1").toFile();

        // --- RUN 1: write through Raft, read back ---
        try (RaftLedgerServer server = new RaftLedgerServer(peerId, group, self, storage, 1000, 10000)) {
            server.start();
            Thread.sleep(2000); // let the single node elect itself leader

            try (RaftLedgerClient client = new RaftLedgerClient(group)) {
                List<CreateAccountResult> ar = client.createAccounts(List.of(acct(1001), acct(1002)));
                assertEquals(List.of(CreateAccountResult.OK, CreateAccountResult.OK), ar);

                List<CreateTransferResult> tr = client.createTransfers(List.of(transfer(5001, 1001, 1002, 100)));
                assertEquals(List.of(CreateTransferResult.OK), tr);

                var a1 = client.lookupAccount(u(1001));
                var a2 = client.lookupAccount(u(1002));
                assertTrue(a1.found());
                assertEquals(100, a1.debitsPosted());
                assertEquals(100, a2.creditsPosted());

                // idempotent retry of the same transfer id → EXISTS, no double-apply
                assertEquals(List.of(CreateTransferResult.EXISTS),
                        client.createTransfers(List.of(transfer(5001, 1001, 1002, 100))));
                assertEquals(100, client.lookupAccount(u(1001)).debitsPosted());
            }
        }

        Thread.sleep(1000); // let the port/server fully release

        // --- RUN 2: restart same node + storage → state rebuilt from the Raft log ---
        // Pass group=null so the server RECOVERS the persisted group (no re-FORMAT).
        RaftPeer self2 = RaftPeer.newBuilder().setId(peerId).setAddress("127.0.0.1:" + port).build();
        RaftGroup group2 = RaftGroup.valueOf(GROUP_ID, self2);
        try (RaftLedgerServer server = new RaftLedgerServer(peerId, null, self2, storage, 1000, 10000)) {
            server.start();
            Thread.sleep(2000);
            try (RaftLedgerClient client = new RaftLedgerClient(group2)) {
                var a1 = client.lookupAccount(u(1001));
                var a2 = client.lookupAccount(u(1002));
                assertEquals(100, a1.debitsPosted(), "debitsPosted must survive restart via log replay");
                assertEquals(100, a2.creditsPosted());
            }
        }
    }
}
