package com.payments.ledger.raft;

import com.payments.ledger.domain.Account;
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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 3-node cluster: write committed transfers, kill the leader, let the cluster
 * elect a new one, then prove the safety guarantees discussed for active-passive:
 * <ul>
 *   <li>committed state survives the leader change (no loss),</li>
 *   <li>retrying a committed transfer id returns {@code EXISTS} — no double-apply
 *       and no "phantom transfer" across failover,</li>
 *   <li>the cluster keeps accepting writes on the new leader.</li>
 * </ul>
 *
 * <p>(The timing-specific "uncommitted entry on the dead leader is discarded"
 * case is safe by Raft design — an unacked command was never promised to the
 * client — but is not deterministically reproducible without fault injection, so
 * this test exercises the committed-survives-failover + idempotency guarantees.)
 */
class RaftFailoverTest {

    private static final RaftGroupId GROUP_ID =
            RaftGroupId.valueOf(UUID.fromString("9b7d2f3a-1c4e-4a8b-9f01-2c3d4e5f6a7b"));

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) { return s.getLocalPort(); }
    }

    private static UInt128 u(long v) { return UInt128.of(v); }
    private static Account acct(long id) { return new Account(u(id), 704, (short) 10, (short) 0, 0, 0, 0); }
    private static Transfer transfer(long id, long d, long c, long amt) {
        return new Transfer(u(id), u(d), u(c), amt, UInt128.ZERO, 0, 0, 0, 704, (short) 1, (short) 0, 0);
    }

    @Test
    void committedStateSurvivesLeaderFailoverWithoutDoubleApply(@TempDir Path dir) throws Exception {
        int[] ports = {freePort(), freePort(), freePort()};
        String[] ids = {"n1", "n2", "n3"};

        List<RaftPeer> peers = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            peers.add(RaftPeer.newBuilder().setId(ids[i]).setAddress("127.0.0.1:" + ports[i]).build());
        }
        RaftGroup group = RaftGroup.valueOf(GROUP_ID, peers);

        List<RaftLedgerServer> servers = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                File storage = dir.resolve(ids[i]).toFile();
                RaftLedgerServer s = new RaftLedgerServer(
                        RaftPeerId.valueOf(ids[i]), group, peers.get(i), storage, 1000, 10000);
                s.start();
                servers.add(s);
            }
            Thread.sleep(4000); // initial leader election

            try (RaftLedgerClient client = new RaftLedgerClient(group)) {
                client.createAccounts(List.of(acct(1001), acct(1002)));
                assertEquals(List.of(CreateTransferResult.OK),
                        client.createTransfers(List.of(transfer(5001, 1001, 1002, 100))));
                assertEquals(100, client.lookupAccount(u(1001)).debitsPosted());

                // Find and KILL the current leader (simulate crash).
                RaftLedgerServer leader = servers.stream()
                        .filter(s -> s.isLeader(GROUP_ID)).findFirst().orElse(null);
                assertNotNull(leader, "a leader must exist before failover");
                leader.close();
                servers.remove(leader);

                Thread.sleep(6000); // re-election among the surviving 2 (quorum)

                RaftLedgerServer newLeader = servers.stream()
                        .filter(s -> s.isLeader(GROUP_ID)).findFirst().orElse(null);
                assertNotNull(newLeader, "a new leader must be elected from the survivors");

                // 1) Committed transfer survived — balance intact, not lost, not doubled.
                assertEquals(100, client.lookupAccount(u(1001)).debitsPosted(),
                        "committed balance must survive failover");

                // 2) Idempotent retry of the committed id across failover → EXISTS (no double-apply).
                assertEquals(List.of(CreateTransferResult.EXISTS),
                        client.createTransfers(List.of(transfer(5001, 1001, 1002, 100))));
                assertEquals(100, client.lookupAccount(u(1001)).debitsPosted(),
                        "retry must not double-apply");

                // 3) New writes work on the new leader.
                assertEquals(List.of(CreateTransferResult.OK),
                        client.createTransfers(List.of(transfer(5002, 1001, 1002, 30))));
                assertEquals(130, client.lookupAccount(u(1001)).debitsPosted());
                assertTrue(client.lookupAccount(u(1002)).creditsPosted() == 130);
            }
        } finally {
            for (RaftLedgerServer s : servers) {
                try { s.close(); } catch (Exception ignored) {}
            }
        }
    }
}
