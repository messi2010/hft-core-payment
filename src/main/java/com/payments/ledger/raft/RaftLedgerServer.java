package com.payments.ledger.raft;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

/**
 * One ledger node = one embedded Ratis gRPC server running a
 * {@link LedgerRaftStateMachine}. A node is part of a fixed group of peers
 * (static membership); a single-node group elects itself leader, a 3-node group
 * tolerates 1 failure (active-passive: one leader writes, followers replay).
 */
public final class RaftLedgerServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RaftLedgerServer.class);

    private final RaftServer server;
    private final LedgerRaftStateMachine stateMachine;
    private final RaftPeerId peerId;

    /**
     * @param group the initial group to bootstrap/format on FIRST start; pass
     *              {@code null} on restart so the server RECOVERS the group
     *              already persisted in {@code storageDir} (re-formatting an
     *              existing storage fails).
     */
    public RaftLedgerServer(RaftPeerId peerId, RaftGroup group, RaftPeer self,
                            File storageDir, int maxAccounts, int maxTransfers) throws IOException {
        this.peerId = peerId;
        this.stateMachine = new LedgerRaftStateMachine(maxAccounts, maxTransfers);

        RaftProperties props = new RaftProperties();
        RaftConfigKeys.Rpc.setType(props, SupportedRpcType.GRPC);
        RaftServerConfigKeys.setStorageDir(props, Collections.singletonList(storageDir));
        GrpcConfigKeys.Server.setPort(props, portOf(self));

        RaftServer.Builder b = RaftServer.newBuilder()
                .setServerId(peerId)
                .setProperties(props)
                .setStateMachine(stateMachine);
        if (group != null) {
            b.setGroup(group);   // first start: bootstrap + FORMAT
        }
        this.server = b.build();
    }

    public void start() throws IOException {
        server.start();
        log.info("RaftLedgerServer {} started", peerId);
    }

    public LedgerRaftStateMachine stateMachine() { return stateMachine; }
    public RaftPeerId peerId() { return peerId; }

    /** True if this node currently believes it is the leader of {@code groupId}. */
    public boolean isLeader(RaftGroupId groupId) {
        try {
            return server.getDivision(groupId).getInfo().isLeader();
        } catch (Exception e) {
            return false;
        }
    }

    private static int portOf(RaftPeer peer) {
        String addr = peer.getAddress();
        return Integer.parseInt(addr.substring(addr.lastIndexOf(':') + 1));
    }

    @Override
    public void close() throws IOException {
        server.close();
        log.info("RaftLedgerServer {} stopped", peerId);
    }
}
