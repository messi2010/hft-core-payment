package com.payments.ledger.raft;

import com.payments.ledger.domain.Account;
import com.payments.ledger.domain.CreateAccountResult;
import com.payments.ledger.domain.CreateTransferResult;
import com.payments.ledger.domain.Transfer;
import com.payments.ledger.domain.UInt128;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.List;

/**
 * Thin client over {@link RaftClient}: writes go through {@code io().send}
 * (replicated + committed before returning), reads through
 * {@code io().sendReadOnly} (served by the leader's applied state).
 */
public final class RaftLedgerClient implements AutoCloseable {

    private final RaftClient client;

    public RaftLedgerClient(RaftGroup group) {
        RaftProperties props = new RaftProperties();
        RaftConfigKeys.Rpc.setType(props, SupportedRpcType.GRPC);
        this.client = RaftClient.newBuilder().setProperties(props).setRaftGroup(group).build();
    }

    public List<CreateAccountResult> createAccounts(List<Account> batch) throws IOException {
        RaftClientReply reply = client.io().send(msg(LedgerCommandCodec.encodeAccounts(batch)));
        return LedgerCommandCodec.decodeAccountResults(content(reply));
    }

    public List<CreateTransferResult> createTransfers(List<Transfer> batch) throws IOException {
        RaftClientReply reply = client.io().send(msg(LedgerCommandCodec.encodeTransfers(batch)));
        return LedgerCommandCodec.decodeTransferResults(content(reply));
    }

    public void expirePending() throws IOException {
        client.io().send(msg(LedgerCommandCodec.encodeExpire()));
    }

    public LedgerCommandCodec.AccountView lookupAccount(UInt128 id) throws IOException {
        RaftClientReply reply = client.io().sendReadOnly(msg(LedgerCommandCodec.encodeLookupAccount(id)));
        return LedgerCommandCodec.decodeAccount(content(reply));
    }

    private static Message msg(byte[] bytes) {
        return Message.valueOf(ByteString.copyFrom(bytes));
    }

    private static byte[] content(RaftClientReply reply) throws IOException {
        if (!reply.isSuccess()) {
            throw new IOException("Raft request failed: " + reply.getException());
        }
        return reply.getMessage().getContent().toByteArray();
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
