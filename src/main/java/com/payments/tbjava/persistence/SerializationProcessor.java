package com.payments.tbjava.persistence;

import com.payments.tbjava.domain.AccountSnapshot;
import com.payments.tbjava.domain.Transfer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Abstraction over checkpoint persistence, modelled on exchange-core's
 * {@code ISerializationProcessor}. Decoupling "how state is stored" from the
 * engine lets tests use an in-memory/no-op implementation while production uses
 * the LSM-backed one -- and lets the on-disk format evolve independently.
 *
 * <p>A checkpoint pairs a full state snapshot with the <em>journal offset</em>
 * it was taken at. Recovery loads the latest snapshot and then replays only the
 * journal commands written after that offset, instead of replaying the entire
 * journal from zero.
 */
public interface SerializationProcessor extends AutoCloseable {

    /**
     * Persist a snapshot of current state (streamed from {@code source}) tagged
     * with {@code journalOffset} -- the journal position from which recovery
     * should replay after loading this snapshot. Must be durable on return.
     */
    void storeSnapshot(long snapshotId, long journalOffset, SnapshotSource source) throws IOException;

    /**
     * Load the most recent snapshot into {@code sink}.
     *
     * @return the journal offset to replay from, or 0 if no snapshot exists
     */
    long loadLatestSnapshot(SnapshotSink sink) throws IOException;

    @Override
    void close() throws IOException;

    /** Streams current state to the processor at checkpoint time. */
    interface SnapshotSource {
        Iterator<AccountSnapshot> accounts();
        Iterator<Transfer> transfers();
    }

    /** Receives restored state from the processor at recovery time. */
    interface SnapshotSink {
        void account(AccountSnapshot account) throws IOException;
        void transfer(Transfer transfer) throws IOException;
    }
}
