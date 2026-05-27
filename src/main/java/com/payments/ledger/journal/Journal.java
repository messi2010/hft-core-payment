package com.payments.ledger.journal;

import com.payments.ledger.domain.Account;
import com.payments.ledger.domain.Transfer;
import com.payments.ledger.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * Append-only write-ahead log for the ledger state machine.
 *
 * <p><b>Command journaling.</b> Each record is one whole input <em>command</em>
 * (a batch of accounts or transfers) plus the {@code baseTimestamp} that was
 * reserved for it -- not the resulting mutated state. Recovery replays these
 * commands through the (deterministic) state machine, so a command is durable
 * as an atomic unit: there is no way to persist half of a linked chain and then
 * roll the rest back in memory, which was the failure mode of per-record state
 * journaling.
 *
 * <p>Record format (little-endian):
 * <pre>
 *   offset  size  field
 *   0       1     record_type
 *   1       1     reserved
 *   2       4     payload_length
 *   6       8     base_timestamp
 *   14      N     payload
 *   14+N    4     CRC32C (over header + payload)
 * </pre>
 *
 * <p>Only the single writer thread calls the {@code append*} methods.
 * {@link #flush} fsyncs the file; the typical pattern is to append the commands
 * of a Disruptor batch and flush once at {@code endOfBatch}.
 */
public final class Journal implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Journal.class);
    static final int HEADER_SIZE = 14;
    static final int CRC_SIZE = 4;

    /** Serialized size of one account's input fields inside a command payload (u128 id). */
    public static final int ACCOUNT_RECORD_SIZE = 36;
    /** Serialized size of one transfer's input fields inside a command payload (u128 ids). */
    public static final int TRANSFER_RECORD_SIZE = 96;

    private final FileChannel channel;
    private final ByteBuffer writeBuf;
    private final CRC32C crc = new CRC32C();
    private final Path path;
    private long position;

    public Journal(Path path, int bufferSizeBytes) throws IOException {
        this.path = path;
        Files.createDirectories(path.getParent());
        this.channel = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.position = channel.size();
        channel.position(this.position);
        this.writeBuf = Bytes.allocateDirect(bufferSizeBytes);
        log.info("Journal opened at {} (size={} bytes)", path, position);
    }

    public long position() { return position; }
    public Path path() { return path; }

    /** Append a CREATE_ACCOUNTS command: the batch's input fields + its base timestamp. */
    public void appendAccountsCommand(long baseTimestamp, List<Account> batch) throws IOException {
        ByteBuffer p = Bytes.allocate(4 + batch.size() * ACCOUNT_RECORD_SIZE);
        p.putInt(batch.size());
        for (Account a : batch) {
            p.putLong(a.id().hi());
            p.putLong(a.id().lo());
            p.putLong(a.userData64());
            p.putInt(a.userData32());
            p.putInt(a.ledger());
            p.putShort(a.code());
            p.putShort(a.flags());
        }
        p.flip();
        write(RecordType.CREATE_ACCOUNTS, baseTimestamp, p);
    }

    /** Append a CREATE_TRANSFERS command: the batch's input fields + its base timestamp. */
    public void appendTransfersCommand(long baseTimestamp, List<Transfer> batch) throws IOException {
        ByteBuffer p = Bytes.allocate(4 + batch.size() * TRANSFER_RECORD_SIZE);
        p.putInt(batch.size());
        for (Transfer t : batch) {
            p.putLong(t.id().hi());
            p.putLong(t.id().lo());
            p.putLong(t.debitAccountId().hi());
            p.putLong(t.debitAccountId().lo());
            p.putLong(t.creditAccountId().hi());
            p.putLong(t.creditAccountId().lo());
            p.putLong(t.amount());
            p.putLong(t.pendingId().hi());
            p.putLong(t.pendingId().lo());
            p.putLong(t.userData64());
            p.putInt(t.userData32());
            p.putInt(t.timeoutSeconds());
            p.putInt(t.ledger());
            p.putShort(t.code());
            p.putShort(t.flags());
        }
        p.flip();
        write(RecordType.CREATE_TRANSFERS, baseTimestamp, p);
    }

    /** Append a CHECKPOINT marker recording the snapshot id taken up to this point. */
    public void appendCheckpoint(long snapshotId) throws IOException {
        ByteBuffer p = Bytes.allocate(8);
        p.putLong(snapshotId);
        p.flip();
        write(RecordType.CHECKPOINT, snapshotId, p);
    }

    /** Append an EXPIRE_PENDING command: void OPEN pendings with timeout <= asOfTimestamp. */
    public void appendExpire(long asOfTimestamp) throws IOException {
        ByteBuffer empty = Bytes.allocate(0);
        empty.flip();
        write(RecordType.EXPIRE_PENDING, asOfTimestamp, empty);
    }

    private void write(byte type, long baseTimestamp, ByteBuffer payload) throws IOException {
        int payloadLen = payload.remaining();
        int totalLen = HEADER_SIZE + payloadLen + CRC_SIZE;

        ByteBuffer rec = Bytes.allocate(totalLen);
        rec.put(type);
        rec.put((byte) 0);
        rec.putInt(payloadLen);
        rec.putLong(baseTimestamp);
        rec.put(payload);
        int end = rec.position();
        crc.reset();
        ByteBuffer slice = rec.duplicate();
        slice.position(0).limit(end);
        crc.update(slice);
        rec.putInt((int) crc.getValue());
        rec.flip();

        if (rec.remaining() > writeBuf.capacity()) {
            // Command larger than the write buffer: flush what's buffered, then
            // stream this record straight to the channel.
            flushBufferToChannel();
            while (rec.hasRemaining()) position += channel.write(rec);
            return;
        }
        if (writeBuf.remaining() < rec.remaining()) {
            flushBufferToChannel();
        }
        writeBuf.put(rec);
    }

    private void flushBufferToChannel() throws IOException {
        writeBuf.flip();
        while (writeBuf.hasRemaining()) {
            position += channel.write(writeBuf);
        }
        writeBuf.clear();
    }

    /** Force buffered records to disk. Call at the end of a Disruptor batch. */
    public void flush() throws IOException {
        if (writeBuf.position() > 0) {
            flushBufferToChannel();
        }
        channel.force(false);
    }

    @Override
    public void close() throws IOException {
        flush();
        channel.close();
        log.info("Journal closed (final position={})", position);
    }
}
