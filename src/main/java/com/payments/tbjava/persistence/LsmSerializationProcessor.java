package com.payments.tbjava.persistence;

import com.payments.tbjava.domain.AccountSnapshot;
import com.payments.tbjava.domain.Transfer;
import com.payments.tbjava.lsm.Lsm;
import com.payments.tbjava.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.zip.CRC32C;

/**
 * Snapshot store backed by the project's own {@link Lsm} engine -- this is where
 * the LSM is wired into the system. Accounts and transfers live in two separate
 * LSM trees (key = id, value = a {@link LedgerCodec}-encoded record); a tiny
 * {@code meta} file records the snapshot id and the journal offset to replay
 * from. Because each checkpoint re-{@code put}s current state, the LSM's own
 * compaction reclaims superseded versions over time.
 *
 * <p>Account/transfer ids share the 128-bit ({@link com.payments.tbjava.domain.UInt128})
 * space and could collide, hence two independent trees rather than one keyspace.
 */
public final class LsmSerializationProcessor implements SerializationProcessor {

    private static final Logger log = LoggerFactory.getLogger(LsmSerializationProcessor.class);
    private static final long META_MAGIC = 0x54424C534D5F4D54L; // "TBLSM_MT"

    private final Lsm accountsLsm;
    private final Lsm transfersLsm;
    private final Path metaPath;

    public LsmSerializationProcessor(Path snapshotDir, Lsm.Config lsmConfig) throws IOException {
        Files.createDirectories(snapshotDir);
        this.accountsLsm = Lsm.open(snapshotDir.resolve("accounts"), lsmConfig);
        this.transfersLsm = Lsm.open(snapshotDir.resolve("transfers"), lsmConfig);
        this.metaPath = snapshotDir.resolve("meta");
    }

    @Override
    public void storeSnapshot(long snapshotId, long journalOffset, SnapshotSource source) throws IOException {
        long accounts = 0, transfers = 0;
        for (Iterator<AccountSnapshot> it = source.accounts(); it.hasNext(); ) {
            AccountSnapshot a = it.next();
            accountsLsm.put(a.id(), LedgerCodec.encodeAccount(a));
            accounts++;
        }
        for (Iterator<Transfer> it = source.transfers(); it.hasNext(); ) {
            Transfer t = it.next();
            transfersLsm.put(t.id(), LedgerCodec.encodeTransfer(t));
            transfers++;
        }
        accountsLsm.flush();
        transfersLsm.flush();
        accountsLsm.sync();
        transfersLsm.sync();
        // Write meta LAST: it is the commit point. A crash before this leaves the
        // previous meta (and previous journal offset) authoritative.
        writeMeta(snapshotId, journalOffset);
        log.info("Snapshot {} stored: {} accounts, {} transfers, journalOffset={}",
                snapshotId, accounts, transfers, journalOffset);
    }

    @Override
    public long loadLatestSnapshot(SnapshotSink sink) throws IOException {
        if (!Files.exists(metaPath)) {
            log.info("No snapshot present -- full journal replay required");
            return 0;
        }
        long[] meta = readMeta();
        long snapshotId = meta[0];
        long journalOffset = meta[1];

        long[] counts = new long[2];
        accountsLsm.scan((key, value) -> {
            sink.account(LedgerCodec.decodeAccount(value));
            counts[0]++;
        });
        transfersLsm.scan((key, value) -> {
            sink.transfer(LedgerCodec.decodeTransfer(value));
            counts[1]++;
        });
        log.info("Snapshot {} loaded: {} accounts, {} transfers, replay journal from offset {}",
                snapshotId, counts[0], counts[1], journalOffset);
        return journalOffset;
    }

    @Override
    public void close() throws IOException {
        accountsLsm.close();
        transfersLsm.close();
    }

    // --- meta file: magic(8) | snapshotId(8) | journalOffset(8) | crc32c(4) ---

    private void writeMeta(long snapshotId, long journalOffset) throws IOException {
        ByteBuffer b = Bytes.allocate(28);
        b.putLong(META_MAGIC);
        b.putLong(snapshotId);
        b.putLong(journalOffset);
        CRC32C crc = new CRC32C();
        ByteBuffer view = b.duplicate();
        view.position(0).limit(24);
        crc.update(view);
        b.putInt((int) crc.getValue());
        b.flip();

        Path tmp = metaPath.resolveSibling("meta.tmp");
        try (FileChannel ch = FileChannel.open(tmp,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            while (b.hasRemaining()) ch.write(b);
            ch.force(true);
        }
        Files.move(tmp, metaPath, StandardCopyOption.ATOMIC_MOVE);
    }

    private long[] readMeta() throws IOException {
        byte[] bytes = Files.readAllBytes(metaPath);
        if (bytes.length != 28) throw new IOException("Corrupt snapshot meta: bad length " + bytes.length);
        ByteBuffer b = Bytes.wrap(bytes);
        long magic = b.getLong();
        if (magic != META_MAGIC) throw new IOException("Corrupt snapshot meta: bad magic");
        long snapshotId = b.getLong();
        long journalOffset = b.getLong();
        int expected = b.getInt();
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, 24);
        if ((int) crc.getValue() != expected) throw new IOException("Corrupt snapshot meta: CRC mismatch");
        return new long[]{snapshotId, journalOffset};
    }
}
