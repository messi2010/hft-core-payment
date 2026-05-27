package com.payments.tbjava.lsm;

import com.payments.tbjava.domain.UInt128;
import com.payments.tbjava.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

/**
 * Write-ahead log guarding the {@link Memtable}.
 *
 * <p>The memtable lives only in RAM, so a crash before its next flush would
 * lose every write since the last SSTable. The WAL closes that window: each
 * mutation is appended here and fsynced before it is acknowledged, so recovery
 * can replay it back into a fresh memtable. Once the memtable is flushed to an
 * SSTable and the manifest records that table, the WAL's contents are durable
 * elsewhere and the log is {@link #rotate rotated} (truncated) to reclaim space.
 *
 * <p>Record format (little-endian): {@code flags(1) | key(8) | valueLen(4) |
 * value(valueLen) | crc32c(4)}, where {@code flags bit0} marks a tombstone.
 * Replay verifies the CRC of every record and stops at the first torn/partial
 * record at the tail — the same power-loss model the main journal uses.
 */
final class LsmWal implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LsmWal.class);
    private static final int TOMBSTONE_FLAG = 0x1;
    private static final int HEADER_SIZE = 21; // flags(1) + key(16) + valueLen(4)
    private static final int CRC_SIZE = 4;

    private final Path path;
    private FileChannel channel;

    LsmWal(Path path) throws IOException {
        this.path = path;
        Files.createDirectories(path.getParent());
        this.channel = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.channel.position(channel.size());
    }

    void append(Entry entry) throws IOException {
        byte[] value = entry.tombstone() ? new byte[0] : entry.value();
        ByteBuffer buf = Bytes.allocate(HEADER_SIZE + value.length + CRC_SIZE);
        int start = buf.position();
        buf.put((byte) (entry.tombstone() ? TOMBSTONE_FLAG : 0));
        buf.putLong(entry.key().hi());
        buf.putLong(entry.key().lo());
        buf.putInt(value.length);
        buf.put(value);
        int end = buf.position();

        CRC32C crc = new CRC32C();
        ByteBuffer slice = buf.duplicate();
        slice.position(start).limit(end);
        crc.update(slice);
        buf.putInt((int) crc.getValue());

        buf.flip();
        while (buf.hasRemaining()) channel.write(buf);
    }

    void flush() throws IOException {
        channel.force(false);
    }

    /** Replay all intact records into {@code sink}, stopping at a torn tail. */
    void replay(Consumer<Entry> sink) throws IOException {
        long size = channel.size();
        long pos = 0;
        CRC32C crc = new CRC32C();
        long recovered = 0;
        while (pos + HEADER_SIZE + CRC_SIZE <= size) {
            ByteBuffer head = Bytes.allocate(HEADER_SIZE);
            readFully(head, pos);
            head.flip();
            byte flags = head.get();
            UInt128 key = new UInt128(head.getLong(), head.getLong());
            int valueLen = head.getInt();
            if (valueLen < 0 || pos + HEADER_SIZE + valueLen + CRC_SIZE > size) {
                log.warn("LSM WAL: truncated record at offset {} -- treating as torn tail", pos);
                break;
            }

            ByteBuffer body = Bytes.allocate(HEADER_SIZE + valueLen);
            readFully(body, pos);
            body.flip();

            ByteBuffer crcBuf = Bytes.allocate(CRC_SIZE);
            readFully(crcBuf, pos + HEADER_SIZE + valueLen);
            crcBuf.flip();
            int expected = crcBuf.getInt();

            crc.reset();
            crc.update(body.duplicate());
            if ((int) crc.getValue() != expected) {
                log.warn("LSM WAL: CRC mismatch at offset {} -- treating as torn tail", pos);
                break;
            }

            if ((flags & TOMBSTONE_FLAG) != 0) {
                sink.accept(Entry.tombstone(key));
            } else {
                body.position(HEADER_SIZE);
                byte[] value = new byte[valueLen];
                body.get(value);
                sink.accept(Entry.put(key, value));
            }
            pos += HEADER_SIZE + valueLen + CRC_SIZE;
            recovered++;
        }
        // Truncate any torn tail so future appends start from a clean boundary.
        if (pos != size) {
            channel.truncate(pos);
        }
        channel.position(pos);
        log.info("LSM WAL replay: {} records recovered from {} (durable bytes={})", recovered, path, pos);
    }

    /** Discard the log after its contents have been flushed to an SSTable. */
    void rotate() throws IOException {
        channel.close();
        Files.deleteIfExists(path);
        this.channel = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    private void readFully(ByteBuffer buf, long position) throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            int n = channel.read(buf, pos);
            if (n < 0) throw new IOException("Unexpected EOF in LSM WAL at " + pos);
            pos += n;
        }
    }
}
