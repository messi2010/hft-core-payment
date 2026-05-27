package com.payments.tbjava.lsm;

import com.payments.tbjava.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * The source of truth for which SSTables are live and in what read-precedence
 * order (index 0 = newest, wins on conflict).
 *
 * <p>Flush and compaction create and delete SSTable files; the manifest is what
 * makes those changes atomic from a reader's point of view. It is rewritten by
 * staging to a temp file, fsyncing, then {@code ATOMIC_MOVE}-renaming over the
 * old one. A crash therefore leaves either the old or the new manifest intact,
 * never a half-written one — and any SSTable file not referenced by the
 * surviving manifest is an orphan from an interrupted operation and is ignored.
 *
 * <p>Format (little-endian): {@code magic(8) | count(4) | seq(8)*count | crc32c(4)}.
 */
final class Manifest {

    private static final long MAGIC = 0x54424C534D5F4D46L; // "TBLSM_MF"

    private final Path path;

    Manifest(Path path) {
        this.path = path;
    }

    boolean exists() {
        return Files.exists(path);
    }

    /** Read the ordered list of live SSTable sequence numbers (newest first). */
    List<Long> read() throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        if (bytes.length < 12 + 4) {
            throw new IOException("Manifest too small: " + path);
        }
        ByteBuffer buf = Bytes.wrap(bytes);
        long magic = buf.getLong();
        if (magic != MAGIC) throw new IOException("Bad manifest magic: " + path);
        int count = buf.getInt();
        int bodyEnd = 12 + count * Long.BYTES;
        if (bodyEnd + 4 > bytes.length) throw new IOException("Manifest truncated: " + path);

        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bodyEnd);
        int expected = Bytes.wrap(bytes).position(bodyEnd).getInt();
        if ((int) crc.getValue() != expected) {
            throw new IOException("Manifest CRC mismatch: " + path);
        }

        List<Long> seqs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) seqs.add(buf.getLong());
        return seqs;
    }

    /** Atomically replace the manifest with {@code seqs} (newest first). */
    void write(List<Long> seqs) throws IOException {
        int bodyEnd = 12 + seqs.size() * Long.BYTES;
        ByteBuffer buf = Bytes.allocate(bodyEnd + 4);
        buf.putLong(MAGIC);
        buf.putInt(seqs.size());
        for (long s : seqs) buf.putLong(s);

        CRC32C crc = new CRC32C();
        ByteBuffer crcView = buf.duplicate();
        crcView.position(0).limit(bodyEnd);
        crc.update(crcView);
        buf.putInt((int) crc.getValue());
        buf.flip();

        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            while (buf.hasRemaining()) ch.write(buf);
            ch.force(true);
        }
        Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE);
    }
}
