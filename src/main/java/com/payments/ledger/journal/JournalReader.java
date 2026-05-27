package com.payments.ledger.journal;

import com.payments.ledger.domain.Account;
import com.payments.ledger.domain.Transfer;
import com.payments.ledger.domain.UInt128;
import com.payments.ledger.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * Reads journal command records sequentially. Verifies CRC on every record;
 * stops at the first corrupted/truncated record and reports the truncation point
 * so the caller can repair a torn tail (power loss) without losing committed
 * commands earlier in the log.
 *
 * <p>Each command is handed back as its original input batch + the base
 * timestamp it was assigned. The caller re-applies it through the deterministic
 * state machine; this reader performs no state mutation itself.
 */
public final class JournalReader {

    private static final Logger log = LoggerFactory.getLogger(JournalReader.class);

    public interface Visitor {
        void onAccountsCommand(long baseTimestamp, List<Account> batch) throws IOException;
        void onTransfersCommand(long baseTimestamp, List<Transfer> batch) throws IOException;
        void onCheckpoint(long snapshotId) throws IOException;
        void onExpire(long asOfTimestamp) throws IOException;
    }

    public record ReplayResult(long recordsRead, long lastValidPosition, boolean tailCorrupted) {}

    /** Replay the whole journal from the beginning. */
    public ReplayResult replay(Path journalPath, Visitor visitor) throws IOException {
        return replayFrom(journalPath, 0, visitor);
    }

    /** Replay from {@code startOffset} (e.g. the offset recorded by a snapshot). */
    public ReplayResult replayFrom(Path journalPath, long startOffset, Visitor visitor) throws IOException {
        if (!Files.exists(journalPath)) {
            log.info("No journal at {} -- nothing to replay", journalPath);
            return new ReplayResult(0, startOffset, false);
        }

        long records = 0;
        long lastValid = startOffset;
        boolean tailCorrupt = false;
        CRC32C crc = new CRC32C();

        try (FileChannel ch = FileChannel.open(journalPath, StandardOpenOption.READ)) {
            long size = ch.size();
            ch.position(startOffset);
            ByteBuffer header = Bytes.allocate(Journal.HEADER_SIZE);

            while (ch.position() < size) {
                long recStart = ch.position();
                header.clear();
                if (readFully(ch, header) < 0) break;
                header.flip();

                byte type = header.get();
                header.get();  // reserved
                int payloadLen = header.getInt();
                long baseTimestamp = header.getLong();

                if (payloadLen < 0 || recStart + Journal.HEADER_SIZE + payloadLen + Journal.CRC_SIZE > size) {
                    log.warn("Journal: truncated record at offset {} -- treating as torn write", recStart);
                    tailCorrupt = true;
                    break;
                }

                ByteBuffer payload = Bytes.allocate(payloadLen);
                readFully(ch, payload);
                payload.flip();

                ByteBuffer crcBuf = Bytes.allocate(Journal.CRC_SIZE);
                readFully(ch, crcBuf);
                crcBuf.flip();
                int expectedCrc = crcBuf.getInt();

                crc.reset();
                header.position(0).limit(Journal.HEADER_SIZE);
                crc.update(header);
                payload.position(0).limit(payloadLen);
                crc.update(payload);
                if ((int) crc.getValue() != expectedCrc) {
                    log.error("Journal: CRC mismatch at offset {} -- corruption, stopping replay", recStart);
                    tailCorrupt = true;
                    break;
                }

                payload.position(0).limit(payloadLen);
                dispatch(type, baseTimestamp, payload, visitor, recStart);
                lastValid = ch.position();
                records++;
            }
        }
        log.info("Journal replay from {}: {} records, lastValid={}, tailCorrupt={}",
                startOffset, records, lastValid, tailCorrupt);
        return new ReplayResult(records, lastValid, tailCorrupt);
    }

    private void dispatch(byte type, long baseTimestamp, ByteBuffer payload,
                          Visitor visitor, long offset) throws IOException {
        switch (type) {
            case RecordType.CREATE_ACCOUNTS -> {
                int count = payload.getInt();
                List<Account> batch = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    UInt128 id = new UInt128(payload.getLong(), payload.getLong());
                    long userData64 = payload.getLong();
                    int userData32 = payload.getInt();
                    int ledger = payload.getInt();
                    short code = payload.getShort();
                    short flags = payload.getShort();
                    batch.add(new Account(id, ledger, code, flags, userData64, userData32, 0));
                }
                visitor.onAccountsCommand(baseTimestamp, batch);
            }
            case RecordType.CREATE_TRANSFERS -> {
                int count = payload.getInt();
                List<Transfer> batch = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    UInt128 id = new UInt128(payload.getLong(), payload.getLong());
                    UInt128 debit = new UInt128(payload.getLong(), payload.getLong());
                    UInt128 credit = new UInt128(payload.getLong(), payload.getLong());
                    long amount = payload.getLong();
                    UInt128 pendingId = new UInt128(payload.getLong(), payload.getLong());
                    long userData64 = payload.getLong();
                    int userData32 = payload.getInt();
                    int timeout = payload.getInt();
                    int ledger = payload.getInt();
                    short code = payload.getShort();
                    short flags = payload.getShort();
                    batch.add(new Transfer(id, debit, credit, amount, pendingId,
                            userData64, userData32, timeout, ledger, code, flags, 0));
                }
                visitor.onTransfersCommand(baseTimestamp, batch);
            }
            case RecordType.CHECKPOINT -> visitor.onCheckpoint(payload.getLong());
            case RecordType.EXPIRE_PENDING -> visitor.onExpire(baseTimestamp);
            default -> log.warn("Unknown record type {} at offset {}", type, offset);
        }
    }

    private int readFully(FileChannel ch, ByteBuffer buf) throws IOException {
        int total = 0;
        while (buf.hasRemaining()) {
            int n = ch.read(buf);
            if (n < 0) return total == 0 ? -1 : total;
            total += n;
        }
        return total;
    }
}
