package com.payments.tbjava.lsm;

import com.payments.tbjava.domain.UInt128;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LsmTest {

    private static UInt128 k(long v) {
        return UInt128.of(v);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String str(byte[] b) {
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

    @Test
    void putGetFromMemtable(@TempDir Path dir) throws IOException {
        try (Lsm lsm = Lsm.open(dir, Lsm.Config.defaults())) {
            lsm.put(k(1), bytes("alice"));
            lsm.put(k(2), bytes("bob"));
            assertEquals("alice", str(lsm.get(k(1))));
            assertEquals("bob", str(lsm.get(k(2))));
            assertNull(lsm.get(k(3)));
        }
    }

    @Test
    void overwriteReturnsNewestValue(@TempDir Path dir) throws IOException {
        try (Lsm lsm = Lsm.open(dir, Lsm.Config.defaults())) {
            lsm.put(k(1), bytes("v1"));
            lsm.put(k(1), bytes("v2"));
            assertEquals("v2", str(lsm.get(k(1))));
        }
    }

    @Test
    void deleteHidesValue(@TempDir Path dir) throws IOException {
        try (Lsm lsm = Lsm.open(dir, Lsm.Config.defaults())) {
            lsm.put(k(1), bytes("alice"));
            lsm.delete(k(1));
            assertNull(lsm.get(k(1)));
        }
    }

    @Test
    void flushPersistsToSsTableAndStillReadable(@TempDir Path dir) throws IOException {
        try (Lsm lsm = Lsm.open(dir, Lsm.Config.defaults())) {
            lsm.put(k(10), bytes("ten"));
            lsm.put(k(20), bytes("twenty"));
            lsm.flush();
            assertEquals(1, lsm.liveTableCount());
            assertEquals("ten", str(lsm.get(k(10))));
            assertEquals("twenty", str(lsm.get(k(20))));
            assertNull(lsm.get(k(99)));
        }
    }

    @Test
    void uint128KeyRoundTrips(@TempDir Path dir) throws IOException {
        UInt128 uuidKey = UInt128.parse("11111111-2222-3333-4444-555555555555");
        try (Lsm lsm = Lsm.open(dir, Lsm.Config.defaults())) {
            lsm.put(uuidKey, bytes("uuid-value"));
            lsm.flush(); // force through the 16-byte on-disk path
            assertEquals("uuid-value", str(lsm.get(uuidKey)));
        }
    }

    @Test
    void tombstoneInNewerTableShadowsOlderTable(@TempDir Path dir) throws IOException {
        try (Lsm lsm = Lsm.open(dir, Lsm.Config.defaults())) {
            lsm.put(k(1), bytes("alive"));
            lsm.flush();
            lsm.delete(k(1));
            lsm.flush();
            assertEquals(2, lsm.liveTableCount());
            assertNull(lsm.get(k(1)));
        }
    }

    @Test
    void recoversMemtableFromWalAfterCrash(@TempDir Path dir) throws IOException {
        Lsm lsm = Lsm.open(dir, Lsm.Config.defaults());
        lsm.put(k(1), bytes("durable"));
        lsm.put(k(2), bytes("also-durable"));
        lsm.sync();
        lsm.close();

        try (Lsm reopened = Lsm.open(dir, Lsm.Config.defaults())) {
            assertEquals("durable", str(reopened.get(k(1))));
            assertEquals("also-durable", str(reopened.get(k(2))));
        }
    }

    @Test
    void recoversSsTablesAndMemtableAfterReopen(@TempDir Path dir) throws IOException {
        Lsm lsm = Lsm.open(dir, Lsm.Config.defaults());
        lsm.put(k(1), bytes("flushed"));
        lsm.flush();
        lsm.put(k(2), bytes("only-in-wal"));
        lsm.sync();
        lsm.close();

        try (Lsm reopened = Lsm.open(dir, Lsm.Config.defaults())) {
            assertEquals("flushed", str(reopened.get(k(1))));
            assertEquals("only-in-wal", str(reopened.get(k(2))));
        }
    }

    @Test
    void compactionMergesTablesAndPreservesNewestValues(@TempDir Path dir) throws IOException {
        Lsm.Config config = new Lsm.Config(64, 2);
        try (Lsm lsm = Lsm.open(dir, config)) {
            lsm.put(k(1), bytes("a1"));
            lsm.flush();
            lsm.put(k(1), bytes("a2"));
            lsm.put(k(2), bytes("b1"));
            lsm.flush();
            lsm.put(k(3), bytes("c1"));
            lsm.delete(k(2));
            lsm.flush();

            assertEquals(1, lsm.liveTableCount());
            assertEquals("a2", str(lsm.get(k(1))));
            assertNull(lsm.get(k(2)));
            assertEquals("c1", str(lsm.get(k(3))));
        }
    }

    @Test
    void compactionPhysicallyDropsTombstones(@TempDir Path dir) throws IOException {
        Lsm.Config config = new Lsm.Config(64, 2);
        try (Lsm lsm = Lsm.open(dir, config)) {
            lsm.put(k(5), bytes("x"));
            lsm.flush();
            lsm.delete(k(5));
            lsm.flush();
            lsm.put(k(6), bytes("y"));
            lsm.flush();
            assertEquals(1, lsm.liveTableCount());
            assertNull(lsm.get(k(5)));
            assertEquals("y", str(lsm.get(k(6))));
        }
        try (Lsm reopened = Lsm.open(dir, config)) {
            assertNull(reopened.get(k(5)));
        }
    }

    @Test
    void randomizedAgainstReferenceMap(@TempDir Path dir) throws IOException {
        Lsm.Config config = new Lsm.Config(1024, 3);
        Map<Long, byte[]> reference = new HashMap<>();
        Random rnd = new Random(42);

        try (Lsm lsm = Lsm.open(dir, config)) {
            for (int i = 0; i < 5000; i++) {
                long key = rnd.nextInt(200);
                int op = rnd.nextInt(10);
                if (op < 7) {
                    byte[] v = bytes("val-" + i);
                    lsm.put(k(key), v);
                    reference.put(key, v);
                } else {
                    lsm.delete(k(key));
                    reference.remove(key);
                }
            }
            for (long key = 0; key < 200; key++) {
                assertArrayEquals(reference.get(key), lsm.get(k(key)), "mismatch at key " + key);
            }
            assertTrue(lsm.liveTableCount() >= 1);
        }

        try (Lsm reopened = Lsm.open(dir, config)) {
            for (long key = 0; key < 200; key++) {
                assertArrayEquals(reference.get(key), reopened.get(k(key)), "post-reopen mismatch at key " + key);
            }
        }
    }

    @Test
    void corruptedSsTableIsRejectedOnOpen(@TempDir Path dir) throws IOException {
        Lsm lsm = Lsm.open(dir, Lsm.Config.defaults());
        lsm.put(k(1), bytes("data"));
        lsm.flush();
        lsm.close();

        Path sst;
        try (var s = Files.list(dir)) {
            sst = s.filter(p -> p.getFileName().toString().endsWith(".sst")).findFirst().orElseThrow();
        }
        try (FileChannel ch = FileChannel.open(sst, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer b = ByteBuffer.allocate(1);
            ch.read(b, 0);
            b.flip();
            byte corrupted = (byte) (b.get(0) ^ 0xFF);
            ch.write(ByteBuffer.wrap(new byte[]{corrupted}), 0);
        }

        assertThrows(IOException.class, () -> Lsm.open(dir, Lsm.Config.defaults()));
    }
}
