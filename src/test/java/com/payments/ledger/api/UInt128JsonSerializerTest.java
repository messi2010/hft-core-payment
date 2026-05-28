package com.payments.ledger.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.payments.ledger.domain.AccountSnapshot;
import com.payments.ledger.domain.UInt128;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("UInt128 JSON serializer — id ra chuỗi (decimal hoặc UUID), không lộ hi/lo")
class UInt128JsonSerializerTest {

    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        SimpleModule m = new SimpleModule();
        m.addSerializer(UInt128.class, new UInt128JsonSerializer());
        mapper = new ObjectMapper().registerModule(m);
    }

    @Test
    @DisplayName("id nhỏ (hi=0) → chuỗi decimal")
    void smallIdAsDecimalString() throws Exception {
        assertThat(mapper.writeValueAsString(UInt128.of(1001))).isEqualTo("\"1001\"");
    }

    @Test
    @DisplayName("ZERO → \"0\"")
    void zeroAsString() throws Exception {
        assertThat(mapper.writeValueAsString(UInt128.ZERO)).isEqualTo("\"0\"");
    }

    @Test
    @DisplayName("id sinh từ UUID (hi≠0) → chuỗi UUID")
    void uuidIdAsUuidString() throws Exception {
        UUID uuid = UUID.fromString("6f9619ff-8b86-d011-b42d-00cf4fc964ff");
        assertThat(mapper.writeValueAsString(RingId.of(uuid)))
                .isEqualTo("\"6f9619ff-8b86-d011-b42d-00cf4fc964ff\"");
    }

    @Test
    @DisplayName("AccountSnapshot không còn lộ {hi,lo,zero} cho field id")
    void accountSnapshotIdIsAString() throws Exception {
        AccountSnapshot snap = new AccountSnapshot(UInt128.of(1001), 704, (short) 10, (short) 0,
                0L, 0, 0L, 1000L, 0L, 0L, 0L);
        String json = mapper.writeValueAsString(snap);
        assertThat(json).contains("\"id\":\"1001\"");
        assertThat(json).doesNotContain("\"hi\"");
        assertThat(json).doesNotContain("\"lo\"");
    }

    @Test
    @DisplayName("round-trip: chuỗi serialize ra → RingId.of(chuỗi) khôi phục đúng id")
    void roundTripThroughRingId() throws Exception {
        UInt128 original = RingId.of(UUID.fromString("6f9619ff-8b86-d011-b42d-00cf4fc964ff"));
        String json = mapper.writeValueAsString(original);                 // "\"6f96...\""
        String idStr = json.substring(1, json.length() - 1);               // strip quotes
        assertThat(RingId.of(idStr)).isEqualTo(original);

        UInt128 small = UInt128.of(1001);
        String smallJson = mapper.writeValueAsString(small);
        assertThat(RingId.of(smallJson.substring(1, smallJson.length() - 1))).isEqualTo(small);
    }
}
