package com.payments.ledger.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Bound from the {@code ledger.*} properties. Registered once via
 * {@code @EnableConfigurationProperties(EngineConfig.class)} on the application
 * class -- intentionally NOT also {@code @Configuration}, which would register a
 * second bean of this type and make injection ambiguous.
 */
@ConfigurationProperties(prefix = "ledger")
public class EngineConfig {

    private long clusterId = 1;
    private String dataDir = "./data";
    private int ringBufferSize = 1 << 20;          // 1M slots
    private int maxAccounts = 10_000_000;
    private int maxTransfers = 100_000_000;
    private int expirySweepSeconds = 30;            // how often to auto-void expired pendings
    private final Journal journal = new Journal();
    private final Snapshot snapshot = new Snapshot();

    public long getClusterId() { return clusterId; }
    public void setClusterId(long v) { this.clusterId = v; }

    public String getDataDir() { return dataDir; }
    public void setDataDir(String v) { this.dataDir = v; }

    public int getRingBufferSize() { return ringBufferSize; }
    public void setRingBufferSize(int v) { this.ringBufferSize = v; }

    public int getMaxAccounts() { return maxAccounts; }
    public void setMaxAccounts(int v) { this.maxAccounts = v; }

    public int getMaxTransfers() { return maxTransfers; }
    public void setMaxTransfers(int v) { this.maxTransfers = v; }

    public int getExpirySweepSeconds() { return expirySweepSeconds; }
    public void setExpirySweepSeconds(int v) { this.expirySweepSeconds = v; }

    public Journal getJournal() { return journal; }
    public Snapshot getSnapshot() { return snapshot; }

    public static class Journal {
        private long segmentSizeBytes = 256L * 1024 * 1024;
        private boolean fsyncBatch = true;
        public long getSegmentSizeBytes() { return segmentSizeBytes; }
        public void setSegmentSizeBytes(long v) { this.segmentSizeBytes = v; }
        public boolean isFsyncBatch() { return fsyncBatch; }
        public void setFsyncBatch(boolean v) { this.fsyncBatch = v; }
    }

    public static class Snapshot {
        private int intervalSeconds = 300;
        public int getIntervalSeconds() { return intervalSeconds; }
        public void setIntervalSeconds(int v) { this.intervalSeconds = v; }
    }
}
