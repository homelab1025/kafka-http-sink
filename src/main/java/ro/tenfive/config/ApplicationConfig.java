package ro.tenfive.config;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("kafka.sink")
public class ApplicationConfig {

    private Storage storage = new Storage();
    private Delivery delivery = new Delivery();
    private Batching batching = new Batching();
    private Retry retry = new Retry();

    public Storage getStorage() {
        return storage;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    public Delivery getDelivery() {
        return delivery;
    }

    public void setDelivery(Delivery delivery) {
        this.delivery = delivery;
    }

    public Batching getBatching() {
        return batching;
    }

    public void setBatching(Batching batching) {
        this.batching = batching;
    }

    public Retry getRetry() {
        return retry;
    }

    public void setRetry(Retry retry) {
        this.retry = retry;
    }

    @ConfigurationProperties("storage")
    public static class Storage {
        private String type = "memory"; // "memory" or "rocksdb"
        private RocksDB rocksdb = new RocksDB();

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public RocksDB getRocksdb() {
            return rocksdb;
        }

        public void setRocksdb(RocksDB rocksdb) {
            this.rocksdb = rocksdb;
        }

        @ConfigurationProperties("rocksdb")
        public static class RocksDB {
            private String path = "/tmp/kafka-sink-db";

            public String getPath() {
                return path;
            }

            public void setPath(String path) {
                this.path = path;
            }
        }
    }

    @ConfigurationProperties("delivery")
    public static class Delivery {
        private boolean guaranteed = false;

        public boolean isGuaranteed() {
            return guaranteed;
        }

        public void setGuaranteed(boolean guaranteed) {
            this.guaranteed = guaranteed;
        }
    }

    @ConfigurationProperties("batching")
    public static class Batching {
        private boolean enabled = false;
        private int size = 100;
        private long timeoutMs = 5000;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            this.size = size;
        }

        public long getTimeoutMs() {
            return timeoutMs;
        }

        public void setTimeoutMs(long timeoutMs) {
            this.timeoutMs = timeoutMs;
        }
    }

    @ConfigurationProperties("retry")
    public static class Retry {
        private int maxAttempts = 3;
        private long delayMs = 1000;

        public int getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public long getDelayMs() {
            return delayMs;
        }

        public void setDelayMs(long delayMs) {
            this.delayMs = delayMs;
        }
    }
}
