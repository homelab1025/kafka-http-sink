package ro.tenfive.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.tenfive.model.Message;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Singleton
@Requires(property = "kafka.sink.storage.type", value = "rocksdb")
public class RocksDBStorage implements MessageStorage {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBStorage.class);

    private final String dbPath;
    private final ObjectMapper objectMapper;
    private RocksDB db;

    public RocksDBStorage(
            @Value("${kafka.sink.storage.rocksdb.path:/tmp/kafka-sink-db}") String dbPath,
            ObjectMapper objectMapper) {
        this.dbPath = dbPath;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() {
        RocksDB.loadLibrary();
        try (Options options = new Options().setCreateIfMissing(true)) {
            db = RocksDB.open(options, dbPath);
            LOG.info("RocksDB initialized at: {}", dbPath);
        } catch (RocksDBException e) {
            LOG.error("Failed to initialize RocksDB", e);
            throw new RuntimeException("Failed to initialize RocksDB", e);
        }
    }

    @Override
    public void store(Message message) {
        try {
            byte[] key = message.id().getBytes(StandardCharsets.UTF_8);
            byte[] value = objectMapper.writeValueAsBytes(message);
            db.put(key, value);
            LOG.debug("Stored message with id: {}", message.id());
        } catch (RocksDBException | IOException e) {
            LOG.error("Failed to store message", e);
            throw new RuntimeException("Failed to store message", e);
        }
    }

    @Override
    public List<Message> retrieve(int batchSize) {
        List<Message> messages = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seekToFirst();
            int count = 0;
            while (iterator.isValid() && count < batchSize) {
                byte[] value = iterator.value();
                Message message = objectMapper.readValue(value, Message.class);
                messages.add(message);
                iterator.next();
                count++;
            }
        } catch (IOException e) {
            LOG.error("Failed to retrieve messages", e);
            throw new RuntimeException("Failed to retrieve messages", e);
        }
        return messages;
    }

    @Override
    public void delete(String messageId) {
        try {
            byte[] key = messageId.getBytes(StandardCharsets.UTF_8);
            db.delete(key);
            LOG.debug("Deleted message with id: {}", messageId);
        } catch (RocksDBException e) {
            LOG.error("Failed to delete message", e);
            throw new RuntimeException("Failed to delete message", e);
        }
    }

    @Override
    public void markAsFailed(Message message) {
        Message updatedMessage = message.withIncrementedRetry();
        store(updatedMessage);
        LOG.warn("Marked message as failed, retry count: {}, id: {}",
                 updatedMessage.retryCount(), message.id());
    }

    @Override
    public long size() {
        try {
            long count = 0;
            try (RocksIterator iterator = db.newIterator()) {
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    count++;
                    iterator.next();
                }
            }
            return count;
        } catch (Exception e) {
            LOG.error("Failed to get size", e);
            return 0;
        }
    }

    @Override
    @PreDestroy
    public void close() {
        if (db != null) {
            db.close();
            LOG.info("RocksDB closed");
        }
    }
}
