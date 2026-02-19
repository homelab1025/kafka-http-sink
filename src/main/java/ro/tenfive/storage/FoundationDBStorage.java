package ro.tenfive.storage;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.tenfive.model.Message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Singleton
@Requires(property = "kafka.sink.storage.type", value = "foundationdb")
public class FoundationDBStorage implements MessageStorage {

    private static final Logger LOG = LoggerFactory.getLogger(FoundationDBStorage.class);

    private final int apiVersion;
    private final String clusterFile;
    private final String directoryPath;
    private final ObjectMapper objectMapper;

    private Database db;
    private DirectorySubspace messageSpace;

    public FoundationDBStorage(
            @Value("${kafka.sink.storage.foundationdb.api-version:730}") int apiVersion,
            @Value("${kafka.sink.storage.foundationdb.cluster-file:}") String clusterFile,
            @Value("${kafka.sink.storage.foundationdb.directory:kafka-sink-messages}") String directoryPath,
            ObjectMapper objectMapper) {
        this.apiVersion = apiVersion;
        this.clusterFile = clusterFile;
        this.directoryPath = directoryPath;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() {
        try {
            FDB fdb = FDB.selectAPIVersion(apiVersion);

            if (clusterFile != null && !clusterFile.isEmpty()) {
                db = fdb.open(clusterFile);
            } else {
                db = fdb.open();
            }

            // Create or open a directory for our messages
            messageSpace = DirectoryLayer.getDefault()
                    .createOrOpen(db, List.of(directoryPath))
                    .join();

            LOG.info("FoundationDB initialized with directory: {}", directoryPath);
        } catch (Exception e) {
            LOG.error("Failed to initialize FoundationDB", e);
            throw new RuntimeException("Failed to initialize FoundationDB", e);
        }
    }

    @Override
    public void store(Message message) {
        try {
            byte[] key = messageSpace.pack(Tuple.from(message.msgId()));
            byte[] value = objectMapper.writeValueAsBytes(message);

            db.run(tr -> {
                tr.set(key, value);
                return null;
            });

            LOG.debug("Stored message with id: {}", message.msgId());
        } catch (IOException e) {
            LOG.error("Failed to store message", e);
            throw new RuntimeException("Failed to store message", e);
        }
    }

    @Override
    public List<Message> retrieve(int batchSize) {
        try {
            return db.read(tr -> {
                List<Message> messages = new ArrayList<>();

                Range range = messageSpace.range();
                int count = 0;

                for (KeyValue kv : tr.getRange(range, batchSize)) {
                    if (count >= batchSize) {
                        break;
                    }

                    try {
                        Message message = objectMapper.readValue(kv.getValue(), Message.class);
                        messages.add(message);
                        count++;
                    } catch (IOException e) {
                        LOG.error("Failed to deserialize message", e);
                    }
                }

                return messages;
            });
        } catch (Exception e) {
            LOG.error("Failed to retrieve messages", e);
            return List.of();
        }
    }

    @Override
    public void delete(String messageId) {
        try {
            byte[] key = messageSpace.pack(Tuple.from(messageId));

            db.run(tr -> {
                tr.clear(key);
                return null;
            });

            LOG.debug("Deleted message with id: {}", messageId);
        } catch (Exception e) {
            LOG.error("Failed to delete message", e);
            throw new RuntimeException("Failed to delete message", e);
        }
    }

    @Override
    public void markAsFailed(Message message) {
        Message updatedMessage = message.withIncrementedRetry();
        store(updatedMessage);
        LOG.warn("Marked message as failed, retry count: {}, id: {}",
                 updatedMessage.retryCount(), message.msgId());
    }

    @Override
    public long size() {
        try {
            return db.read(tr -> {
                long count = 0;
                Range range = messageSpace.range();

                for (KeyValue ignored : tr.getRange(range)) {
                    count++;
                }

                return count;
            });
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
            LOG.info("FoundationDB closed");
        }
    }
}
