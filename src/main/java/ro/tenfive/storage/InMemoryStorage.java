package ro.tenfive.storage;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.tenfive.model.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@Singleton
@Requires(property = "kafka.sink.storage.type", value = "memory")
public class InMemoryStorage implements MessageStorage {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryStorage.class);

    private final ConcurrentLinkedQueue<Message> queue = new ConcurrentLinkedQueue<>();
    private final ConcurrentHashMap<String, Message> messagesById = new ConcurrentHashMap<>();

    @Override
    public void store(Message message) {
        messagesById.put(message.msgId(), message);
        queue.offer(message);
        LOG.debug("Stored message with id: {}", message.msgId());
    }

    @Override
    public List<Message> retrieve(int batchSize) {
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            Message message = queue.peek();
            if (message == null) {
                break;
            }
            messages.add(message);
        }
        return messages;
    }

    @Override
    public void delete(String messageId) {
        Message message = messagesById.remove(messageId);
        if (message != null) {
            queue.remove(message);
            LOG.debug("Deleted message with id: {}", messageId);
        }
    }

    @Override
    public void markAsFailed(Message message) {
        Message updatedMessage = message.withIncrementedRetry();
        messagesById.put(message.msgId(), updatedMessage);
        // Remove and re-add to update the queue
        queue.remove(message);
        queue.offer(updatedMessage);
        LOG.warn("Marked message as failed, retry count: {}, id: {}",
                 updatedMessage.retryCount(), message.msgId());
    }

    @Override
    public long size() {
        return queue.size();
    }

    @Override
    public void close() {
        queue.clear();
        messagesById.clear();
        LOG.info("InMemoryStorage closed");
    }
}
