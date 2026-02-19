package ro.tenfive.storage;

import ro.tenfive.model.Message;

import java.util.List;
import java.util.Optional;

public interface MessageStorage {

    /**
     * Store a message for later processing
     */
    void store(Message message);

    /**
     * Retrieve a batch of messages for processing
     * @param batchSize maximum number of messages to retrieve
     * @return list of messages
     */
    List<Message> retrieve(int batchSize);

    /**
     * Delete a message after successful processing
     */
    void delete(String messageId);

    /**
     * Mark a message as failed and update retry count
     */
    void markAsFailed(Message message);

    /**
     * Get the current size of the storage
     */
    long size();

    /**
     * Close and cleanup resources
     */
    void close();
}
