package ro.tenfive.pusher;

import ro.tenfive.model.Message;

import java.util.List;

public interface MessagePusher {

    /**
     * Push a single message to the destination
     * @param message the message to push
     * @return true if successful, false otherwise
     */
    boolean push(Message message);

    /**
     * Push a batch of messages to the destination
     * @param messages the list of messages to push
     * @return true if successful, false otherwise
     */
    boolean pushBatch(List<Message> messages);
}
