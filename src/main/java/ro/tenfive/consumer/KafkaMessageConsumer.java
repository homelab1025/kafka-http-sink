package ro.tenfive.consumer;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Requires;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.tenfive.model.Message;
import ro.tenfive.storage.MessageStorage;

import java.util.UUID;

@KafkaListener(
    groupId = "${kafka.sink.consumer.group-id}",
    threads = 10
)
@Requires(property = "kafka.sink.consumer.enabled", value = "true", defaultValue = "true")
public class KafkaMessageConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageConsumer.class);

    private final MessageStorage storage;

    public KafkaMessageConsumer(MessageStorage storage) {
        this.storage = storage;
    }

    @Topic("${kafka.sink.consumer.topic}")
    public void consume(String payload) {
        try {
            String messageId = UUID.randomUUID().toString();
            Message message = new Message(messageId, payload);
            storage.store(message);
            LOG.debug("Consumed and stored message with id: {}", messageId);
        } catch (Exception e) {
            LOG.error("Failed to consume message", e);
        }
    }
}
