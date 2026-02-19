package ro.tenfive.consumer;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.tenfive.config.ApplicationConfig;
import ro.tenfive.model.Message;
import ro.tenfive.pusher.MessagePusher;
import ro.tenfive.storage.MessageStorage;

import java.util.List;
import java.util.UUID;

@KafkaListener(
    groupId = "${kafka.sink.consumer.group-id}",
    threads = 10,
    properties = @Property(name = ConsumerConfig.MAX_POLL_RECORDS_CONFIG, value = "${kafka.sink.batching.size:100}")
)
@Requires(property = "kafka.sink.consumer.enabled", value = "true", defaultValue = "true")
public class KafkaMessageConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageConsumer.class);

    private final MessageStorage storage;
    private final MessagePusher pusher;
    private final ApplicationConfig config;

    public KafkaMessageConsumer(MessageStorage storage, MessagePusher pusher, ApplicationConfig config) {
        this.storage = storage;
        this.pusher = pusher;
        this.config = config;
    }

    @Topic("${kafka.sink.consumer.topic}")
    public void consume(List<String> payloads) {
        LOG.debug("Received {} messages.", payloads.size());
        if (config.isAsyncDelivery()) {
            for (String payload : payloads) {
                storage.store(new Message(UUID.randomUUID().toString(), payload));
            }
        } else {
            List<Message> messages = payloads.stream()
                .map(p -> new Message(UUID.randomUUID().toString(), p))
                .toList();
            pusher.pushBatch(messages);
        }
    }
}
