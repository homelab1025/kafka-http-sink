package ro.tenfive.consumer;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.tenfive.config.ApplicationConfig;
import ro.tenfive.model.Message;
import ro.tenfive.pusher.MessagePusher;
import ro.tenfive.storage.MessageStorage;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

@KafkaListener(
    groupId = "${kafka.sink.consumer.group-id}",
    threads = 10,
    properties = @Property(name = ConsumerConfig.MAX_POLL_RECORDS_CONFIG, value = "${kafka.sink.batching.size:100}")
)
@Requires(property = "kafka.sink.consumer.enabled", value = "true", defaultValue = "true")
public class KafkaMessageConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageConsumer.class);
    public static final String HEADER_MSG_ID = "msg_id";
    public static final String HEADER_PARTNER_ID = "partner_id";

    private final MessageStorage storage;
    private final MessagePusher pusher;
    private final ApplicationConfig config;

    public KafkaMessageConsumer(MessageStorage storage, MessagePusher pusher, ApplicationConfig config) {
        this.storage = storage;
        this.pusher = pusher;
        this.config = config;
    }

    @Topic("${kafka.sink.consumer.topic}")
    public void consume(List<ConsumerRecord<String, String>> records) {
        LOG.debug("Received {} messages.", records.size());
        if (config.isAsyncDelivery()) {
            for (ConsumerRecord<String, String> record : records) {
                Message message = toMessage(record);
                if (message != null) {
                    storage.store(message);
                }
            }
        } else {
            List<Message> messages = records.stream()
                .map(this::toMessage)
                .filter(Objects::nonNull)
                .toList();
            if (!messages.isEmpty()) {
                pusher.pushBatch(messages);
            }
        }
    }

    private Message toMessage(ConsumerRecord<String, String> record) {
        Header msgIdHeader = record.headers().lastHeader(HEADER_MSG_ID);
        Header partnerIdHeader = record.headers().lastHeader(HEADER_PARTNER_ID);

        if (msgIdHeader == null || partnerIdHeader == null) {
            LOG.warn("Skipping record at offset {} — missing required headers (msg_id, partner_id)", record.offset());
            return null;
        }

        try {
            String msgId = new String(msgIdHeader.value(), StandardCharsets.UTF_8);
            int partnerId = Integer.parseInt(new String(partnerIdHeader.value(), StandardCharsets.UTF_8));
            return new Message(msgId, partnerId, record.value());
        } catch (NumberFormatException e) {
            LOG.warn("Skipping record at offset {} — partner_id is not a valid integer", record.offset());
            return null;
        }
    }
}
