package ro.tenfive.service;

import io.micronaut.context.annotation.Requires;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.tenfive.config.ApplicationConfig;
import ro.tenfive.model.Message;
import ro.tenfive.pusher.MessagePusher;
import ro.tenfive.storage.MessageStorage;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Singleton
@Requires(property = "kafka.sink.pusher.enabled", value = "true", defaultValue = "true")
public class PusherService {

    private static final Logger LOG = LoggerFactory.getLogger(PusherService.class);

    private final MessageStorage storage;
    private final MessagePusher pusher;
    private final ApplicationConfig config;
    private final ExecutorService executorService;

    public PusherService(
            MessageStorage storage,
            MessagePusher pusher,
            ApplicationConfig config) {
        this.storage = storage;
        this.pusher = pusher;
        this.config = config;
        this.executorService = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Scheduled(fixedDelay = "${kafka.sink.pusher.poll-interval:1s}")
    public void processPendingMessages() {
        executorService.submit(() -> {
            try {
                if (storage.size() == 0) {
                    return;
                }

                if (config.getBatching().isEnabled()) {
                    processBatch();
                } else {
                    processIndividual();
                }
            } catch (Exception e) {
                LOG.error("Error processing pending messages", e);
            }
        });
    }

    private void processBatch() {
        int batchSize = config.getBatching().getSize();
        List<Message> messages = storage.retrieve(batchSize);

        if (messages.isEmpty()) {
            return;
        }

        LOG.debug("Processing batch of {} messages", messages.size());

        boolean success = pusher.pushBatch(messages);

        if (success) {
            messages.forEach(message -> storage.delete(message.id()));
            LOG.info("Successfully pushed batch of {} messages", messages.size());
        } else {
            handleFailedMessages(messages);
        }
    }

    private void processIndividual() {
        List<Message> messages = storage.retrieve(1);

        if (messages.isEmpty()) {
            return;
        }

        Message message = messages.get(0);
        boolean success = pusher.push(message);

        if (success) {
            storage.delete(message.id());
            LOG.debug("Successfully pushed message with id: {}", message.id());
        } else {
            handleFailedMessages(List.of(message));
        }
    }

    private void handleFailedMessages(List<Message> messages) {
        int maxAttempts = config.getRetry().getMaxAttempts();

        for (Message message : messages) {
            if (message.retryCount() >= maxAttempts) {
                LOG.error("Message with id: {} exceeded max retry attempts, removing",
                          message.id());
                storage.delete(message.id());
            } else {
                storage.markAsFailed(message);
                LOG.warn("Message with id: {} failed, will retry. Attempt: {}/{}",
                         message.id(), message.retryCount() + 1, maxAttempts);
            }
        }
    }
}
