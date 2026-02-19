package ro.tenfive.model;

import io.micronaut.serde.annotation.Serdeable;

import java.time.Instant;

@Serdeable
public record Message(
    String msgId,
    int partnerId,
    String payload,
    Instant receivedAt,
    int retryCount
) {
    public Message(String msgId, int partnerId, String payload) {
        this(msgId, partnerId, payload, Instant.now(), 0);
    }

    public Message withIncrementedRetry() {
        return new Message(msgId, partnerId, payload, receivedAt, retryCount + 1);
    }
}
