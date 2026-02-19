package ro.tenfive.model;

import io.micronaut.serde.annotation.Serdeable;

import java.time.Instant;

@Serdeable
public record Message(
    String id,
    String payload,
    Instant receivedAt,
    int retryCount
) {
    public Message(String id, String payload) {
        this(id, payload, Instant.now(), 0);
    }

    public Message withIncrementedRetry() {
        return new Message(id, payload, receivedAt, retryCount + 1);
    }
}
