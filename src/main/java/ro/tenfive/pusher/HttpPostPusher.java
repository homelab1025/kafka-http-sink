package ro.tenfive.pusher;

import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.tenfive.model.Message;

import java.util.List;

@Singleton
public class HttpPostPusher implements MessagePusher {

    private static final Logger LOG = LoggerFactory.getLogger(HttpPostPusher.class);

    private final HttpClient httpClient;
    private final String targetUrl;

    public HttpPostPusher(
            @Client HttpClient httpClient,
            @Value("${kafka.sink.pusher.http.url}") String targetUrl) {
        this.httpClient = httpClient;
        this.targetUrl = targetUrl;
    }

    @Override
    public boolean push(Message message) {
        try {
            HttpRequest<?> request = HttpRequest.POST(targetUrl, message.payload())
                    .contentType("application/json");

            HttpResponse<String> response = httpClient.toBlocking().exchange(request, String.class);

            if (response.status().getCode() >= 200 && response.status().getCode() < 300) {
                LOG.debug("Successfully pushed message with id: {}", message.msgId());
                return true;
            } else {
                LOG.warn("Failed to push message with id: {}, status: {}",
                         message.msgId(), response.status());
                return false;
            }
        } catch (Exception e) {
            LOG.error("Error pushing message with id: {}", message.msgId(), e);
            return false;
        }
    }

    @Override
    public boolean pushBatch(List<Message> messages) {
        try {
            List<String> payloads = messages.stream()
                    .map(Message::payload)
                    .toList();

            HttpRequest<?> request = HttpRequest.POST(targetUrl, payloads)
                    .contentType("application/json");

            HttpResponse<String> response = httpClient.toBlocking().exchange(request, String.class);

            if (response.status().getCode() >= 200 && response.status().getCode() < 300) {
                LOG.debug("Successfully pushed batch of {} messages", messages.size());
                return true;
            } else {
                LOG.warn("Failed to push batch, status: {}", response.status());
                return false;
            }
        } catch (Exception e) {
            LOG.error("Error pushing batch of {} messages", messages.size(), e);
            return false;
        }
    }
}
