package com.javacodeex.kafka.order.producer;

import com.javacodeex.kafka.order.entity.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Publishes orders to Kafka with a bounded, application-level retry policy.
 *
 * <p>The Kafka send is asynchronous. A failed send is retried until it succeeds
 * or {@code maxAttempts} is reached. The configured backoff is fixed, so with
 * the default settings an order can be sent at most three times with a one
 * second delay between attempts.</p>
 *
 * <p>This class does not make the database write and Kafka publish atomic. A
 * caller should use an outbox pattern when the event must not be lost after a
 * successful database commit.</p>
 */
@Component
public class OrderKafkaProducer {
    private static final Logger log = LoggerFactory.getLogger(OrderKafkaProducer.class);
    private final KafkaTemplate<String, Order> kafkaTemplate;
    private final String topic;
    private final int maxAttempts;
    private final long backoffMs;

    /**
     * Creates a producer using the topic and retry values from application configuration.
     *
     * @param kafkaTemplate Spring Kafka template used to send JSON order events
     * @param topic Kafka topic receiving order events
     * @param maxAttempts maximum number of send attempts, including the first attempt
     * @param backoffMs fixed delay between failed attempts in milliseconds
     */
    public OrderKafkaProducer(KafkaTemplate<String, Order> kafkaTemplate,
                              @Value("${app.kafka.topic}") String topic,
                              @Value("${app.kafka.retry.max-attempts:3}") int maxAttempts,
                              @Value("${app.kafka.retry.backoff-ms:1000}") long backoffMs) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.backoffMs = Math.max(0, backoffMs);
    }

    /**
     * Starts an asynchronous publish and logs the final result.
     *
     * <p>The method returns immediately. When all attempts fail, the failure is
     * logged; it is not propagated to the HTTP request that originally created
     * the order.</p>
     *
     * @param order order to serialize and publish; its id is used as the Kafka key
     */
    public void publish(Order order) {
        sendWithRetry(order, 1).whenComplete((result, exception) -> {
            if (exception != null) {
                log.error("Giving up after {} attempts for order {} on Kafka topic {}",
                        maxAttempts, order.getId(), topic, unwrap(exception));
            } else {
                log.info("Published order {} to topic {}, partition {}, offset {}",
                        order.getId(), topic, result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }

    /**
     * Sends one record and chains the next attempt after a failed asynchronous send.
     *
     * @param order order being published
     * @param attempt one-based attempt number
     * @return future completed with the Kafka send result or the final failure
     */
    private CompletableFuture<SendResult<String, Order>> sendWithRetry(Order order, int attempt) {
        log.info("Publishing order {} to topic {} (attempt {}/{})",
                order.getId(), topic, attempt, maxAttempts);

        CompletableFuture<SendResult<String, Order>> send;
        try {
            send = kafkaTemplate.send(topic, order.getId().toString(), order);
        } catch (Exception exception) {
            send = CompletableFuture.failedFuture(exception);
        }

        return send.handle((result, exception) -> {
            if (exception == null) {
                return CompletableFuture.completedFuture(result);
            }

            Throwable cause = unwrap(exception);
            if (attempt >= maxAttempts) {
                return CompletableFuture.<SendResult<String, Order>>failedFuture(cause);
            }

            log.warn("Kafka publish failed for order {} on attempt {}/{}. Retrying in {} ms",
                    order.getId(), attempt, maxAttempts, backoffMs, cause);
            return delay(backoffMs).thenCompose(ignored -> sendWithRetry(order, attempt + 1));
        }).thenCompose(future -> future);
    }

    /**
     * Applies the configured fixed backoff without blocking the Kafka callback thread.
     *
     * @param delayMs delay in milliseconds
     * @return future completed when the delay has elapsed
     */
    private CompletableFuture<Void> delay(long delayMs) {
        if (delayMs == 0) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new CompletionException(exception);
            }
        });
    }

    /**
     * Removes completion wrappers so logs show the underlying Kafka or serialization error.
     *
     * @param exception completion failure
     * @return root cause when it is wrapped, otherwise the original exception
     */
    private Throwable unwrap(Throwable exception) {
        Throwable cause = exception;
        while ((cause instanceof CompletionException || cause instanceof java.util.concurrent.ExecutionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }
}
