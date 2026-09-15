package io.agentops.lite.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.web.reactive.function.client.WebClient;

/** Provides worker-only infrastructure clients. */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(WorkerProperties.class)
public class WorkerConfiguration {
    /** Creates a neutral WebClient because the evaluation URL is fully configured. */
    @Bean WebClient evaluationWebClient(WebClient.Builder builder) { return builder.build(); }

    /** Retries transient listener failures finitely and durably publishes poison records to a sibling DLT. */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafka, WorkerProperties properties) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafka,
                (record, error) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
        // Never advance the source offset when the dead-letter publication itself was not acknowledged.
        recoverer.setFailIfSendResultIsError(true);

        long attempts = Math.max(1, properties.kafkaRetryMaxAttempts());
        long backoffMillis = Math.max(0, properties.kafkaRetryBackoff().toMillis());
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer,
                new FixedBackOff(backoffMillis, attempts - 1));
        // Malformed payloads and invalid domain arguments cannot become valid by waiting and retrying.
        handler.addNotRetryableExceptions(JsonProcessingException.class, IllegalArgumentException.class);
        return handler;
    }
}
