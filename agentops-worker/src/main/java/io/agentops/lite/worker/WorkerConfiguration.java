package io.agentops.lite.worker;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

/** Provides worker-only infrastructure clients. */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(WorkerProperties.class)
public class WorkerConfiguration {
    /** Creates a neutral WebClient because the evaluation URL is fully configured. */
    @Bean WebClient evaluationWebClient(WebClient.Builder builder) { return builder.build(); }
}
