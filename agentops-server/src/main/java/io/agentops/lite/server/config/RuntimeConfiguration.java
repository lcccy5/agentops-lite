package io.agentops.lite.server.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/** Defines bounded blocking and provider resources away from the Netty event loop. */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(AgentOpsProperties.class)
public class RuntimeConfiguration {
    /** Creates the provider client with a configured upstream origin. */
    @Bean WebClient providerWebClient(WebClient.Builder builder, AgentOpsProperties properties) {
        return builder.baseUrl(properties.providerBaseUrl()).build();
    }

    /** Runs JDBC and Redis admission operations on virtual threads with a concurrency cap. */
    @Bean(destroyMethod = "close") ExecutorService blockingExecutor() {
        return Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("agentops-blocking-", 0).factory());
    }

    /** Adapts the virtual-thread executor for Reactor boundaries. */
    @Bean(destroyMethod = "dispose") Scheduler blockingScheduler(ExecutorService blockingExecutor) {
        return Schedulers.fromExecutorService(blockingExecutor);
    }
}
