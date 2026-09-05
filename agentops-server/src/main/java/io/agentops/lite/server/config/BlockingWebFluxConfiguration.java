package io.agentops.lite.server.config;

import java.util.concurrent.ExecutorService;
import org.reactivestreams.Publisher;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.springframework.web.reactive.config.BlockingExecutionConfigurer;
import org.springframework.web.reactive.config.WebFluxConfigurer;

/**
 * Moves legacy synchronous controller work off Netty event-loop threads.
 *
 * <p>The server intentionally retains JDBC and the blocking Redis template for transactional
 * accounting. WebFlux endpoints that return ordinary values therefore run on the existing
 * virtual-thread executor, while controllers that already return a reactive Publisher keep
 * their normal non-blocking execution path.</p>
 */
@Configuration(proxyBeanMethods = false)
public final class BlockingWebFluxConfiguration implements WebFluxConfigurer {
    private final ExecutorService blockingExecutor;

    /** Creates the WebFlux bridge over the server's application-owned virtual-thread executor. */
    public BlockingWebFluxConfiguration(ExecutorService blockingExecutor) {
        this.blockingExecutor = blockingExecutor;
    }

    /**
     * Delegates only synchronous handler methods to the blocking executor.
     * Reactive handlers such as the streaming chat gateway remain on their native Reactor path.
     *
     * @param configurer WebFlux handler execution configuration
     */
    @Override
    public void configureBlockingExecution(BlockingExecutionConfigurer configurer) {
        configurer.setExecutor(new ConcurrentTaskExecutor(blockingExecutor));
        configurer.setControllerMethodPredicate(handler ->
                !Publisher.class.isAssignableFrom(handler.getMethod().getReturnType()));
    }
}