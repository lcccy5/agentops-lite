package io.agentops.lite.worker;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** External evaluation target and polling intervals for the worker process. */
@ConfigurationProperties(prefix = "agentops.worker")
public record WorkerProperties(
    String fundAgentEvalUrl,
    String fundAgentAdminToken,
    long relayDelayMs,
    long recoveryDelayMs,
    String providerBaseUrl,
    String providerApiKey,
    int usageQueryRetryLimit,
    Duration usageQueryDeadline,
    int kafkaRetryMaxAttempts,
    Duration kafkaRetryBackoff) {}
