package io.agentops.lite.worker;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** External evaluation target and polling intervals for the worker process. */
@ConfigurationProperties(prefix = "agentops.worker")
public record WorkerProperties(String fundAgentEvalUrl, String fundAgentAdminToken,
                               long relayDelayMs, long recoveryDelayMs) { }
