package io.agentops.lite.server.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Runtime limits and local credentials for the V0.1 deployment. */
@ConfigurationProperties(prefix = "agentops")
public record AgentOpsProperties(String adminToken, String providerBaseUrl,
                                 String providerApiKey, int defaultMaxTokens, int projectMaxTokens,
                                 int safetyMarginTokens, Duration reservationTimeout,
                                 int streamBufferSize) { }
