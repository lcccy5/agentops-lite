package io.agentops.lite.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Starts the online gateway and control-plane process. */
@SpringBootApplication(scanBasePackages = "io.agentops.lite")
@EnableScheduling
public class AgentOpsServerApplication {
    /** Starts AgentOps Server. */
    public static void main(String[] args) { SpringApplication.run(AgentOpsServerApplication.class, args); }
}
