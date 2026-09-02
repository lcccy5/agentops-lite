package io.agentops.lite.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Starts asynchronous outbox, projection, evaluation and reconciliation workers. */
@SpringBootApplication(scanBasePackages = "io.agentops.lite")
@EnableScheduling
public class AgentOpsWorkerApplication {
    /** Starts AgentOps Worker. */
    public static void main(String[] args) { SpringApplication.run(AgentOpsWorkerApplication.class, args); }
}
