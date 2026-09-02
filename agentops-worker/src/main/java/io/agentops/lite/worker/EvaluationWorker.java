package io.agentops.lite.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.agentops.lite.contract.Contracts.AgentEvalObservation;
import io.agentops.lite.contract.Contracts.AgentEvalRequest;
import io.agentops.lite.contract.Contracts.EvalCaseDefinition;
import io.agentops.lite.contract.Contracts.EvalCaseEvent;
import io.agentops.lite.core.domain.DeterministicEvalScorer;
import io.agentops.lite.core.domain.EvalGatePolicy;
import io.agentops.lite.core.domain.EvalGatePolicy.QualityMetrics;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.reactive.function.client.WebClient;

/** Reliably dispatches cases, calls the fund Agent, scores observations and closes the deterministic gate. */
@Component
public final class EvaluationWorker {
    private final DeterministicEvalScorer scorer = new DeterministicEvalScorer();
    private final EvalGatePolicy gatePolicy = new EvalGatePolicy();
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final KafkaTemplate<String, String> kafka;
    private final WebClient webClient;
    private final WorkerProperties properties;
    private final ObjectMapper mapper;

    /** Creates the evaluation background worker. */
    public EvaluationWorker(JdbcTemplate jdbc, TransactionTemplate transactions, KafkaTemplate<String, String> kafka,
                            WebClient evaluationWebClient, WorkerProperties properties, ObjectMapper mapper) {
        this.jdbc = jdbc; this.transactions = transactions; this.kafka = kafka; this.webClient = evaluationWebClient; this.properties = properties; this.mapper = mapper;
    }

    /** Publishes each case with caseId as the partition key to preserve parallelism. */
    @Scheduled(fixedDelayString = "${agentops.worker.relay-delay-ms:1000}")
    public void relayEvaluationOutbox() {
        List<Map<String, Object>> rows = jdbc.queryForList("select event_id,event_key,payload_json from eval_dispatch_outbox where status='PENDING' and next_attempt_at<=? order by created_at limit 100", Instant.now());
        for (Map<String, Object> row : rows) {
            try {
                kafka.send("agentops.eval.case.v1", row.get("event_key").toString(), row.get("payload_json").toString()).get();
                jdbc.update("update eval_dispatch_outbox set status='PUBLISHED',published_at=? where event_id=?", Instant.now(), row.get("event_id"));
            } catch (Exception exception) {
                jdbc.update("update eval_dispatch_outbox set attempts=attempts+1,next_attempt_at=? where event_id=?", Instant.now().plusSeconds(5), row.get("event_id"));
            }
        }
    }

    /** Redrives cases left pending after Kafka retries are exhausted, without duplicating recent dispatches. */
    @Scheduled(fixedDelayString = "${agentops.worker.recovery-delay-ms:10000}")
    public void recoverPendingEvaluationCases() {
        Instant recentCutoff = Instant.now().minusSeconds(30);
        List<Map<String, Object>> stranded = jdbc.queryForList("select c.job_id,c.case_id,c.prompt_version from eval_job_case c join eval_job j on j.job_id=c.job_id where c.status='PENDING' and j.status='PENDING' and not exists (select 1 from eval_dispatch_outbox o where o.job_id=c.job_id and o.case_id=c.case_id and o.prompt_version=c.prompt_version and (o.status='PENDING' or o.created_at>?)) limit 100", recentCutoff);
        for (Map<String, Object> row : stranded) {
            String jobId = row.get("job_id").toString();
            String caseId = row.get("case_id").toString();
            String version = row.get("prompt_version").toString();
            Instant now = Instant.now();
            int reset = jdbc.update("update eval_dispatch_outbox set status='PENDING',attempts=0,next_attempt_at=?,published_at=null where job_id=? and case_id=? and prompt_version=? and status='PUBLISHED'",
                    now, jobId, caseId, version);
            if (reset == 0) {
                EvalCaseEvent event = new EvalCaseEvent(jobId, caseId, version);
                jdbc.update("insert into eval_dispatch_outbox(event_id,job_id,case_id,prompt_version,event_key,payload_json,status,next_attempt_at,created_at) values(?,?,?,?,?,?,'PENDING',?,?)",
                        UUID.randomUUID().toString(), jobId, caseId, version, caseId, json(event), now, now);
            }
        }
    }

    /** Executes one idempotent case/version observation through the local-only fund Agent endpoint. */
    @KafkaListener(topics = "agentops.eval.case.v1", concurrency = "4")
    public void evaluateCase(String payload) throws Exception {
        EvalCaseEvent event = mapper.readValue(payload, EvalCaseEvent.class);
        Integer exists = jdbc.queryForObject("select count(*) from eval_result where job_id=? and case_id=? and prompt_version=?", Integer.class, event.jobId(), event.caseId(), event.promptVersion());
        if (exists != null && exists > 0) return;
        EvalCaseDefinition definition = loadDefinition(event);
        jdbc.update("update eval_job_case set status='RUNNING',attempts=attempts+1 where job_id=? and case_id=? and prompt_version=?", event.jobId(), event.caseId(), event.promptVersion());
        AgentEvalObservation observation;
        try {
            observation = webClient.post().uri(properties.fundAgentEvalUrl()).contentType(MediaType.APPLICATION_JSON)
                    .header("X-Agent-Eval-Token", properties.fundAgentAdminToken())
                    .bodyValue(new AgentEvalRequest(event.caseId(), event.promptVersion(), definition.question(), definition.fixtureId()))
                    .retrieve().bodyToMono(AgentEvalObservation.class).timeout(Duration.ofMinutes(2)).block();
            if (observation == null) throw new IllegalStateException("Fund Agent returned no observation");
        } catch (RuntimeException failure) {
            jdbc.update("update eval_job_case set status='PENDING' where job_id=? and case_id=? and prompt_version=?", event.jobId(), event.caseId(), event.promptVersion());
            throw failure;
        }
        DeterministicEvalScorer.Score score = scorer.score(definition, observation);
        transactions.executeWithoutResult(status -> {
            try {
                jdbc.update("insert into eval_result(result_id,job_id,case_id,prompt_version,passed,hard_safety,score_json,observation_json,input_tokens,output_tokens,first_token_millis,total_millis,created_at) values(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        UUID.randomUUID().toString(), event.jobId(), event.caseId(), event.promptVersion(), score.passed(), definition.hardSafety(), json(score), json(observation), observation.inputTokens(), observation.outputTokens(), observation.firstTokenMillis(), observation.totalMillis(), Instant.now());
            } catch (DuplicateKeyException ignored) { }
            jdbc.update("update eval_job_case set status='COMPLETED' where job_id=? and case_id=? and prompt_version=?", event.jobId(), event.caseId(), event.promptVersion());
        });
        closeGateIfComplete(event.jobId());
    }

    private EvalCaseDefinition loadDefinition(EvalCaseEvent event) throws Exception {
        String json = jdbc.queryForObject("select c.definition_json from eval_case c join eval_job j on j.dataset_id=c.dataset_id where j.job_id=? and c.case_id=?", String.class, event.jobId(), event.caseId());
        return mapper.readValue(json, EvalCaseDefinition.class);
    }

    private void closeGateIfComplete(String jobId) {
        Integer pending = jdbc.queryForObject("select count(*) from eval_job_case where job_id=? and status<>'COMPLETED'", Integer.class, jobId);
        if (pending == null || pending > 0) return;
        Map<String, Object> job = jdbc.queryForMap("select stable_version,candidate_version,max_token_growth_percent from eval_job where job_id=?", jobId);
        String stable = job.get("stable_version").toString(); String candidate = job.get("candidate_version").toString();
        Number unsafeFailures = jdbc.queryForObject("select count(*) from eval_result where job_id=? and prompt_version=? and hard_safety=true and passed=false", Number.class, jobId, candidate);
        QualityMetrics stableMetrics = qualityMetrics(jobId, stable);
        QualityMetrics candidateMetrics = qualityMetrics(jobId, candidate);
        double stableTokens = averageTokens(jobId, stable); double candidateTokens = averageTokens(jobId, candidate);
        long allowedGrowth = ((Number) job.get("max_token_growth_percent")).longValue();
        EvalGatePolicy.Decision decision = gatePolicy.decide(unsafeFailures.longValue(), stableMetrics, candidateMetrics,
                stableTokens, candidateTokens, allowedGrowth);
        transactions.executeWithoutResult(status -> {
            jdbc.update("insert into eval_gate_result(gate_result_id,job_id,passed,reasons_json,created_at) values(?,?,?,?,?) on duplicate key update passed=values(passed),reasons_json=values(reasons_json)", UUID.randomUUID().toString(), jobId, decision.passed(), json(decision.reasons()), Instant.now());
            jdbc.update("update eval_job set status=?,completed_at=? where job_id=?", decision.passed() ? "PASSED" : "FAILED", Instant.now(), jobId);
        });
    }

    /** Reads persisted score dimensions so the release gate cannot hide a regression behind an aggregate score. */
    private QualityMetrics qualityMetrics(String job, String version) {
        Double passRate = jdbc.queryForObject("select avg(case when passed then 1.0 else 0.0 end) from eval_result where job_id=? and prompt_version=?", Double.class, job, version);
        List<String> scores = jdbc.queryForList("select score_json from eval_result where job_id=? and prompt_version=?", String.class, job, version);
        return new QualityMetrics(passRate == null ? 0 : passRate,
                scoreRate(scores, "toolSelectionPassed"), scoreRate(scores, "argumentsPassed"),
                scoreRate(scores, "evidencePassed"));
    }

    private double scoreRate(List<String> scores, String field) {
        if (scores.isEmpty()) return 0;
        long passed = scores.stream().filter(score -> {
            try { return mapper.readTree(score).path(field).asBoolean(false); }
            catch (Exception invalidScore) { throw new IllegalStateException("Invalid persisted evaluation score", invalidScore); }
        }).count();
        return (double) passed / scores.size();
    }
    private double averageTokens(String job, String version) { Double value = jdbc.queryForObject("select avg(input_tokens+output_tokens) from eval_result where job_id=? and prompt_version=?", Double.class, job, version); return value == null ? 0 : value; }
    private String json(Object value) { try { return mapper.writeValueAsString(value); } catch (Exception exception) { throw new IllegalStateException(exception); } }
}
