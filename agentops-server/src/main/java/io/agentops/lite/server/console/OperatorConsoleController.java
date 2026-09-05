package io.agentops.lite.server.console;

import io.agentops.lite.server.controlplane.ControlPlaneService;
import io.agentops.lite.server.gateway.ApiKeyAuthenticationFilter;
import io.agentops.lite.server.usage.UsageService;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.util.HtmlUtils;

/** Serves the thin, dependency-free operator console over existing control-plane data. */
@RestController
@RequestMapping("/console")
public final class OperatorConsoleController {
    private final UsageService usage;
    private final ControlPlaneService controlPlane;
    private final JdbcTemplate jdbc;

    /** Creates a read-mostly console without introducing a third front-end runtime. */
    public OperatorConsoleController(UsageService usage, ControlPlaneService controlPlane, JdbcTemplate jdbc) {
        this.usage = usage;
        this.controlPlane = controlPlane;
        this.jdbc = jdbc;
    }

    /** Opens the recent request view by default. */
    @GetMapping
    public ResponseEntity<Void> openConsole() {
        return ResponseEntity.status(302).location(URI.create("/console/requests")).build();
    }

    /** Lists upstream Agent runs with aggregated reservation and settlement state. */
    @GetMapping(value = "/requests", produces = MediaType.TEXT_HTML_VALUE)
    public String queryRecentRuns(ServerWebExchange exchange) {
        StringBuilder rows = new StringBuilder();
        for (Map<String, Object> run : usage.queryRecentRuns(project(exchange), 100)) {
            String id = value(run, "correlation_id");
            rows.append("<tr><td><a href='/console/requests/").append(escape(id)).append("'>")
                    .append(shortId(id)).append("</a></td><td>").append(escape(value(run, "prompt_version")))
                    .append("</td><td>").append(escape(value(run, "model_call_count")))
                    .append("</td><td>").append(escape(value(run, "actual_tokens")))
                    .append("</td><td><span class='status'>").append(escape(value(run, "settlement_status")))
                    .append("</span></td><td>").append(escape(value(run, "started_at"))).append("</td></tr>");
        }
        return page("调用记录", "一次 FundPilot 运行可关联多次真实模型调用；账本仍按调用独立结算。",
                table(List.of("Correlation ID", "Prompt 版本", "模型调用", "实际 Token", "账本状态", "开始时间"), rows));
    }

    /** Shows every reservation and ledger result associated with one upstream run. */
    @GetMapping(value = "/requests/{correlationId}", produces = MediaType.TEXT_HTML_VALUE)
    public String queryRunDetails(@PathVariable String correlationId, ServerWebExchange exchange) {
        Map<String, Object> run = usage.queryRun(project(exchange), correlationId);
        @SuppressWarnings("unchecked") List<Map<String, Object>> calls = (List<Map<String, Object>>) run.get("calls");
        StringBuilder rows = new StringBuilder();
        for (Map<String, Object> call : calls) {
            rows.append("<tr><td>").append(shortId(value(call, "request_id")))
                    .append("</td><td>").append(escape(value(call, "status")))
                    .append("</td><td>").append(escape(value(call, "reserved_tokens")))
                    .append("</td><td>").append(escape(value(call, "actual_tokens")))
                    .append("</td><td>").append(escape(value(call, "usage_source")))
                    .append("</td><td>").append(escape(value(call, "failure_code"))).append("</td></tr>");
        }
        String cards = "<div class='cards'><div><b>模型调用</b><strong>" + run.get("modelCallCount")
                + "</strong></div><div><b>预占 Token</b><strong>" + run.get("reservedTokens")
                + "</strong></div><div><b>实际 Token</b><strong>" + run.get("actualTokens")
                + "</strong></div><div><b>全部结算</b><strong>" + run.get("settled") + "</strong></div></div>";
        return page("运行详情", "Correlation ID · " + escape(correlationId), cards
                + table(List.of("Request ID", "状态", "预占", "实际", "用量来源", "失败码"), rows));
    }

    /** Lists deterministic evaluation jobs and their release gate decisions. */
    @GetMapping(value = "/evaluations", produces = MediaType.TEXT_HTML_VALUE)
    public String queryEvaluationGates(ServerWebExchange exchange) {
        List<Map<String, Object>> jobs = jdbc.queryForList("""
                select j.job_id,j.prompt_key,j.stable_version,j.candidate_version,j.status,j.created_at,
                       g.gate_result_id,g.passed,g.reasons_json,
                       count(r.result_id) result_count,sum(case when r.passed then 1 else 0 end) passed_count
                from eval_job j left join eval_gate_result g on g.job_id=j.job_id
                left join eval_result r on r.job_id=j.job_id where j.project_id=?
                group by j.job_id,j.prompt_key,j.stable_version,j.candidate_version,j.status,j.created_at,
                         g.gate_result_id,g.passed,g.reasons_json order by j.created_at desc limit 100
                """, project(exchange));
        StringBuilder rows = new StringBuilder();
        for (Map<String, Object> job : jobs) {
            rows.append("<tr><td>").append(shortId(value(job, "job_id")))
                    .append("</td><td>").append(escape(value(job, "prompt_key")))
                    .append("</td><td>").append(escape(value(job, "stable_version"))).append(" → ")
                    .append(escape(value(job, "candidate_version")))
                    .append("</td><td>").append(escape(value(job, "result_count"))).append(" / ")
                    .append(escape(value(job, "passed_count")))
                    .append("</td><td><span class='status'>").append(escape(gateState(job)))
                    .append("</span></td><td class='reason'>").append(escape(value(job, "reasons_json"))).append("</td></tr>");
        }
        return page("评测门禁", "真实 Agent 的确定性评测结果；硬安全失败或任一维度退化都会阻止发布。",
                table(List.of("Job", "Prompt", "版本对比", "结果 / 通过", "Gate", "原因"), rows));
    }
    /** Lists active, superseded and rolled-back releases with an immediate rollback action. */
    @GetMapping(value = "/releases", produces = MediaType.TEXT_HTML_VALUE)
    public String queryReleases(ServerWebExchange exchange) {
        List<Map<String, Object>> releases = jdbc.queryForList("""
                select release_id,prompt_key,environment_name,stable_version,candidate_version,canary_percent,
                       status,created_at,rolled_back_at from prompt_release
                where project_id=? order by created_at desc limit 100
                """, project(exchange));
        StringBuilder rows = new StringBuilder();
        for (Map<String, Object> release : releases) {
            boolean active = "ACTIVE".equals(value(release, "status"));
            String action = active ? "<form method='post' action='/console/releases/rollbackRelease/"
                    + escape(value(release, "release_id")) + "'><button type='submit'>回滚到稳定版</button></form>" : "—";
            rows.append("<tr><td>").append(escape(value(release, "prompt_key")))
                    .append("</td><td>").append(escape(value(release, "environment_name")))
                    .append("</td><td>").append(escape(value(release, "stable_version")))
                    .append("</td><td>").append(escape(value(release, "candidate_version")))
                    .append("</td><td>").append(escape(value(release, "canary_percent"))).append("%</td><td><span class='status'>")
                    .append(escape(value(release, "status"))).append("</span></td><td>").append(action).append("</td></tr>");
        }
        return page("发布治理", "发布只能引用已通过的 Gate；5% 灰度稳定分桶，回滚立即恢复稳定版本。",
                table(List.of("Prompt", "环境", "稳定版", "候选版", "灰度", "状态", "操作"), rows));
    }

    /** Executes the existing transactional rollback operation and returns to the release list. */
    @PostMapping("/releases/rollbackRelease/{releaseId}")
    public ResponseEntity<Void> rollbackRelease(@PathVariable String releaseId) {
        controlPlane.rollback(releaseId);
        return ResponseEntity.status(303).header(HttpHeaders.LOCATION, "/console/releases").build();
    }

    private String project(ServerWebExchange exchange) {
        return exchange.getAttribute(ApiKeyAuthenticationFilter.PROJECT_ATTRIBUTE);
    }

    private String page(String title, String subtitle, String body) {
        return """
                <!doctype html><html lang='zh-CN'><head><meta charset='utf-8'>
                <meta name='viewport' content='width=device-width,initial-scale=1'>
                <title>AgentOps Lite · %s</title><style>
                :root{font-family:Inter,"Microsoft YaHei",sans-serif;color:#172033;background:#f5f7fb}
                *{box-sizing:border-box}body{margin:0}header{background:#111827;color:white;padding:24px 5vw;display:flex;align-items:center;gap:34px}
                header b{font-size:22px}nav a{color:#cbd5e1;text-decoration:none;margin-right:22px}
                main{max-width:1280px;margin:34px auto;padding:0 24px}h1{margin-bottom:8px}p{color:#64748b}
                .panel{background:white;border:1px solid #e5eaf1;border-radius:16px;overflow:auto;margin-top:24px;box-shadow:0 8px 30px #1e293b0d}
                table{width:100%%;border-collapse:collapse}th,td{text-align:left;padding:15px 16px;border-bottom:1px solid #edf0f5;white-space:nowrap}
                th{font-size:12px;color:#718096;background:#fafbfc}.reason{max-width:420px;white-space:normal}
                .status{background:#e8f5ef;color:#14765a;border-radius:999px;padding:5px 9px;font-size:12px}a{color:#2463eb}
                .cards{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-top:24px}.cards div{background:white;border:1px solid #e5eaf1;border-radius:14px;padding:18px}
                .cards b{display:block;color:#718096;font-size:13px}.cards strong{display:block;font-size:26px;margin-top:10px}
                button{border:0;border-radius:8px;background:#172033;color:#fff;padding:9px 12px;cursor:pointer}
                @media(max-width:760px){.cards{grid-template-columns:1fr 1fr}header{display:block}nav{margin-top:16px}th,td{padding:12px 10px}}
                </style></head><body><header><b>AgentOps Lite</b><nav>
                <a href='/console/platform'>Agent 与预算</a><a href='/console/requests'>调用记录</a><a href='/console/evaluations'>评测门禁</a>
                <a href='/console/releases'>发布治理</a></nav></header>
                <main><h1>%s</h1><p>%s</p>%s</main></body></html>
                """.formatted(escape(title), escape(title), escape(subtitle), body);
    }

    private String table(List<String> headers, StringBuilder rows) {
        StringBuilder head = new StringBuilder();
        headers.forEach(header -> head.append("<th>").append(escape(header)).append("</th>"));
        String content = rows.isEmpty() ? "<tr><td colspan='" + headers.size() + "'>暂无数据</td></tr>" : rows.toString();
        return "<div class='panel'><table><thead><tr>" + head + "</tr></thead><tbody>" + content + "</tbody></table></div>";
    }

    private String gateState(Map<String, Object> job) {
        Object passed = job.get("passed");
        return passed == null ? value(job, "status") : Boolean.TRUE.equals(passed) ? "PASSED" : "REJECTED";
    }

    private String value(Map<String, Object> row, String key) {
        Object value = row.get(key);
        return value == null ? "—" : String.valueOf(value);
    }

    private String shortId(String id) {
        String escaped = escape(id);
        return escaped.length() > 16 ? escaped.substring(0, 16) + "…" : escaped;
    }

    private String escape(String value) {
        return HtmlUtils.htmlEscape(value == null ? "" : value);
    }
}
