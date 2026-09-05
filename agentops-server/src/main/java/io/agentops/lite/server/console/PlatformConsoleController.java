package io.agentops.lite.server.console;

import io.agentops.lite.server.project.ProjectAdministrationService;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.HtmlUtils;

/** Serves the visual multi-Agent control surface over the project administration API. */
@RestController
@RequestMapping("/console/platform")
public final class PlatformConsoleController {
    private final ProjectAdministrationService projects;

    /** Creates the platform page backed by durable project configuration and live Redis quota state. */
    public PlatformConsoleController(ProjectAdministrationService projects) { this.projects = projects; }

    /** Renders the selected Agent's budget, connection state and management forms. */
    @GetMapping
    public String queryPlatform(@RequestParam(required = false) String projectId) {
        List<Map<String, Object>> all = projects.queryProjects();
        if (all.isEmpty()) return page("无可用 Agent", "请先创建一个 Agent 产品。", "");
        String selected = projectId == null || projectId.isBlank() ? value(all.getFirst(), "project_id") : projectId;
        Map<String, Object> overview = projects.queryProjectOverview(selected);
        StringBuilder options = new StringBuilder();
        for (Map<String, Object> project : all) {
            String id = value(project, "project_id");
            options.append("<option value='").append(escape(id)).append("'").append(id.equals(selected) ? " selected" : "").append(">")
                    .append(escape(value(project, "name"))).append(" · ").append(escape(id)).append("</option>");
        }
        long limit = number(overview, "token_limit"), consumed = number(overview, "consumedTokens"), reserved = number(overview, "reservedTokens");
        int percent = limit == 0 ? 0 : (int) Math.min(100, ((consumed + reserved) * 100) / limit);
        String body = """
                <section class='topbar'><form method='get'><label>当前 Agent</label><select name='projectId' onchange='this.form.submit()'>%s</select></form><a class='link' href='/console/requests'>调用记录</a><a class='link' href='/console/evaluations'>评测门禁</a><a class='link' href='/console/releases'>发布治理</a></section>
                <section class='hero'><div><span class='eyebrow'>AGENT CONTROL PLANE</span><h1>%s</h1><p>独立预算、并发和接入密钥。所有用量与发布数据按 Agent 项目隔离。</p></div><div class='badge'>%s<br><small>在线可用 Token</small></div></section>
                <section class='cards'><article><span>累计额度</span><strong>%s</strong><small>已消耗 %s · 预占 %s</small></article><article><span>在线配额</span><strong>%s%%</strong><div class='meter'><i style='width:%s%%'></i></div><small>消耗加预占占额度</small></article><article><span>并发请求</span><strong>%s / %s</strong><small>活跃模型请求 / 上限</small></article><article><span>接入密钥</span><strong>%s</strong><small>启用中的 API Key</small></article></section>
                <section class='grid'><article class='panel'><h2>调整预算与并发</h2><p>修改只影响后续请求；在途请求照原状态结算。</p><form method='post' action='/console/platform/updateProjectQuota'><input type='hidden' name='projectId' value='%s'><label>累计 Token 额度<input required min='1' type='number' name='tokenLimit' value='%s'></label><label>最大并发请求<input required min='1' type='number' name='maxConcurrency' value='%s'></label><label>默认输出 Token<input required min='1' type='number' name='defaultMaxTokens' value='%s'></label><label>单次输出上限<input required min='1' type='number' name='projectMaxTokens' value='%s'></label><button>保存预算</button></form></article>
                <article class='panel'><h2>接入密钥</h2><p>新密钥只在下一页显示一次，请立即保存到 Agent 服务端。</p><form method='post' action='/console/platform/createProjectApiKey'><input type='hidden' name='projectId' value='%s'><label>密钥标签<input name='label' placeholder='例如 production-gateway'></label><button>生成 API Key</button></form><p class='hint'>当前启用密钥：%s 个。</p></article></section>
                <section class='panel create'><h2>接入新的 Agent 产品</h2><p>每个 Agent 产品创建为独立项目，拥有自己的预算、并发和 API Key。</p><form method='post' action='/console/platform/createProject'><label>项目 ID<input required pattern='[a-z0-9-]+' name='newProjectId' placeholder='project-customer-agent'></label><label>名称<input required name='name' placeholder='Customer Service Agent'></label><label>Token 额度<input required min='1' type='number' name='tokenLimit' value='1000000'></label><label>并发<input required min='1' type='number' name='maxConcurrency' value='32'></label><label>默认输出<input required min='1' type='number' name='defaultMaxTokens' value='1024'></label><label>输出上限<input required min='1' type='number' name='projectMaxTokens' value='4096'></label><button>创建 Agent</button></form></section>
                """.formatted(options, escape(value(overview, "name")), format(number(overview, "availableTokens")), format(limit), format(consumed), format(reserved), percent, percent, number(overview, "activeRequests"), number(overview, "max_concurrency"), number(overview, "apiKeyCount"), escape(selected), limit, number(overview, "max_concurrency"), number(overview, "default_max_tokens"), number(overview, "project_max_tokens"), escape(selected), number(overview, "apiKeyCount"));
        return page("Agent 平台", "多 Agent 治理", body);
    }

    /** Creates an isolated Agent product from the visual onboarding form. */
    @PostMapping("/createProject")
    public ResponseEntity<Void> createProject(@RequestParam String newProjectId, @RequestParam String name, @RequestParam long tokenLimit, @RequestParam int maxConcurrency, @RequestParam int defaultMaxTokens, @RequestParam int projectMaxTokens) {
        projects.createProject(new ProjectAdministrationService.CreateProjectRequest(newProjectId, name, tokenLimit, maxConcurrency, defaultMaxTokens, projectMaxTokens));
        return redirect(newProjectId);
    }

    /** Persists quota controls for the selected Agent product. */
    @PostMapping("/updateProjectQuota")
    public ResponseEntity<Void> updateProjectQuota(@RequestParam String projectId, @RequestParam long tokenLimit, @RequestParam int maxConcurrency, @RequestParam int defaultMaxTokens, @RequestParam int projectMaxTokens) {
        projects.updateProjectQuota(projectId, new ProjectAdministrationService.UpdateQuotaRequest(tokenLimit, maxConcurrency, defaultMaxTokens, projectMaxTokens));
        return redirect(projectId);
    }

    /** Creates a project API key and returns a no-store page that displays it exactly once. */
    @PostMapping(value = "/createProjectApiKey", produces = "text/html")
    public ResponseEntity<String> createProjectApiKey(@RequestParam String projectId, @RequestParam(required = false) String label) {
        Map<String, Object> created = projects.createProjectApiKey(projectId, new ProjectAdministrationService.CreateApiKeyRequest(label == null ? "" : label));
        String secret = escape(String.valueOf(created.get("apiKey")));
        String body = "<section class='panel secret'><span class='eyebrow'>SAVE THIS NOW</span><h1>新的 API Key</h1><p>关闭或刷新页面后无法再次查看。请将它保存到 " + escape(projectId) + " 的服务端密钥库。</p><code>" + secret + "</code><p><a class='link' href='/console/platform?projectId=" + escape(projectId) + "'>返回 Agent 管理页</a></p></section>";
        return ResponseEntity.ok().header(HttpHeaders.CACHE_CONTROL, "no-store, max-age=0").header("Pragma", "no-cache").body(page("保存 API Key", "一次性凭据", body));
    }

    private ResponseEntity<Void> redirect(String projectId) { return ResponseEntity.status(303).location(URI.create("/console/platform?projectId=" + projectId)).build(); }
    private String page(String title, String subtitle, String body) { return """
                <!doctype html><html lang='zh-CN'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>AgentOps · %s</title><style>:root{font-family:Inter,"Microsoft YaHei",sans-serif;color:#ebf2ff;background:#0b1220}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 80%% 0,#19345a 0,#0b1220 46%%);min-height:100vh}.topbar,main{max-width:1180px;margin:auto}.topbar{display:flex;gap:18px;align-items:center;padding:22px 20px}.topbar form{margin-right:auto}.topbar label,.topbar select{color:#b9c8dd;font-size:13px}.topbar select{margin-left:9px;background:#142238;color:#fff;border:1px solid #34506f;border-radius:8px;padding:8px}.link{color:#9fc3ff;text-decoration:none;font-size:14px}main{padding:24px 20px 64px}.hero{display:flex;justify-content:space-between;gap:30px;padding:34px 0}.eyebrow{color:#50d5b7;letter-spacing:1.5px;font-size:12px}.hero h1{font-size:36px;margin:10px 0}.hero p,.panel p,.hint{color:#a9b9cc;line-height:1.6}.badge{background:#102a46;border:1px solid #2d6382;border-radius:18px;padding:23px;min-width:180px;text-align:center;color:#70edce;font-size:27px}.badge small{font-size:12px;color:#b6c6d8}.cards,.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px}.cards article,.panel{background:#101c2e;border:1px solid #263b56;border-radius:16px;padding:20px}.cards span,.panel label{display:block;color:#9fb0c5;font-size:13px}.cards strong{font-size:25px;display:block;margin:10px 0;color:#fff}.cards small{color:#8598b0}.meter{height:7px;background:#263b56;border-radius:10px;overflow:hidden;margin:16px 0 9px}.meter i{display:block;height:100%%;background:linear-gradient(90deg,#50d5b7,#55a9ff)}.grid{grid-template-columns:1fr 1fr;margin-top:16px}.panel h2{margin-top:0;font-size:18px}.panel form,.create form{display:grid;grid-template-columns:1fr 1fr;gap:12px}.create{margin-top:16px}.create form{grid-template-columns:repeat(3,1fr)}input{width:100%%;margin-top:6px;padding:10px;border-radius:8px;border:1px solid #334a67;background:#0a1423;color:white}button{background:#55a9ff;border:0;border-radius:8px;color:#06111f;font-weight:700;padding:11px;cursor:pointer;align-self:end}.secret{max-width:720px;margin:80px auto}.secret code{display:block;background:#07101c;border:1px solid #50d5b7;padding:18px;border-radius:10px;word-break:break-all;color:#70edce}@media(max-width:760px){.cards,.grid,.create form{grid-template-columns:1fr}.hero{display:block}.badge{margin-top:18px}.topbar{flex-wrap:wrap}}</style></head><body><div class='topbar'><b>AgentOps Lite</b><a class='link' href='/console/platform'>Agent 与预算</a></div><main><p class='eyebrow'>%s</p>%s</main></body></html>
                """.formatted(escape(title), escape(subtitle), body); }
    private static long number(Map<String, Object> row, String key) { Object value = row.get(key); return value instanceof Number number ? number.longValue() : 0; }
    private static String value(Map<String, Object> row, String key) { Object value = row.get(key); return value == null ? "" : String.valueOf(value); }
    private static String format(long value) { return String.format("%,d", value); }
    private static String escape(String value) { return HtmlUtils.htmlEscape(value == null ? "" : value); }
}
