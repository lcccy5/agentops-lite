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

/** Serves the visual multi-Agent control surface over durable configuration and live quota state. */
@RestController
@RequestMapping("/console/platform")
public final class PlatformConsoleController {
    private final ProjectAdministrationService projects;

    /** Creates the platform page backed by durable project configuration and Redis quota state. */
    public PlatformConsoleController(ProjectAdministrationService projects) { this.projects = projects; }

    /** Renders a selected Agent dashboard and keeps management actions progressively disclosed. */
    @GetMapping
    public String queryPlatform(@RequestParam(required = false) String projectId) {
        List<Map<String, Object>> all = projects.queryProjects();
        String selected = projectId == null || projectId.isBlank() ? (all.isEmpty() ? "" : value(all.getFirst(), "project_id")) : projectId;
        Map<String, Object> overview = selected.isBlank() ? Map.of() : projects.queryProjectOverview(selected);
        long limit = number(overview, "token_limit"), consumed = number(overview, "consumedTokens"), reserved = number(overview, "reservedTokens");
        int percent = limit == 0 ? 0 : (int) Math.min(100, ((consumed + reserved) * 100) / limit);
        String navProject = escape(selected);
        StringBuilder options = new StringBuilder();
        for (Map<String, Object> project : all) {
            String id = value(project, "project_id");
            options.append("<option value='").append(escape(id)).append("'").append(id.equals(selected) ? " selected" : "").append(">")
                    .append(escape(value(project, "name"))).append(" · ").append(escape(id)).append("</option>");
        }
        String keys = keyRows(selected);
        String main = selected.isBlank() ? emptyState() : """
                <section class='switcher'><form method='get'><label>当前 Agent 产品</label><select name='projectId' onchange='this.form.submit()'>%s</select></form><span class='isolation'>已隔离 · 预算、密钥、调用与发布</span><button class='quiet' onclick=\"showDialog('create-agent')\">＋ 添加 Agent</button></section>
                <section class='hero'><div><span class='eyebrow'>AGENT CONTROL PLANE</span><h1>%s</h1><p>把运行状态留在首页；配置与接入在需要时展开。当前所有数字均来自实时配额和持久化配置。</p></div><div class='available'><b>%s</b><span>在线可用 Token</span></div></section>
                <section class='cards'><article><span>累计额度</span><strong>%s</strong><small>已消耗 %s · 预占 %s</small></article><article><span>额度占用</span><strong>%s%%</strong><div class='meter'><i style='width:%s%%'></i></div><small>消耗与在途预占</small></article><article><span>并发请求</span><strong>%s / %s</strong><small>活跃请求 / 上限</small></article><article><span>启用密钥</span><strong>%s</strong><small>可撤销的服务端凭据</small></article></section>
                <section class='actions'><article><h2>预算与并发</h2><p>额度、默认输出与单次上限影响后续请求。</p><button onclick=\"showDialog('budget')\">编辑预算</button></article><article><h2>接入密钥</h2><p>密钥仅生成时显示一次；可随时撤销失效密钥。</p><button onclick=\"showDialog('new-key')\">生成 API Key</button></article><article><h2>运行与治理</h2><p>查看当前 Agent 的调用、评测门禁和发布记录。</p><a class='button' href='/console/requests?projectId=%s'>进入运行视图</a></article></section>
                <section class='panel keys'><div><span class='eyebrow'>CREDENTIALS</span><h2>接入密钥</h2></div><div class='key-list'>%s</div></section>
                %s%s
                """.formatted(options, escape(value(overview, "name")), format(number(overview, "availableTokens")), format(limit), format(consumed), format(reserved), percent, percent, number(overview, "activeRequests"), number(overview, "max_concurrency"), number(overview, "apiKeyCount"), navProject, keys, budgetDialog(selected, overview), keyDialog(selected));
        return page("Agent 平台", main + createDialog());
    }

    /** Creates an isolated Agent product and returns the operator directly to its dashboard. */
    @PostMapping("/createProject")
    public ResponseEntity<Void> createProject(@RequestParam String newProjectId, @RequestParam String name, @RequestParam long tokenLimit, @RequestParam int maxConcurrency, @RequestParam int defaultMaxTokens, @RequestParam int projectMaxTokens) {
        projects.createProject(new ProjectAdministrationService.CreateProjectRequest(newProjectId, name, tokenLimit, maxConcurrency, defaultMaxTokens, projectMaxTokens));
        return redirect(newProjectId);
    }

    /** Persists budget controls for the selected Agent product. */
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
        String body = "<section class='secret panel'><span class='eyebrow'>SAVE THIS NOW</span><h1>新的 API Key</h1><p>关闭或刷新后无法再次查看。请把它保存到服务端密钥库。</p><code>" + secret + "</code><p><a class='button' href='/console/platform?projectId=" + escape(projectId) + "'>我已保存，返回管理页</a></p></section>";
        return ResponseEntity.ok().header(HttpHeaders.CACHE_CONTROL, "no-store, max-age=0").header("Pragma", "no-cache").body(page("保存 API Key", body));
    }

    /** Updates a key's enabled state without exposing the secret again. */
    @PostMapping("/updateProjectApiKeyStatus")
    public ResponseEntity<Void> updateProjectApiKeyStatus(@RequestParam String projectId, @RequestParam String apiKeyId, @RequestParam boolean enabled) {
        projects.updateProjectApiKeyStatus(projectId, apiKeyId, new ProjectAdministrationService.UpdateApiKeyStatusRequest(enabled));
        return redirect(projectId);
    }

    /** Builds rows from hashed key metadata; plaintext key material never returns to this page. */
    private String keyRows(String projectId) {
        if (projectId.isBlank()) return "";
        List<Map<String, Object>> keys = projects.queryProjectApiKeys(projectId);
        if (keys.isEmpty()) return "<div class='empty'>尚未创建密钥。生成一把密钥后，将它配置到 Agent 服务端。</div>";
        StringBuilder rows = new StringBuilder();
        for (Map<String, Object> key : keys) {
            boolean enabled = Boolean.parseBoolean(value(key, "enabled"));
            rows.append("<div class='key-row'><div><b>").append(escape(value(key, "api_key_id"))).append("</b><small>创建于 ").append(escape(value(key, "created_at"))).append("</small></div><span class='status ").append(enabled ? "good'>启用" : "off'>已撤销").append("</span>");
            if (enabled) rows.append("<form method='post' action='/console/platform/updateProjectApiKeyStatus'><input type='hidden' name='projectId' value='").append(escape(projectId)).append("'><input type='hidden' name='apiKeyId' value='").append(escape(value(key, "api_key_id"))).append("'><input type='hidden' name='enabled' value='false'><button class='danger'>撤销</button></form>");
            rows.append("</div>");
        }
        return rows.toString();
    }

    /** Provides the intentionally empty onboarding state when no Agent has been connected. */
    private String emptyState() { return "<section class='empty-state'><span class='eyebrow'>MULTI-AGENT READY</span><h1>先接入第一个 Agent 产品</h1><p>每个 Agent 都会获得独立的 Token 预算、并发上限、API Key、调用记录和发布治理空间。</p><button onclick=\"showDialog('create-agent')\">＋ 添加 Agent</button></section>"; }
    /** Renders quota inputs only after the operator deliberately opens the budget dialog. */
    private String budgetDialog(String projectId, Map<String, Object> row) { return "<dialog id='budget'><form method='dialog' class='dialog-head'><b>编辑预算与并发</b><button class='icon'>×</button></form><form method='post' action='/console/platform/updateProjectQuota' class='dialog-form'><input type='hidden' name='projectId' value='" + escape(projectId) + "'><label>累计 Token 额度<input required min='1' type='number' name='tokenLimit' value='" + number(row,"token_limit") + "'></label><label>最大并发请求<input required min='1' type='number' name='maxConcurrency' value='" + number(row,"max_concurrency") + "'></label><details><summary>高级输出策略</summary><label>默认输出 Token<input required min='1' type='number' name='defaultMaxTokens' value='" + number(row,"default_max_tokens") + "'></label><label>单次输出上限<input required min='1' type='number' name='projectMaxTokens' value='" + number(row,"project_max_tokens") + "'></label></details><button>保存变更</button></form></dialog>"; }
    /** Renders the one-time key generator behind a deliberate user action. */
    private String keyDialog(String projectId) { return "<dialog id='new-key'><form method='dialog' class='dialog-head'><b>生成 API Key</b><button class='icon'>×</button></form><p>密钥只显示一次，请保存至 Agent 服务端。</p><form method='post' action='/console/platform/createProjectApiKey' class='dialog-form'><input type='hidden' name='projectId' value='" + escape(projectId) + "'><label>用途标签<input name='label' placeholder='production-gateway'></label><button>生成并查看</button></form></dialog>"; }
    /** Renders lean onboarding inputs with advanced quota choices folded by default. */
    private String createDialog() { return "<dialog id='create-agent'><form method='dialog' class='dialog-head'><b>接入新的 Agent 产品</b><button class='icon'>×</button></form><p>先完成身份信息；预算策略可直接使用安全默认值。</p><form method='post' action='/console/platform/createProject' class='dialog-form'><label>Agent 名称<input required name='name' placeholder='Fund Research Agent' oninput=\"suggestProjectId(this.value)\"></label><label>项目 ID<input required pattern='[a-z0-9-]+' name='newProjectId' id='newProjectId' value='project-agent'></label><details><summary>高级预算策略</summary><label>Token 额度<input required min='1' type='number' name='tokenLimit' value='1000000'></label><label>最大并发<input required min='1' type='number' name='maxConcurrency' value='32'></label><label>默认输出 Token<input required min='1' type='number' name='defaultMaxTokens' value='1024'></label><label>单次输出上限<input required min='1' type='number' name='projectMaxTokens' value='4096'></label></details><button>创建并进入 Agent</button></form></dialog>"; }
    /** Creates a redirect that preserves the selected project in browser navigation. */
    private ResponseEntity<Void> redirect(String projectId) { return ResponseEntity.status(303).location(URI.create("/console/platform?projectId=" + projectId)).build(); }
    /** Renders the shared deep-blue visual language used throughout the operator console. */
    private String page(String title, String body) { return """
        <!doctype html><html lang='zh-CN'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>AgentOps · %s</title><style>
        :root{font-family:Inter,"Microsoft YaHei",sans-serif;color:#eaf2ff;background:#081321}*{box-sizing:border-box}body{margin:0;min-height:100vh;background:radial-gradient(circle at 82%% -10%%,#1a4874 0,#081321 48%%)}header,main{max-width:1180px;margin:auto}header{height:70px;display:flex;align-items:center;justify-content:space-between;padding:0 20px}header b{letter-spacing:.3px}.nav a,.link{color:#a9c9ff;text-decoration:none;margin-left:18px;font-size:14px}main{padding:16px 20px 64px}.switcher{display:flex;gap:14px;align-items:center;margin-bottom:26px}.switcher form{margin-right:auto}.switcher label,.switcher select{font-size:13px;color:#b9c8dd}.switcher select{margin-left:8px;padding:8px;background:#12253c;color:#fff;border:1px solid #315574;border-radius:8px}.isolation{color:#65e6c6;font-size:12px}.hero{display:flex;justify-content:space-between;gap:24px;padding:16px 0 26px}.eyebrow{color:#5ce0c0;font-size:12px;letter-spacing:1.6px}.hero h1,.empty-state h1{font-size:36px;margin:10px 0}.hero p,.actions p,dialog p,.empty-state p{line-height:1.65;color:#9eb1ca;max-width:660px}.available{min-width:216px;padding:22px;border:1px solid #2b678b;background:#102c48;border-radius:18px;text-align:center}.available b{display:block;font-size:30px;color:#69efce}.available span{color:#bdd0e5;font-size:13px}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:14px}.cards article,.panel,.actions article{background:#0f2034;border:1px solid #29445f;border-radius:15px;padding:18px}.cards span,.cards small,.key-row small{display:block;color:#93a9c1;font-size:12px}.cards strong{display:block;font-size:25px;margin:10px 0}.meter{height:7px;border-radius:9px;background:#29445f;margin:14px 0 8px;overflow:hidden}.meter i{display:block;height:100%%;background:linear-gradient(90deg,#5ce0c0,#61aaff)}.actions{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin:16px 0}.actions h2,.panel h2{margin:0;font-size:18px}.actions p{font-size:13px}.button,button{display:inline-block;border:0;border-radius:8px;padding:10px 13px;background:#62abff;color:#071321;text-decoration:none;font-weight:700;cursor:pointer}.quiet{background:transparent;color:#a9c9ff;border:1px solid #3a6388}.keys{margin-top:16px}.key-list{margin-top:13px}.key-row{display:flex;gap:14px;align-items:center;padding:12px 0;border-top:1px solid #29445f}.key-row>div{margin-right:auto}.key-row b{font-size:13px}.status{border-radius:999px;padding:4px 8px;font-size:12px}.good{color:#5ce0c0;background:#123d3c}.off{color:#a9b8ca;background:#263748}.danger{background:#372438;color:#ffc0cf}.empty,.empty-state{color:#9eb1ca}.empty-state{max-width:660px;margin:100px auto;text-align:center}.empty-state button{margin-top:12px}dialog{width:min(520px,calc(100%% - 30px));border:1px solid #386281;border-radius:16px;background:#102238;color:#edf5ff;padding:22px;box-shadow:0 24px 80px #0009}dialog::backdrop{background:#020812bb}.dialog-head{display:flex;justify-content:space-between;align-items:center}.icon{padding:0;background:none;color:#d8e7f8;font-size:24px}.dialog-form{display:grid;gap:13px;margin-top:16px}.dialog-form label{color:#a9bdd4;font-size:13px}.dialog-form input{display:block;width:100%%;margin-top:6px;padding:10px;border-radius:8px;border:1px solid #39536f;background:#091727;color:#fff}details{border:1px solid #2c4b68;border-radius:9px;padding:11px;color:#bdd0e5}details label{margin-top:10px}.secret{max-width:720px;margin:80px auto}.secret code{display:block;margin:18px 0;padding:16px;border:1px solid #5ce0c0;border-radius:8px;background:#07121e;color:#65e6c6;word-break:break-all}@media(max-width:760px){.cards,.actions{grid-template-columns:1fr}.hero,.switcher{display:block}.available{margin-top:16px}.switcher>*{margin:8px 0}.key-row{align-items:flex-start;flex-wrap:wrap}.nav{display:none}}</style></head><body><header><b>AgentOps Lite</b><nav class='nav'><a href='/console/platform'>Agent 与预算</a><a href='/console/requests'>调用记录</a><a href='/console/evaluations'>评测门禁</a><a href='/console/releases'>发布治理</a></nav></header><main>%s</main><script>function showDialog(id){document.getElementById(id).showModal()}function suggestProjectId(name){var slug=name.toLowerCase().trim().replace(/[^a-z0-9]+/g,'-').replace(/^-|-$/g,'');document.getElementById('newProjectId').value='project-'+(slug||'agent')}</script></body></html>
        """.formatted(escape(title), body); }
    private static long number(Map<String, Object> row, String key) { Object value = row.get(key); return value instanceof Number number ? number.longValue() : 0; }
    private static String value(Map<String, Object> row, String key) { Object value = row.get(key); return value == null ? "" : String.valueOf(value); }
    private static String format(long value) { return String.format("%,d", value); }
    private static String escape(String value) { return HtmlUtils.htmlEscape(value == null ? "" : value); }
}
