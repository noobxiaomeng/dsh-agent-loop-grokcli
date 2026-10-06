/**
 * dsh-agent-loop-grokcli —— 把 GrokCLI（xAI grok-build）注册为 dsh 主引擎的桥驱动插件。
 *
 * 用法（源码树 dev 回路）：
 *   overlay.yml:
 *     - id: agent-loop
 *       disabled: true            # 顶掉默认引擎（setFactory 单槽互斥）
 *     - insert:
 *         - id: grokcli-bridge
 *           name: '/absolute/path/to/dsh-agent-loop-grokcli/src/index.ts'   # 绝对路径，正斜杠
 *           config:
 *             grokBin: '~/.grok/bin/grok.exe'  # 省略则自动探测官方安装位
 *             realHome: '~'             # 省略则取当前用户主目录
 *             cwd: '/your/workspace'
 *             defaultModel: 'grok-4.7'
 *             reasoningEffort: 'low'
 *   启动：pnpm dsh web --patch ./overlay.yml
 *
 * 事件面：turn/start→user/message→step/start→request/header→(agent/assistant-stream 直播帧)
 *        →tool/call|tool/result→assistant/message→step/end→turn/end（session.append 提交）。
 * 权限桥：grok session/request_permission → ctx.approval.request → 原生审批 UI。
 */
import type { Context } from "@deepseek-ai/cordis";
import { execFileSync } from "node:child_process";
import { existsSync, readdirSync, rmSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { GrokBridgeFactory, type BridgeConfig } from "./bridge.ts";
export { AcpDriver } from "./acp-driver.ts";
export { ModelPinningProxy, usageSince } from "./model-pinning-proxy.ts";
export const name = "grokcli-bridge";
// credentials 服务必须声明 inject 才可从本插件访问（Cordis DI：服务按 inject 挂载到本 fiber）；
// configEditor 是 accessor，无需 inject。userQuestions **不能进 inject**：可选注入语法
// （'userQuestions?'）本版 cordis 不认会整插件拒载（2026-10-05 实测坑）；桥内用
// try/catch 属性访问兜底，取不到自动降级文本问答流。
export const inject = ["agents", "sessions", "sessionProjections", "credentials"];

/** turnBoundary 投影（照抄 agent-loop/src/index.ts:46-95 的纯 fold；UI 的 turn 状态读它） */
const turnBoundaryProjection = {
  key: "turnBoundary",
  stateVersion: 2,
  init: () => ({
    openTurnStartSeq: null,
    lastStepStartSeq: null,
    lastStepBoundary: null,
    lastTurn: 0,
  }),
  apply: (state: Record<string, unknown>, event: { type: string; seq: number; data: { turn?: number } }) => {
    switch (event.type) {
      case "turn/start":
        return { ...state, openTurnStartSeq: event.seq, lastTurn: event.data.turn };
      case "turn/end":
        return { ...state, openTurnStartSeq: null };
      case "step/start":
        return { ...state, lastStepStartSeq: event.seq, lastStepBoundary: { kind: "start", seq: event.seq } };
      case "step/end":
        return { ...state, lastStepBoundary: { kind: "end", seq: event.seq } };
      default:
        return state;
    }
  },
};

export function apply(ctx: Context, config: BridgeConfig) {
  // 路径默认值通用化（开源形态）：grokBin 落 GrokCLI 官方安装位（~/.grok/bin），
  // realHome 落当前用户主目录，cwd 落 dsh 进程工作目录——本机特化（隔离 alpha 副本等）
  // 由 profile 手工 patch 层的 id-targeted config override 提供，不进仓库。
  const conf: BridgeConfig = {
    grokBin: config?.grokBin || (process.platform === "win32"
      ? join(homedir(), ".grok", "bin", "grok.exe")
      : join(homedir(), ".grok", "bin", "grok")),
    realHome: config?.realHome || homedir(),
    cwd: config?.cwd || process.cwd(),
    defaultModel: config?.defaultModel,
    reasoningEffort: config?.reasoningEffort,
    profiles: config?.profiles,
    pinModel: config?.pinModel,
    retryAbortMs: config?.retryAbortMs,
    promptIdleMs: config?.promptIdleMs,
    promptHardMs: config?.promptHardMs,
  };
  console.log(`[grokcli-bridge] loaded (grok=${conf.grokBin}, model=${conf.defaultModel || "(default profile)"}, effort=${conf.reasoningEffort || "(default)"})`);

  const projections = (ctx as unknown as {
    sessionProjections?: { register(def: unknown): void };
  }).sessionProjections;
  projections?.register(turnBoundaryProjection);

  const factory = new GrokBridgeFactory(ctx, conf);
  ctx.effect(() => ctx.agents.setFactory(factory as never), "grokcli.setFactory()");

  // 设置桥：读「设置 → 模型 → 自定义模型 API」（llm-pi-ai 段）为 grok 档案；
  // Models 页保存时 settings/document-updated 触发重读（baseURL/协议/模型清单 + credentials 密钥）。
  void factory.refreshProfiles();
  ctx.on("settings/document-updated" as never, ((ns: string) => {
    if (ns === "llm-pi-ai") void factory.refreshProfiles();
  }) as never);

  // ── 前端「永久删除」执行端点（2026-10-06）：dsh 原生只有归档没有删除；侧栏菜单项
  // （client 模块注册）POST 到这里。编排 = archive（归档集推送 → 前端列表即时隐藏）
  // → 物理删（磁盘目录 + projcache）→ unarchive 清归档集（触发 workspace 候选账目
  // prune，残项自动剪除）——三层都走官方事件/账目链，前端无需额外刷新逻辑。
  ctx.inject(["webServer"], (webCtx: Context) => {
    webCtx.effect(() => webCtx.webServer.register({
      kind: "exact",
      path: "/grokdesk/delete-session",
      handler: async (req, res) => {
        let body = "";
        for await (const chunk of req) body += String(chunk);
        let sessionId = "";
        try { sessionId = String((JSON.parse(body) || {}).sessionId ?? ""); } catch { /* bad json */ }
        res.setHeader("content-type", "application/json; charset=utf-8");
        if (!/^session-[0-9a-f-]{30,}$/i.test(sessionId)) {
          res.statusCode = 400;
          res.end(JSON.stringify({ ok: false, error: "bad sessionId" }));
          return;
        }
        res.end(JSON.stringify(await deleteSessionCompletely(ctx, sessionId)));
      },
    }), "grokcli.delete-session route");
  });

  // ── 斜杠命令（2026-10-06 ·「更像 zcode」第二批；commands 服务 = 插件级人命令注册表，
  // handler 不进模型上下文）。Cordis 铁律：服务获取整体 try/catch，取不到静默降级。
  const commandsSvc = (() => {
    try { return (ctx as unknown as { get(n: string): unknown }).get("commands") as { register(def: unknown): () => void } | undefined; }
    catch { return undefined; }
  })();
  if (commandsSvc?.register) {
    const versionCache = new Map<string, string>();
    const grokVersion = (bin: string): string => {
      const hit = versionCache.get(bin);
      if (hit) return hit;
      try {
        const out = execFileSync(bin, ["--version"], { encoding: "utf8", timeout: 10_000 }).trim();
        versionCache.set(bin, out);
        return out;
      } catch (e) {
        return `版本获取失败: ${String(e).slice(0, 60)}`;
      }
    };
    ctx.effect(() => commandsSvc.register({
      name: "grokstatus",
      description: "grok 引擎状态：版本/路径/当前会话绑定/子代理镜像/pin 配置",
      handler: (inv: { agent?: unknown }) => {
        const lines = [grokVersion(conf.grokBin), `grokBin: ${conf.grokBin}`];
        const profileNames = Object.keys(conf.profiles ?? {});
        lines.push(profileNames.length ? `档案通道: ${profileNames.join(", ")}` : "档案通道: （无，走用户默认档案）");
        const a = inv.agent as { acpSessionId?: string | null; subagentMirrors?: Map<string, unknown>; boundWantModel?: string } | undefined;
        if (a && typeof a === "object" && "acpSessionId" in a) {
          lines.push(a.acpSessionId ? `当前 grok 会话: ${String(a.acpSessionId).slice(0, 8)}` : "当前 grok 会话: （未建立）");
          lines.push(`模型: ${a.boundWantModel || conf.defaultModel || "(默认)"}`);
          lines.push(`子代理镜像: ${a.subagentMirrors?.size ?? 0} 个运行中`);
        }
        lines.push(`pin 模型转发: ${conf.pinModel === false ? "关闭" : "开启"}`);
        return { kind: "success", text: lines.join("\n") };
      },
    } as never), "grokcli.commands()");
    ctx.effect(() => commandsSvc.register({
      name: "grokclean",
      description: "会话清理：无参=统计（总数/归档数）；archived=删除全部已归档会话（磁盘+projcache+注册表——dsh 原生只有归档没有删除，本命令补位）",
      input: { hint: "archived = 删除全部已归档会话" },
      handler: async (inv: { agent?: unknown; rawInput?: string }) => {
        const arg = (inv.rawInput ?? "").trim();
        let registry: { archivedSessionIds: readonly string[]; unarchiveSession(id: string): Promise<void> } | undefined;
        try { registry = (ctx as unknown as { get(n: string): unknown }).get("workspaceRegistry") as typeof registry; } catch { registry = undefined; }
        const archived = registry?.archivedSessionIds ?? [];
        const sessionsRoot = join(homedir(), ".dsh", "sessions");
        const projcacheDir = join(homedir(), ".dsh", "storages", "session_projcache", "sessions");
        const all: string[] = [];
        try {
          for (const ws of readdirSync(sessionsRoot)) {
            for (const d of readdirSync(`${sessionsRoot}/${ws}`)) if (d.startsWith("session-")) all.push(d);
          }
        } catch { /* 无会话目录 */ }
        if (arg !== "archived") {
          return { kind: "success", text: [`会话总数: ${all.length}`, `已归档: ${archived.length}${archived.length ? "\n  " + archived.map(id => id.slice(0, 18)).join("\n  ") : ""}`, "", "删除全部归档会话：/grokclean archived"].join("\n") };
        }
        // 删除归档会话：磁盘目录 + projcache 条目 + 注册表归档集（workspace 候选账目缺 header 自动剪除）
        const deleted: string[] = [];
        for (const id of archived) {
          const uuid = id.replace(/^session-/, "");
          let removedDir = false;
          try {
            for (const ws of readdirSync(sessionsRoot)) {
              const dir = `${sessionsRoot}/${ws}/${id}`;
              if (existsSync(dir)) { rmSync(dir, { recursive: true, force: true }); removedDir = true; }
            }
          } catch { /* readdir 失败跳过 */ }
          try { rmSync(`${projcacheDir}/${uuid}.json`, { force: true }); } catch { /* projcache 条目可缺 */ }
          try { rmSync(`${projcacheDir}/${id}.json`, { force: true }); } catch { /* 两种文件名形态都清 */ }
          try { await registry?.unarchiveSession(id); } catch (e) { console.log(`[grokcli] grokclean unarchive ${id.slice(0, 14)} failed: ${String(e).slice(0, 140)}`); }
          deleted.push(`${id.slice(0, 18)}${removedDir ? "" : "（目录已不在，仅清注册表）"}`);
        }
        return { kind: "success", text: [`已删除 ${deleted.length} 个归档会话：`, ...deleted.map(d => `  ${d}`), "", "（workspace 成员账目随下次变更自动剪除残项）"].join("\n") };
      },
    } as never), "grokcli.commands.grokclean()");
    console.log("[grokcli-bridge] command registered: /grokstatus, /grokclean");
  }
}

/** 单会话彻底删除（archive → 磁盘 + projcache → unarchive 清归档集），前端删除按钮的执行体。 */
async function deleteSessionCompletely(ctx: Context, sessionId: string): Promise<{ ok: boolean; removedDir: boolean; notes: string[] }> {
  const sessionsRoot = join(homedir(), ".dsh", "sessions");
  const projcacheDir = join(homedir(), ".dsh", "storages", "session_projcache", "sessions");
  let registry: {
    archiveSession?(id: string, o?: Record<string, unknown>): Promise<void>;
    unarchiveSession?(id: string): Promise<void>;
  } | undefined;
  try { registry = (ctx as unknown as { get(n: string): unknown }).get("workspaceRegistry") as typeof registry; } catch { registry = undefined; }
  const notes: string[] = [];
  try { await registry?.archiveSession?.(sessionId); } catch (e) { notes.push(`archive: ${String(e).slice(0, 90)}`); }
  let removedDir = false;
  try {
    for (const ws of readdirSync(sessionsRoot)) {
      const dir = join(sessionsRoot, ws, sessionId);
      if (existsSync(dir)) { rmSync(dir, { recursive: true, force: true }); removedDir = true; }
    }
  } catch { /* readdir 失败跳过 */ }
  // projcache 文件名两种形态都清（实测见 session- 前缀形态；uuid 形态留兼容）
  for (const fn of [`${sessionId}.json`, `${sessionId.replace(/^session-/, "")}.json`]) {
    try { rmSync(join(projcacheDir, fn), { force: true }); } catch { /* 条目可缺 */ }
  }
  // unarchive 延迟一拍：archive 的归档集推送先让前端把行过滤掉（archivedSet 是响应式），
  // 立刻 unarchive 会让前端只见到最终态（归档集外+成员账目残项）——幽灵行当场复活
  // （实测踩坑）。8s 后清归档集，残项由后续 workspace mutation prune。
  setTimeout(() => {
    void registry?.unarchiveSession?.(sessionId).catch(() => {});
  }, 8_000);
  console.log(`[grokcli] session deleted: ${sessionId.slice(0, 18)} dir=${removedDir}${notes.length ? ` notes=${notes.join("; ")}` : ""}`);
  return { ok: true, removedDir, notes };
}
