/**
 * GrokCLI ACP 驱动层（dsh 插件用）
 * 移植自 GrokDesk 项目的 ACP 参考实现（grok-app-server.cjs，已对 grok v1.0.41 实测）。
 *
 * 职责：spawn `grok agent [-m profile] [-e effort] stdio`（ACP over stdio，JSON-RPC LF 分帧），
 * 管理 per-(model,effort) 连接池，处理 initialize/session/new/prompt/cancel、
 * 权限请求回调、_x.ai retry_state 可见化+止损。
 *
 * 事实依据（全部实测，见 reports/HANDOVER-dsh-route.md §三）：
 * - -m/-e 是 `grok agent` 父命令选项，必须在 stdio 子命令之前（clap 拒启）；
 * - 认证走真实 HOME（~/.grok/config.toml 的模型档案），子进程必须给真实 HOME/USERPROFILE；
 * - 中转对部分模型只开 chat-completions：裸模型走默认档案 responses 会无限退避重试
 *   （retry_state），必须显式走档案通道（-m grokdesk-<id>）并动态写档案 model 字段；
 * - prompt 的 resolve 即回合结束（stopReason: end_turn|cancelled|...）；
 * - 会话绑定模型进程：换模型/换档位 = 换连接 = 换会话。
 */
import { spawn, execFileSync } from "node:child_process";
import { createInterface } from "node:readline";
import { randomUUID } from "node:crypto";

export interface AcpPermissionOption {
  optionId: string;
  kind: "allow_once" | "allow_always" | "reject" | string;
  name?: string;
}

export interface AcpPermissionRequest {
  sessionId: string;
  toolCallId: string;
  title?: string;
  rawInput?: unknown;
  options: AcpPermissionOption[];
}

export type AcpPermissionDecision =
  | { outcome: "selected"; optionId: string }
  | { outcome: "rejected" };

/** 投影层关心的 ACP session/update 词汇（grok 实测全集的子集+透传） */
export interface AcpSessionUpdate {
  sessionUpdate: string;
  [key: string]: unknown;
}

export interface AcpRetryState {
  attempt: number;
  reason: string;
}

export interface AcpDriverHandlers {
  /** agent_message_chunk / agent_thought_chunk / tool_call / tool_call_update / session_info_update / ... */
  onUpdate(acpSessionId: string, update: AcpSessionUpdate): void;
  /** 权限请求：由宿主接审批 UI / 策略；返回 Promise，永不 resolve 的话 180s 超时自动拒绝 */
  onPermission(req: AcpPermissionRequest): Promise<AcpPermissionDecision>;
  /** retry_state（中转拒请求指数退避）：宿主做可见化 */
  onRetryState(acpSessionId: string, retry: AcpRetryState): void;
  /** grok 私有扩展 `_x.ai/exit_plan_mode`（服务端→客户端请求，2026-10-05 抓线实证：
   *  params {sessionId, toolCallId, planContent}；不回 result 则回合永久挂起——此前
   *  两个「计划模式卡死」事故的真正根因）。返回值即 JSON-RPC result：
   *  {outcome:"accepted"} 批准（grok 同回合继续实施）| {outcome:"abandoned"} 放弃。 */
  onExitPlanMode?(params: { sessionId?: string; toolCallId?: string; planContent?: string }): Promise<{ outcome: string; comments?: string }>;
  /** grok 私有扩展 `_x.ai/ask_user_question`（params {sessionId, toolCallId, questions, mode}）。
   *  result：{outcome:"accepted", answers:{"<问题>":"选项label"|[...]}} 或
   *  {outcome:"skip_interview"|"chat_about_this"|"cancelled"}（格式实证自 @1agents/acp-service）。 */
  onAskUserQuestion?(params: { sessionId?: string; toolCallId?: string; questions?: unknown; mode?: unknown }): Promise<{ outcome: string; answers?: Record<string, string | string[]> }>;
  /** 其他通知（_x.ai/queue/changed、sessions/changed、mcp/*...） */
  onNotification?(method: string, params: unknown): void;
  log?(msg: string, extra?: unknown): void;
}

interface Conn {
  key: string;
  child: import("node:child_process").ChildProcess | null;
  nextId: number;
  pending: Map<number, { resolve: (v: any) => void; reject: (e: Error) => void }>;
  ready: Promise<void> | null;
  stderrTail: string;
  /** acpSessionId -> true（本连接上建立的会话，连接死亡时清理用） */
  acpSessions: Set<string>;
}

export interface AcpSpawnOptions {
  /** grok 可执行文件绝对路径 */
  grokBin: string;
  /** 真实主目录（认证/会话在 ~/.grok） */
  realHome: string;
  /** ACP 子进程的 cwd（grok 会话落 ~/.grok/sessions/<encoded(cwd)>） */
  cwd: string;
  /** 模型档案（-m）：空 = 默认档案 */
  modelProfile?: string;
  /** 思考档位（-e）：low|high|xhigh...；进程级参数，进连接键 */
  reasoningEffort?: string;
  /** retry 止损阈值（默认 120s；硬错误类：鉴权/协议/参数，重试无意义快速失败） */
  retryAbortMs?: number;
  /** 同类重试到 N 次即止损（默认 5 次，约 60-70s；grok 退避节奏实测） */
  retryAbortAttempts?: number;
  /** 瞬态错误（上游 5xx/超时/限流/网络抖动）止损次数（默认 10；等待常自愈，2026-10-08 老大定标） */
  retryTransientAttempts?: number;
  /** 瞬态错误止损时长（默认 600s=10 分钟） */
  retryTransientMs?: number;
  /** prompt 空闲超时 ms（默认 600s：会话有任何新流量即续期。实测 grok-4.7@xhigh 长探索
   *  回合可合法跑 16 分钟+，固定 600s 会把正常回合误杀成 "ACP session/prompt timeout"） */
  promptIdleMs?: number;
  /** prompt 绝对上限 ms（默认 43200s=12h，防「持续有流量但永不收尾」的真死循环；
   *  常规挂死由空闲超时兜底，此上限只是最后护栏） */
  promptHardMs?: number;
}

const DEFAULT_ABORT_MS = 120_000;
const DEFAULT_ABORT_ATTEMPTS = 5;
/** 瞬态错误（上游 5xx/超时/限流）的放宽止损预算：等待常能自愈（老大定标 10 次 / 10 分钟） */
const DEFAULT_TRANSIENT_MS = 600_000;
const DEFAULT_TRANSIENT_ATTEMPTS = 10;
const PERM_TIMEOUT_MS = 180_000;
/** 只经 _x.ai/session_notification 送达、session/update 不送的 update 类别（抓线实证；
 *  其余类别两通道都会送，放行会造成双投递 → 重复 tool/call → 冷读取判损坏） */
const NOTIFICATION_ONLY_KINDS = new Set(["response_completed", "turn_completed", "turn_started", "subagent_spawned", "subagent_progress", "session_summary_generated"]);

/** 瞬态错误判定（上游 5xx/超时/限流/网络抖动）：等待常自愈，止损预算放宽；
 *  鉴权/协议/参数类硬错误重试无意义，维持紧止损。bridge 侧同用此分类决定
 *  止损后是否保留 grok 会话上下文。 */
export function isTransientRetryReason(reason: string): boolean {
  return /50[234]|upstream|temporar|unavail|rate.?limit|timeout|timed?\s*out|econn|reset|hang\s*up|network|connection|error sending request|send(ing)? request|fetch failed|overload|busy|too\s*many/i.test(reason);
}

export class AcpDriver {
  private pool = new Map<string, Conn>();
  /** acpSessionId -> { connKey, retryStartedAt, retry, aborted, pendingPrompt, lastRx } */
  private sessionInfo = new Map<string, { connKey: string; retryStartedAt: number; retry: AcpRetryState | null; aborted?: boolean; pendingPrompt?: boolean; lastRx: number }>();
  private permWaiters = new Map<string, { conn: Conn; jsonRpcId: number; timer: NodeJS.Timeout }>();
  private disposers: Array<() => void> = [];

  private handlers: AcpDriverHandlers;
  private opts: AcpSpawnOptions;

  constructor(handlers: AcpDriverHandlers, opts: AcpSpawnOptions) {
    this.handlers = handlers;
    this.opts = opts;
  }

  private log(msg: string, extra?: unknown) {
    this.handlers.log?.(msg, extra);
  }

  private connKey(): string {
    return `${this.opts.modelProfile || ""}|${this.opts.reasoningEffort || ""}`;
  }

  private write(conn: Conn, obj: unknown) {
    if (!conn.child || !conn.child.stdin) throw new Error("ACP connection not alive");
    conn.child.stdin.write(JSON.stringify(obj) + "\n");
  }

  request<T = any>(conn: Conn, method: string, params: unknown, timeoutMs = 120_000, keepAlive?: () => boolean): Promise<T> {
    const id = conn.nextId++;
    return new Promise<T>((resolve, reject) => {
      // keepAlive 模式 = 空闲超时：到期先问一次「有无新流量」，有则续期一个完整窗口。
      // 无 keepAlive（initialize/session/new 等短请求）行为与原固定超时一致。
      let deadline = Date.now() + timeoutMs;
      const timer = setInterval(() => {
        if (Date.now() < deadline) return;
        if (keepAlive?.()) { deadline = Date.now() + timeoutMs; return; }
        clearInterval(timer);
        conn.pending.delete(id);
        reject(new Error(`ACP ${method} timeout`));
      }, 5_000);
      conn.pending.set(id, {
        resolve: v => { clearInterval(timer); resolve(v); },
        reject: e => { clearInterval(timer); reject(e); },
      });
      try {
        this.write(conn, { jsonrpc: "2.0", id, method, params });
      } catch (e) {
        clearInterval(timer);
        conn.pending.delete(id);
        reject(e as Error);
      }
    });
  }

  /** 取/建连接（per modelProfile×effort），initialize 握手完成后可用 */
  async ensure(): Promise<Conn> {
    const key = this.connKey();
    let conn = this.pool.get(key);
    if (conn && conn.child && conn.ready) return conn;
    conn = { key, child: null, nextId: 1, pending: new Map(), ready: null, stderrTail: "", acpSessions: new Set() };
    this.pool.set(key, conn);
    conn.ready = new Promise<void>((resolve, reject) => {
      // 参数顺序铁律：-m/--reasoning-effort 是 `grok agent` 父命令选项，必须在 stdio 之前。
      // 实测 grok v1.0.41：-e 不存在（clap 报 unexpected argument exit 2），
      // 长旗标 --reasoning-effort 才是正身（别名 --effort）。
      // --no-plan 是**根命令**旗标（必须放 agent 之前；agent 子命令不认它，实测 exit 2）：
      // 计划模式的 exit_plan_mode 工具需要 TUI 审批面板（"scrollable preview + action
      // bar"），ACP 桥接环境无人能按确认键 → 工具执行永久挂起（2026-10-05 18:27 实锄件：
      // 回合卡死 13 分钟，grok events 终止于 exit_plan_mode 的 tool_execution phase）。
      const args = ["--no-plan", "agent"];
      if (this.opts.modelProfile) args.push("-m", this.opts.modelProfile);
      if (this.opts.reasoningEffort) args.push("--reasoning-effort", this.opts.reasoningEffort);
      args.push("stdio");
      const child = spawn(this.opts.grokBin, args, {
        stdio: ["pipe", "pipe", "pipe"],
        cwd: this.opts.cwd,
        env: { ...process.env, HOME: this.opts.realHome, USERPROFILE: this.opts.realHome },
        windowsHide: true,
      });
      conn.child = child;
      this.log("acp spawn", { key, pid: child.pid });
      child.stderr?.on("data", d => {
        conn.stderrTail = (conn.stderrTail + String(d)).slice(-300);
        this.log("acp stderr", String(d).slice(0, 200));
      });
      child.on("exit", code => {
        this.log("acp child exit", key, code, conn.stderrTail.slice(-120));
        conn.child = null;
        conn.ready = null;
        for (const [, p] of conn.pending) p.reject(new Error(`ACP connection lost (exit ${code})`));
        conn.pending.clear();
        for (const sid of conn.acpSessions) this.sessionInfo.delete(sid);
        conn.acpSessions.clear();
        for (const [iid, w] of this.permWaiters) {
          if (w.conn === conn) { clearTimeout(w.timer); this.permWaiters.delete(iid); }
        }
      });
      const rl = createInterface({ input: child.stdout!, terminal: false });
      let lineCount = 0;
      rl.on("line", line => {
        lineCount++;
        if (lineCount <= 3 || lineCount % 25 === 0) console.log(`[grokcli] acp rx line#${lineCount}: ${line.slice(0, 90)}`);
        if (!line.trim()) return;
        let msg: any;
        try { msg = JSON.parse(line); } catch { return; }
        // 装甲：任一分支抛异常都不能打断 readline 流（实测桌面版回合响应被静默丢失）
        try {
          // 会话级流量戳：prompt 空闲超时据此续期（正常回合的流式帧/工具事件持续到达）
          const rxSid = (msg as { params?: { sessionId?: unknown } })?.params?.sessionId;
          if (typeof rxSid === "string") {
            const si = this.sessionInfo.get(rxSid);
            if (si) si.lastRx = Date.now();
          }
          if (msg.id !== undefined && msg.result !== undefined && conn.pending.has(msg.id)) {
            conn.pending.get(msg.id)!.resolve(msg.result);
            conn.pending.delete(msg.id);
          } else if (msg.id !== undefined && msg.error !== undefined && conn.pending.has(msg.id)) {
            conn.pending.get(msg.id)!.reject(new Error(JSON.stringify(msg.error)));
            conn.pending.delete(msg.id);
          } else if (msg.id !== undefined && msg.method === "session/request_permission") {
            this.handlePermission(conn, msg.id, msg.params || {});
          } else if (msg.id !== undefined && typeof msg.method === "string") {
            // 服务端→客户端请求（带 id 等 result）。grok 私有扩展的交互语义在此：
            // 不回 result = 工具永久挂起（计划模式/提问卡死事故根因，抓线实证）。
            this.handleServerRequest(conn, msg.id, msg.method, msg.params || {});
          } else if (msg.method === "session/update") {
            this.handlers.onUpdate(msg.params?.sessionId, (msg.params || {}).update || {});
          } else if (msg.method === "_x.ai/session_notification") {
            const u = (msg.params && msg.params.update) || {};
            if (u.sessionUpdate === "retry_state") this.handleRetryState(conn, msg.params || {}, u);
            // 通道分工去重（2026-10-05 实测踩坑：tool_call 等会经 session/update 与本通道
            // 双投递，写重 tool/call 毒化会话）：本通道只放行 session/update 不送的类别
            else if (NOTIFICATION_ONLY_KINDS.has(String(u.sessionUpdate))) {
              this.handlers.onUpdate(msg.params?.sessionId, u);
            }
          } else if (typeof msg.method === "string") {
            this.handlers.onNotification?.(msg.method, msg.params);
          }
        } catch (e) {
          console.log(`[grokcli] acp line handler error: ${String(e).slice(0, 200)} | line=${line.slice(0, 120)}`);
        }
      });
      this.request(conn, "initialize", {
        protocolVersion: 1,
        clientCapabilities: { fs: { readTextFile: false, writeTextFile: false } },
      }, 15_000).then(() => resolve(), reject);
    });
    await conn.ready;
    return conn;
  }

  /** 恢复已存在的 grok 会话（跨进程重启；~/.grok/sessions 落盘）。成功返回会话 id，失败返回 null。 */
  async loadSession(acpSessionId: string): Promise<string | null> {
    const conn = await this.ensure();
    try {
      await this.request(conn, "session/load", {
        sessionId: acpSessionId,
        cwd: this.opts.cwd,
        mcpServers: [], // mcpServers 必填（实测缺省报 Invalid params）
      }, 30_000);
      if (!this.sessionInfo.has(acpSessionId)) {
        conn.acpSessions.add(acpSessionId);
        this.sessionInfo.set(acpSessionId, { connKey: conn.key, retryStartedAt: 0, retry: null, lastRx: 0 });
      }
      this.log("acp session/load ok", { sid: acpSessionId.slice(0, 8) });
      return acpSessionId;
    } catch (e) {
      this.log("acp session/load failed", `${String(e).slice(0, 120)} sid=${acpSessionId.slice(0, 8)}`);
      return null;
    }
  }

  /** 新建 ACP 会话 */
  async newSession(): Promise<string> {
    const conn = await this.ensure();
    const sess = await this.request<{ sessionId: string }>(conn, "session/new", {
      cwd: this.opts.cwd,
      mcpServers: [],
    });
    conn.acpSessions.add(sess.sessionId);
    this.sessionInfo.set(sess.sessionId, { connKey: conn.key, retryStartedAt: 0, retry: null, lastRx: 0 });
    this.log("acp session/new", { acp: sess.sessionId, connKey: conn.key });
    return sess.sessionId;
  }

  /** 发起回合；resolve 即回合结束（返回 stopReason）。
   *  timeoutMs 是「空闲窗口」而非回合时长上限：会话有任何新流量（流式帧/工具事件/
   *  retry_state）就续期，绝对上限 promptHardMs 兜底。实测教训（2026-10-05 06:51 事故）：
   *  grok-4.7@xhigh 探索整目录的长回合合法跑了 16m18s 且每分钟都有工具调用，固定 600s
   *  把它误杀成 "ACP session/prompt timeout"，grok 侧还孤儿白跑 7 分钟。 */
  async prompt(acpSessionId: string, text: string, timeoutMs?: number): Promise<string> {
    const conn = await this.ensure();
    const idleMs = timeoutMs ?? this.opts.promptIdleMs ?? 600_000;
    const hardMs = this.opts.promptHardMs ?? 43_200_000;
    const info = this.sessionInfo.get(acpSessionId);
    if (info) { info.retryStartedAt = 0; info.retry = null; info.aborted = false; info.pendingPrompt = true; info.lastRx = 0; }
    const startedAt = Date.now();
    let seenRx = 0;
    const keepAlive = () => {
      if (Date.now() - startedAt >= hardMs) return false; // 绝对上限到头，不再续期
      const cur = this.sessionInfo.get(acpSessionId)?.lastRx ?? 0;
      if (cur > seenRx) { seenRx = cur; return true; } // 有新流量 → 续期
      return false;
    };
    try {
      const result = await this.request<{ stopReason: string }>(conn, "session/prompt", {
        sessionId: acpSessionId,
        prompt: [{ type: "text", text }],
      }, idleMs, keepAlive);
      console.log(`[grokcli] prompt resolved: stopReason=${result?.stopReason}`);
      return result?.stopReason || "end_turn";
    } finally {
      if (info) info.pendingPrompt = false;
    }
  }

  async cancel(acpSessionId: string): Promise<void> {
    const conn = await this.ensure();
    // ACP 规范里 session/cancel 是通知（无 id 无响应）；grok 对 request 形式回 -32601（实测）。
    try {
      this.write(conn, { jsonrpc: "2.0", method: "session/cancel", params: { sessionId: acpSessionId } });
    } catch (e) {
      this.log("acp cancel notify failed", String(e));
    }
    // 兜底：8s 后 prompt 仍挂着 → 杀连接（prompt 会以 connection lost 结束，绝不无限空转）
    const info = this.sessionInfo.get(acpSessionId);
    setTimeout(() => {
      const cur = this.sessionInfo.get(acpSessionId);
      if (cur?.pendingPrompt && conn.child) {
        this.log("acp cancel escalation: kill connection", { sid: acpSessionId.slice(0, 8) });
        try {
          // Windows 树杀（/T 连子进程 /F 强制）：child.kill() 对个别场景失效（ECONNRESET
          // 孤儿实测），taskkill 兜底
          const pid = conn.child.pid;
          conn.child.kill();
          if (pid) { try { execFileSync("taskkill", ["/PID", String(pid), "/T", "/F"], { stdio: "ignore" }); } catch { /* 已死则忽略 */ } }
        } catch {}
      }
      void info;
    }, 8_000).unref?.();
  }

  private connOfSession(acpSessionId: string): Conn | null {
    const info = this.sessionInfo.get(acpSessionId);
    if (!info) return null;
    const conn = this.pool.get(info.connKey);
    return conn && conn.child ? conn : null;
  }

  // ── grok 私有扩展请求（_x.ai/*）：计划审批与用户提问的真协议应答 ─────────────
  private handleServerRequest(conn: Conn, jsonRpcId: number, method: string, params: any): void {
    const INTERACTIVE_TIMEOUT_MS = 15 * 60_000;
    const answer = (result: unknown) => {
      try { this.write(conn, { jsonrpc: "2.0", id: jsonRpcId, result }); }
      catch (e) { this.log("acp server-request answer failed", String(e)); }
    };
    if (method === "_x.ai/exit_plan_mode") {
      console.log(`[grokcli] _x.ai/exit_plan_mode request (plan ${String(params?.planContent ?? "").length}B)`);
      const fallback = { outcome: "abandoned" };
      if (!this.handlers.onExitPlanMode) { answer(fallback); return; }
      const timer = setTimeout(() => {
        console.log("[grokcli] _x.ai/exit_plan_mode 15min 超时 -> abandoned");
        answer(fallback);
      }, INTERACTIVE_TIMEOUT_MS);
      timer.unref?.();
      void Promise.resolve(this.handlers.onExitPlanMode(params)).then(
        r => { clearTimeout(timer); answer(r ?? fallback); },
        e => { clearTimeout(timer); this.log("onExitPlanMode handler error", String(e)); answer(fallback); },
      );
      return;
    }
    if (method === "_x.ai/ask_user_question") {
      const qCount = Array.isArray(params?.questions) ? params.questions.length : 0;
      console.log(`[grokcli] _x.ai/ask_user_question request (${qCount} 问)`);
      const fallback = { outcome: "cancelled" };
      if (!this.handlers.onAskUserQuestion) { answer(fallback); return; }
      const timer = setTimeout(() => {
        console.log("[grokcli] _x.ai/ask_user_question 15min 超时 -> cancelled");
        answer(fallback);
      }, INTERACTIVE_TIMEOUT_MS);
      timer.unref?.();
      void Promise.resolve(this.handlers.onAskUserQuestion(params)).then(
        r => { clearTimeout(timer); answer(r ?? fallback); },
        e => { clearTimeout(timer); this.log("onAskUserQuestion handler error", String(e)); answer(fallback); },
      );
      return;
    }
    // 未知的服务端请求：打日志并回 method-not-found，绝不让 grok 悬等
    console.log(`[grokcli] unhandled server request id=${jsonRpcId} method=${method}`);
    try { this.write(conn, { jsonrpc: "2.0", id: jsonRpcId, error: { code: -32601, message: `method not found: ${method}` } }); } catch {}
  }

  // ── 权限桥 ──────────────────────────────────────────────────────────────────
  private handlePermission(conn: Conn, jsonRpcId: number, params: any) {    const req: AcpPermissionRequest = {
      sessionId: params.sessionId,
      toolCallId: params.toolCallId || "unknown",
      title: params.title,
      rawInput: params.rawInput,
      options: params.options || [],
    };
    const interactionId = "perm-" + randomUUID();
    const timer = setTimeout(() => {
      // 超时 fail-closed：自动拒绝
      const w = this.permWaiters.get(interactionId);
      if (w) {
        this.permWaiters.delete(interactionId);
        this.answerPermission(w.conn, w.jsonRpcId, { outcome: "rejected" });
      }
    }, PERM_TIMEOUT_MS);
    this.permWaiters.set(interactionId, { conn, jsonRpcId, timer });
    this.handlers.onPermission(req).then(
      decision => {
        const w = this.permWaiters.get(interactionId);
        if (!w) return; // 已被超时路径处理
        clearTimeout(w.timer);
        this.permWaiters.delete(interactionId);
        this.answerPermission(w.conn, w.jsonRpcId, decision);
      },
      err => {
        const w = this.permWaiters.get(interactionId);
        if (!w) return;
        clearTimeout(w.timer);
        this.permWaiters.delete(interactionId);
        this.log("acp permission handler error", String(err));
        this.answerPermission(w.conn, w.jsonRpcId, { outcome: "rejected" });
      },
    );
  }

  private answerPermission(conn: Conn, jsonRpcId: number, decision: AcpPermissionDecision) {
    if (!conn.child) return;
    try {
      this.write(conn, { jsonrpc: "2.0", id: jsonRpcId, result: { outcome: decision } });
    } catch (e) {
      this.log("acp permission answer failed", String(e));
    }
  }

  // ── retry_state：可见化 + 止损 ───────────────────────────────────────────────
  private handleRetryState(conn: Conn, params: any, u: any) {
    const sid = params.sessionId;
    const info = this.sessionInfo.get(sid);
    if (!info) return;
    const retry: AcpRetryState = {
      attempt: Number(u.attempt || u.retry || 1),
      reason: String(u.reason || u.error || u.kind || "provider_retry").slice(0, 200),
    };
    if (!info.retryStartedAt) info.retryStartedAt = Date.now();
    info.retry = retry;
    this.handlers.onRetryState(sid, retry);
    // 双保险止损，按错误可恢复性分类给预算（2026-10-08 老人反馈"重试 1 次太少"放宽）：
    // - 瞬态类（上游 5xx/超时/限流/网络抖动）：等待往往自愈——10 次 / 10 分钟才止损，
    //   期间 grok 的回合内退避重试成功则任务无感继续。
    // - 硬错误类（鉴权/协议/参数）：重试无意义——维持紧止损 5 次 / 120s 快速失败。
    // 实测教训（紧止损的由来）：中转返回空响应/鉴权失败时 grok 会无限指数退避，
    // prompt 永不 resolve——所以硬错误类绝不能放开。
    const transient = isTransientRetryReason(retry.reason);
    const attemptLimit = transient
      ? (this.opts.retryTransientAttempts ?? DEFAULT_TRANSIENT_ATTEMPTS)
      : (this.opts.retryAbortAttempts ?? DEFAULT_ABORT_ATTEMPTS);
    const elapsedLimit = transient
      ? (this.opts.retryTransientMs ?? DEFAULT_TRANSIENT_MS)
      : (this.opts.retryAbortMs ?? DEFAULT_ABORT_MS);
    const elapsedAbort = Date.now() - info.retryStartedAt > elapsedLimit;
    if (retry.attempt >= attemptLimit || elapsedAbort) {
      if (!info.aborted) {
        info.aborted = true;
        this.log(`acp retry ABORT (attempt=${retry.attempt} reason=${retry.reason} transient=${transient})`, { sid: sid.slice(0, 8) });
        this.cancel(sid).catch(() => {});
      }
    }
  }

  /** 该会话是否已因重试止损（宿主据此生成解释性错误而非静默中断） */
  retryAbortedOf(acpSessionId: string): AcpRetryState | null {
    const info = this.sessionInfo.get(acpSessionId);
    return info?.aborted ? info.retry : null;
  }

  /** 优雅关停：取消权限等待、杀掉全部子进程 */
  dispose() {
    for (const [, w] of this.permWaiters) {
      clearTimeout(w.timer);
      this.answerPermission(w.conn, w.jsonRpcId, { outcome: "rejected" });
    }
    this.permWaiters.clear();
    for (const [, conn] of this.pool) {
      if (conn.child) {
        try {
          // Windows 树杀（/T 连子进程 /F 强制）：child.kill() 对个别场景失效（ECONNRESET
          // 孤儿实测），taskkill 兜底
          const pid = conn.child.pid;
          conn.child.kill();
          if (pid) { try { execFileSync("taskkill", ["/PID", String(pid), "/T", "/F"], { stdio: "ignore" }); } catch { /* 已死则忽略 */ } }
        } catch {}
      }
    }
    this.pool.clear();
    this.sessionInfo.clear();
    for (const d of this.disposers) d();
    this.disposers = [];
  }
}
