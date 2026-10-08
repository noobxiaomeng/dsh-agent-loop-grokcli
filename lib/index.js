// src/index.ts
import { execFileSync as execFileSync2 } from "node:child_process";
import { existsSync as existsSync3, readFileSync as readFileSync3, readdirSync as readdirSync2, rmSync } from "node:fs";
import { homedir as homedir4 } from "node:os";
import { join as join4 } from "node:path";

// src/bridge.ts
import { randomUUID as randomUUID3 } from "node:crypto";
import { appendFileSync as appendFileSync2, existsSync as existsSync2, readFileSync as readFileSync2, readdirSync, writeFileSync as writeFileSync3 } from "node:fs";
import { join as join3 } from "node:path";
import { homedir as homedir3 } from "node:os";

// src/acp-driver.ts
import { spawn, execFileSync } from "node:child_process";
import { createInterface } from "node:readline";
import { randomUUID } from "node:crypto";
var DEFAULT_ABORT_MS = 12e4;
var DEFAULT_ABORT_ATTEMPTS = 5;
var DEFAULT_TRANSIENT_MS = 6e5;
var DEFAULT_TRANSIENT_ATTEMPTS = 10;
var PERM_TIMEOUT_MS = 18e4;
var NOTIFICATION_ONLY_KINDS = /* @__PURE__ */ new Set(["response_completed", "turn_completed", "turn_started", "subagent_spawned", "subagent_progress", "session_summary_generated"]);
function isTransientRetryReason(reason) {
  return /50[234]|upstream|temporar|unavail|rate.?limit|timeout|timed?\s*out|econn|reset|hang\s*up|network|connection|error sending request|send(ing)? request|fetch failed|overload|busy|too\s*many/i.test(reason);
}
var AcpDriver = class {
  pool = /* @__PURE__ */ new Map();
  /** acpSessionId -> { connKey, retryStartedAt, retry, aborted, pendingPrompt, lastRx } */
  sessionInfo = /* @__PURE__ */ new Map();
  permWaiters = /* @__PURE__ */ new Map();
  disposers = [];
  handlers;
  opts;
  constructor(handlers, opts) {
    this.handlers = handlers;
    this.opts = opts;
  }
  log(msg, extra) {
    this.handlers.log?.(msg, extra);
  }
  connKey() {
    return `${this.opts.modelProfile || ""}|${this.opts.reasoningEffort || ""}`;
  }
  write(conn, obj) {
    if (!conn.child || !conn.child.stdin) throw new Error("ACP connection not alive");
    conn.child.stdin.write(JSON.stringify(obj) + "\n");
  }
  request(conn, method, params, timeoutMs = 12e4, keepAlive) {
    const id = conn.nextId++;
    return new Promise((resolve, reject) => {
      let deadline = Date.now() + timeoutMs;
      const timer = setInterval(() => {
        if (Date.now() < deadline) return;
        if (keepAlive?.()) {
          deadline = Date.now() + timeoutMs;
          return;
        }
        clearInterval(timer);
        conn.pending.delete(id);
        reject(new Error(`ACP ${method} timeout`));
      }, 5e3);
      conn.pending.set(id, {
        resolve: (v) => {
          clearInterval(timer);
          resolve(v);
        },
        reject: (e) => {
          clearInterval(timer);
          reject(e);
        }
      });
      try {
        this.write(conn, { jsonrpc: "2.0", id, method, params });
      } catch (e) {
        clearInterval(timer);
        conn.pending.delete(id);
        reject(e);
      }
    });
  }
  /** 取/建连接（per modelProfile×effort），initialize 握手完成后可用 */
  async ensure() {
    const key = this.connKey();
    let conn = this.pool.get(key);
    if (conn && conn.child && conn.ready) return conn;
    conn = { key, child: null, nextId: 1, pending: /* @__PURE__ */ new Map(), ready: null, stderrTail: "", acpSessions: /* @__PURE__ */ new Set() };
    this.pool.set(key, conn);
    conn.ready = new Promise((resolve, reject) => {
      const args = ["--no-plan", "agent"];
      if (this.opts.modelProfile) args.push("-m", this.opts.modelProfile);
      if (this.opts.reasoningEffort) args.push("--reasoning-effort", this.opts.reasoningEffort);
      args.push("stdio");
      const child = spawn(this.opts.grokBin, args, {
        stdio: ["pipe", "pipe", "pipe"],
        cwd: this.opts.cwd,
        env: { ...process.env, HOME: this.opts.realHome, USERPROFILE: this.opts.realHome },
        windowsHide: true
      });
      conn.child = child;
      this.log("acp spawn", { key, pid: child.pid });
      child.stderr?.on("data", (d) => {
        conn.stderrTail = (conn.stderrTail + String(d)).slice(-300);
        this.log("acp stderr", String(d).slice(0, 200));
      });
      child.on("exit", (code) => {
        this.log("acp child exit", key, code, conn.stderrTail.slice(-120));
        conn.child = null;
        conn.ready = null;
        for (const [, p] of conn.pending) p.reject(new Error(`ACP connection lost (exit ${code})`));
        conn.pending.clear();
        for (const sid of conn.acpSessions) this.sessionInfo.delete(sid);
        conn.acpSessions.clear();
        for (const [iid, w] of this.permWaiters) {
          if (w.conn === conn) {
            clearTimeout(w.timer);
            this.permWaiters.delete(iid);
          }
        }
      });
      const rl = createInterface({ input: child.stdout, terminal: false });
      let lineCount = 0;
      rl.on("line", (line) => {
        lineCount++;
        if (lineCount <= 3 || lineCount % 25 === 0) console.log(`[grokcli] acp rx line#${lineCount}: ${line.slice(0, 90)}`);
        if (!line.trim()) return;
        let msg;
        try {
          msg = JSON.parse(line);
        } catch {
          return;
        }
        try {
          const rxSid = msg?.params?.sessionId;
          if (typeof rxSid === "string") {
            const si = this.sessionInfo.get(rxSid);
            if (si) si.lastRx = Date.now();
          }
          if (msg.id !== void 0 && msg.result !== void 0 && conn.pending.has(msg.id)) {
            conn.pending.get(msg.id).resolve(msg.result);
            conn.pending.delete(msg.id);
          } else if (msg.id !== void 0 && msg.error !== void 0 && conn.pending.has(msg.id)) {
            conn.pending.get(msg.id).reject(new Error(JSON.stringify(msg.error)));
            conn.pending.delete(msg.id);
          } else if (msg.id !== void 0 && msg.method === "session/request_permission") {
            this.handlePermission(conn, msg.id, msg.params || {});
          } else if (msg.id !== void 0 && typeof msg.method === "string") {
            this.handleServerRequest(conn, msg.id, msg.method, msg.params || {});
          } else if (msg.method === "session/update") {
            this.handlers.onUpdate(msg.params?.sessionId, (msg.params || {}).update || {});
          } else if (msg.method === "_x.ai/session_notification") {
            const u = msg.params && msg.params.update || {};
            if (u.sessionUpdate === "retry_state") this.handleRetryState(conn, msg.params || {}, u);
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
        clientCapabilities: { fs: { readTextFile: false, writeTextFile: false } }
      }, 15e3).then(() => resolve(), reject);
    });
    await conn.ready;
    return conn;
  }
  /** 恢复已存在的 grok 会话（跨进程重启；~/.grok/sessions 落盘）。成功返回会话 id，失败返回 null。 */
  async loadSession(acpSessionId) {
    const conn = await this.ensure();
    try {
      await this.request(conn, "session/load", {
        sessionId: acpSessionId,
        cwd: this.opts.cwd,
        mcpServers: []
        // mcpServers 必填（实测缺省报 Invalid params）
      }, 3e4);
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
  async newSession() {
    const conn = await this.ensure();
    const sess = await this.request(conn, "session/new", {
      cwd: this.opts.cwd,
      mcpServers: []
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
  async prompt(acpSessionId, text, timeoutMs) {
    const conn = await this.ensure();
    const idleMs = timeoutMs ?? this.opts.promptIdleMs ?? 6e5;
    const hardMs = this.opts.promptHardMs ?? 432e5;
    const info = this.sessionInfo.get(acpSessionId);
    if (info) {
      info.retryStartedAt = 0;
      info.retry = null;
      info.aborted = false;
      info.pendingPrompt = true;
      info.lastRx = 0;
    }
    const startedAt = Date.now();
    let seenRx = 0;
    const keepAlive = () => {
      if (Date.now() - startedAt >= hardMs) return false;
      const cur = this.sessionInfo.get(acpSessionId)?.lastRx ?? 0;
      if (cur > seenRx) {
        seenRx = cur;
        return true;
      }
      return false;
    };
    try {
      const result = await this.request(conn, "session/prompt", {
        sessionId: acpSessionId,
        prompt: [{ type: "text", text }]
      }, idleMs, keepAlive);
      console.log(`[grokcli] prompt resolved: stopReason=${result?.stopReason}`);
      return result?.stopReason || "end_turn";
    } finally {
      if (info) info.pendingPrompt = false;
    }
  }
  async cancel(acpSessionId) {
    const conn = await this.ensure();
    try {
      this.write(conn, { jsonrpc: "2.0", method: "session/cancel", params: { sessionId: acpSessionId } });
    } catch (e) {
      this.log("acp cancel notify failed", String(e));
    }
    const info = this.sessionInfo.get(acpSessionId);
    setTimeout(() => {
      const cur = this.sessionInfo.get(acpSessionId);
      if (cur?.pendingPrompt && conn.child) {
        this.log("acp cancel escalation: kill connection", { sid: acpSessionId.slice(0, 8) });
        try {
          const pid = conn.child.pid;
          conn.child.kill();
          if (pid) {
            try {
              execFileSync("taskkill", ["/PID", String(pid), "/T", "/F"], { stdio: "ignore" });
            } catch {
            }
          }
        } catch {
        }
      }
      void info;
    }, 8e3).unref?.();
  }
  connOfSession(acpSessionId) {
    const info = this.sessionInfo.get(acpSessionId);
    if (!info) return null;
    const conn = this.pool.get(info.connKey);
    return conn && conn.child ? conn : null;
  }
  // ── grok 私有扩展请求（_x.ai/*）：计划审批与用户提问的真协议应答 ─────────────
  handleServerRequest(conn, jsonRpcId, method, params) {
    const INTERACTIVE_TIMEOUT_MS = 15 * 6e4;
    const answer = (result) => {
      try {
        this.write(conn, { jsonrpc: "2.0", id: jsonRpcId, result });
      } catch (e) {
        this.log("acp server-request answer failed", String(e));
      }
    };
    if (method === "_x.ai/exit_plan_mode") {
      console.log(`[grokcli] _x.ai/exit_plan_mode request (plan ${String(params?.planContent ?? "").length}B)`);
      const fallback = { outcome: "abandoned" };
      if (!this.handlers.onExitPlanMode) {
        answer(fallback);
        return;
      }
      const timer = setTimeout(() => {
        console.log("[grokcli] _x.ai/exit_plan_mode 15min \u8D85\u65F6 -> abandoned");
        answer(fallback);
      }, INTERACTIVE_TIMEOUT_MS);
      timer.unref?.();
      void Promise.resolve(this.handlers.onExitPlanMode(params)).then(
        (r) => {
          clearTimeout(timer);
          answer(r ?? fallback);
        },
        (e) => {
          clearTimeout(timer);
          this.log("onExitPlanMode handler error", String(e));
          answer(fallback);
        }
      );
      return;
    }
    if (method === "_x.ai/ask_user_question") {
      const qCount = Array.isArray(params?.questions) ? params.questions.length : 0;
      console.log(`[grokcli] _x.ai/ask_user_question request (${qCount} \u95EE)`);
      const fallback = { outcome: "cancelled" };
      if (!this.handlers.onAskUserQuestion) {
        answer(fallback);
        return;
      }
      const timer = setTimeout(() => {
        console.log("[grokcli] _x.ai/ask_user_question 15min \u8D85\u65F6 -> cancelled");
        answer(fallback);
      }, INTERACTIVE_TIMEOUT_MS);
      timer.unref?.();
      void Promise.resolve(this.handlers.onAskUserQuestion(params)).then(
        (r) => {
          clearTimeout(timer);
          answer(r ?? fallback);
        },
        (e) => {
          clearTimeout(timer);
          this.log("onAskUserQuestion handler error", String(e));
          answer(fallback);
        }
      );
      return;
    }
    console.log(`[grokcli] unhandled server request id=${jsonRpcId} method=${method}`);
    try {
      this.write(conn, { jsonrpc: "2.0", id: jsonRpcId, error: { code: -32601, message: `method not found: ${method}` } });
    } catch {
    }
  }
  // ── 权限桥 ──────────────────────────────────────────────────────────────────
  handlePermission(conn, jsonRpcId, params) {
    const req = {
      sessionId: params.sessionId,
      toolCallId: params.toolCallId || "unknown",
      title: params.title,
      rawInput: params.rawInput,
      options: params.options || []
    };
    const interactionId = "perm-" + randomUUID();
    const timer = setTimeout(() => {
      const w = this.permWaiters.get(interactionId);
      if (w) {
        this.permWaiters.delete(interactionId);
        this.answerPermission(w.conn, w.jsonRpcId, { outcome: "rejected" });
      }
    }, PERM_TIMEOUT_MS);
    this.permWaiters.set(interactionId, { conn, jsonRpcId, timer });
    this.handlers.onPermission(req).then(
      (decision) => {
        const w = this.permWaiters.get(interactionId);
        if (!w) return;
        clearTimeout(w.timer);
        this.permWaiters.delete(interactionId);
        this.answerPermission(w.conn, w.jsonRpcId, decision);
      },
      (err) => {
        const w = this.permWaiters.get(interactionId);
        if (!w) return;
        clearTimeout(w.timer);
        this.permWaiters.delete(interactionId);
        this.log("acp permission handler error", String(err));
        this.answerPermission(w.conn, w.jsonRpcId, { outcome: "rejected" });
      }
    );
  }
  answerPermission(conn, jsonRpcId, decision) {
    if (!conn.child) return;
    try {
      this.write(conn, { jsonrpc: "2.0", id: jsonRpcId, result: { outcome: decision } });
    } catch (e) {
      this.log("acp permission answer failed", String(e));
    }
  }
  // ── retry_state：可见化 + 止损 ───────────────────────────────────────────────
  handleRetryState(conn, params, u) {
    const sid = params.sessionId;
    const info = this.sessionInfo.get(sid);
    if (!info) return;
    const retry = {
      attempt: Number(u.attempt || u.retry || 1),
      reason: String(u.reason || u.error || u.kind || "provider_retry").slice(0, 200)
    };
    if (!info.retryStartedAt) info.retryStartedAt = Date.now();
    info.retry = retry;
    this.handlers.onRetryState(sid, retry);
    const transient = isTransientRetryReason(retry.reason);
    const attemptLimit = transient ? this.opts.retryTransientAttempts ?? DEFAULT_TRANSIENT_ATTEMPTS : this.opts.retryAbortAttempts ?? DEFAULT_ABORT_ATTEMPTS;
    const elapsedLimit = transient ? this.opts.retryTransientMs ?? DEFAULT_TRANSIENT_MS : this.opts.retryAbortMs ?? DEFAULT_ABORT_MS;
    const elapsedAbort = Date.now() - info.retryStartedAt > elapsedLimit;
    if (retry.attempt >= attemptLimit || elapsedAbort) {
      if (!info.aborted) {
        info.aborted = true;
        this.log(`acp retry ABORT (attempt=${retry.attempt} reason=${retry.reason} transient=${transient})`, { sid: sid.slice(0, 8) });
        this.cancel(sid).catch(() => {
        });
      }
    }
  }
  /** 该会话是否已因重试止损（宿主据此生成解释性错误而非静默中断） */
  retryAbortedOf(acpSessionId) {
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
          const pid = conn.child.pid;
          conn.child.kill();
          if (pid) {
            try {
              execFileSync("taskkill", ["/PID", String(pid), "/T", "/F"], { stdio: "ignore" });
            } catch {
            }
          }
        } catch {
        }
      }
    }
    this.pool.clear();
    this.sessionInfo.clear();
    for (const d of this.disposers) d();
    this.disposers = [];
  }
};

// src/profile-router.ts
import { existsSync, readFileSync, writeFileSync } from "node:fs";
function configTomlPath(realHome) {
  return realHome + "/.grok/config.toml";
}
function syncGrokProfiles(realHome, entries, modelOverride, baseUrlOverride) {
  if (!entries.length) return false;
  const path = configTomlPath(realHome);
  try {
    const raw = existsSync(path) ? readFileSync(path, "utf-8") : "";
    const lines = raw.split("\n");
    const kept = [];
    const oldManaged = [];
    let inManaged = false;
    for (const line of lines) {
      const h = line.match(/^\s*\[([^\]]+)\]\s*$/);
      if (h) inManaged = /^model\.grokdesk-/.test(h[1]);
      if (!inManaged) kept.push(line);
      else oldManaged.push(line.trim());
    }
    let out = kept.join("\n").replace(/\n{3,}$/, "\n");
    const newManaged = [];
    for (const e of entries) {
      const section = `
[model.grokdesk-${e.id}]
`;
      const profModel = modelOverride || (e.models.length ? e.models[0] : null);
      const body = `api_backend = ${JSON.stringify(e.backend)}
api_key = ${JSON.stringify(e.apiKey)}
base_url = ${JSON.stringify(baseUrlOverride || e.baseUrl)}
` + (profModel ? `model = ${JSON.stringify(profModel)}
` : "") + `name = ${JSON.stringify(String(e.name))}
`;
      out += section + body;
      newManaged.push(section.trim(), ...body.split("\n").filter(Boolean));
    }
    const changed = oldManaged.join("\n") !== newManaged.join("\n");
    if (changed) writeFileSync(path, out, "utf-8");
    return changed;
  } catch (e) {
    console.warn("[grokcli] config.toml sync failed:", e);
    return false;
  }
}
function decideRoute(entries, wantModel) {
  const want = wantModel || null;
  const usable = entries.filter((e) => e.baseUrl && e.apiKey);
  const owner = want ? usable.find((e) => e.models.includes(want)) : void 0;
  const profile = owner ?? usable[0] ?? null;
  if (profile) {
    return { spawnKey: `grokdesk-${profile.id}`, model: want };
  }
  return { spawnKey: want || "", model: want };
}

// src/model-pinning-proxy.ts
import { createServer, request as httpRequest } from "node:http";
import { appendFileSync, writeFileSync as writeFileSync2 } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { request as httpsRequest } from "node:https";
var ModelPinningProxy = class _ModelPinningProxy {
  server;
  port;
  /** 给 grok 当 base_url 用（与真实 base 同构，含 /v1） */
  baseUrl;
  constructor(port, server) {
    this.port = port;
    this.server = server;
    this.baseUrl = `http://127.0.0.1:${port}/v1`;
  }
  setTarget(target) {
    pinState.target = target;
  }
  /** 固定端口候选（2026-10-08 端口漂移根治）：随机端口在桌面重启后漂移，grok 进程
   *  spawn 时读的 config.toml 里的旧端口在空窗期是死的——孤儿 grok / 时序错配都会打
   *  死端口报 error sending request。固定端口让新桌面重新监听同一端口，旧配置继续
   *  有效。候选依次尝试（被占/防火墙拦则顺延），全占回退随机端口。 */
  static start() {
    const envPort = Number(process.env.GROKDESK_PIN_PORT ?? 0);
    const candidates = [
      ...Number.isInteger(envPort) && envPort > 0 ? [envPort] : [],
      53909,
      53910,
      53911
    ];
    return new Promise((resolve, reject) => {
      const tryBind = (port, fallback) => {
        const server = createServer((req, res) => handle(req, res, () => pinState.target));
        const fail = (err) => {
          server.removeAllListeners();
          const next = fallback.shift();
          if (next === void 0) {
            if (port === 0) {
              reject(new Error(`pinning proxy failed to bind: ${String(err)}`));
              return;
            }
            tryBind(0, []);
            return;
          }
          tryBind(next, fallback);
        };
        server.on("error", fail);
        server.listen(port, "127.0.0.1", () => {
          const addr = server.address();
          if (addr == null || typeof addr === "string") {
            fail(new Error("pinning proxy failed to bind"));
            return;
          }
          console.log(`[grokcli] pin proxy listening on 127.0.0.1:${addr.port}${port === 0 ? " (random fallback)" : ""}`);
          resolve(new _ModelPinningProxy(addr.port, server));
        });
      };
      const first = candidates.shift();
      tryBind(first ?? 0, candidates);
    });
  }
  close() {
    try {
      this.server.close();
    } catch {
    }
  }
};
var pinState = { target: null };
var usageLog = [];
var usageListeners = /* @__PURE__ */ new Set();
function onPinUsage(cb) {
  usageListeners.add(cb);
  return () => {
    usageListeners.delete(cb);
  };
}
function recordUsage(u) {
  if (!u || typeof u !== "object") return;
  const o = u;
  if (o.prompt_tokens === void 0 && o.completion_tokens === void 0 && (o.input_tokens !== void 0 || o.output_tokens !== void 0)) {
    o.prompt_tokens = o.input_tokens;
    o.completion_tokens = o.output_tokens;
    o.total_tokens = o.total_tokens ?? (o.input_tokens ?? 0) + (o.output_tokens ?? 0);
    o.prompt_tokens_details = { cached_tokens: o.input_tokens_details?.cached_tokens ?? 0 };
  }
  if (o.prompt_tokens === void 0 && o.completion_tokens === void 0) return;
  usageLog.push({
    ts: Date.now(),
    usage: {
      prompt_tokens: Number(o.prompt_tokens ?? 0) || 0,
      completion_tokens: Number(o.completion_tokens ?? 0) || 0,
      total_tokens: Number(o.total_tokens ?? 0) || 0,
      prompt_tokens_details: { cached_tokens: Number(o.prompt_tokens_details?.cached_tokens ?? 0) || 0 }
    }
  });
  if (usageLog.length > 200) usageLog.splice(0, usageLog.length - 200);
  console.log(`[grokcli] pin-usage captured: +${o.prompt_tokens}in/+${o.completion_tokens}out`);
  const cached = Number(o.prompt_tokens_details?.cached_tokens ?? 0) || 0;
  const snapshot = {
    inputTokens: Math.max(0, (Number(o.prompt_tokens ?? 0) || 0) - cached),
    outputTokens: Number(o.completion_tokens ?? 0) || 0,
    cacheReadTokens: cached,
    cacheWriteTokens: 0
  };
  for (const cb of [...usageListeners]) {
    try {
      cb(snapshot);
    } catch {
    }
  }
}
function usageSince(sinceMs) {
  const rows = usageLog.filter((r) => r.ts >= sinceMs);
  if (rows.length === 0) return null;
  let prompt = 0, completion = 0, total = 0, cached = 0;
  for (const r of rows) {
    prompt += r.usage.prompt_tokens ?? 0;
    completion += r.usage.completion_tokens ?? 0;
    total += r.usage.total_tokens ?? 0;
    cached += r.usage.prompt_tokens_details?.cached_tokens ?? 0;
  }
  return {
    inputTokens: Math.max(0, prompt - cached),
    outputTokens: completion,
    cacheReadTokens: cached,
    cacheWriteTokens: 0,
    totalTokens: total || prompt + completion
  };
}
function completeRelayEventShape(v) {
  let changed = false;
  const walk = (o) => {
    for (const [k, val] of Object.entries(o)) {
      if (k === "error" && val !== null && typeof val === "object" && !Array.isArray(val)) {
        const e = val;
        if (e.code === void 0) {
          e.code = "relay_error";
          changed = true;
        }
        if (e.message === void 0) {
          e.message = "relay error without message";
          changed = true;
        }
        walk(e);
      } else if (k === "response" && val !== null && typeof val === "object" && !Array.isArray(val)) {
        const r = val;
        if (!Array.isArray(r.output)) {
          r.output = [];
          changed = true;
        }
        walk(r);
      } else if (Array.isArray(val)) {
        for (const item of val) {
          if (item !== null && typeof item === "object" && !Array.isArray(item)) walk(item);
        }
      } else if (val !== null && typeof val === "object") {
        walk(val);
      }
    }
  };
  if (v !== null && typeof v === "object" && !Array.isArray(v)) walk(v);
  return changed;
}
function handle(req, res, getTarget) {
  const chunks = [];
  req.on("data", (c) => chunks.push(c));
  req.on("end", () => {
    const target = getTarget();
    if (target == null) {
      res.writeHead(503, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "pinning proxy: no target configured" }));
      return;
    }
    let body = Buffer.concat(chunks);
    console.log(`[grokcli] pin-req ${req.method} ${req.url} ${body.length}B model=${(() => {
      try {
        return JSON.parse(body.toString("utf-8")).model || "-";
      } catch {
        return "-";
      }
    })()}`);
    if (req.method === "POST" && body.length > 0) {
      try {
        const parsed = JSON.parse(body.toString("utf-8"));
        if (parsed && typeof parsed === "object") {
          const notes = [];
          if (typeof parsed.model === "string" && parsed.model !== target.model) {
            notes.push(`model ${parsed.model} -> ${target.model}`);
            parsed.model = target.model;
          }
          const isResponsesApi = typeof req.url === "string" && req.url.includes("/responses");
          if (!isResponsesApi && target.effort && parsed.reasoning_effort === void 0 && parsed.reasoning === void 0) {
            parsed.reasoning_effort = target.effort;
            notes.push(`reasoning_effort=${target.effort} injected`);
          }
          if (isResponsesApi && target.effort) {
            const r = parsed.reasoning;
            if ((r === void 0 || r.effort === void 0) && parsed.reasoning_effort === void 0) {
              parsed.reasoning = { ...r ?? {}, effort: target.effort };
              notes.push(`reasoning.effort=${target.effort} injected`);
            }
          }
          if (!isResponsesApi && parsed.stream === true && parsed.stream_options?.include_usage !== true) {
            parsed.stream_options = { ...parsed.stream_options || {}, include_usage: true };
            notes.push("stream_options.include_usage injected");
          }
          if (notes.length > 0) {
            body = Buffer.from(JSON.stringify(parsed), "utf-8");
            console.log(`[grokcli] pin (${req.url}): ${notes.join(", ")}`);
          }
        }
      } catch {
      }
    }
    const m = /^(https?):\/\/([^/]+)(\/.*)?$/.exec(target.upstreamBase.replace(/\/+$/, ""));
    if (!m) {
      res.writeHead(502, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "pinning proxy: bad upstream base" }));
      return;
    }
    const [, scheme, hostport, basePath = ""] = m;
    const endpointTyped = /\/(responses|chat\/completions)$/.test(basePath);
    const path = endpointTyped ? basePath : req.url && req.url.startsWith("/v1") ? basePath + req.url.slice(3) : basePath + (req.url || "");
    const send = scheme === "https" ? httpsRequest : httpRequest;
    const headers = { ...req.headers, host: hostport, "content-length": String(body.length) };
    const up = send({ hostname: hostport.split(":")[0], port: Number(hostport.split(":")[1] ?? (scheme === "https" ? 443 : 80)), path, method: req.method, headers }, (upRes) => {
      const outHeaders = {};
      for (const [k, v] of Object.entries(upRes.headers)) {
        if (v == null || k === "transfer-encoding" || k === "connection" || k === "keep-alive" || k === "content-length") continue;
        outHeaders[k] = Array.isArray(v) ? v.join(", ") : v;
      }
      res.writeHead(upRes.statusCode ?? 502, outHeaders);
      const isSSE = String(upRes.headers["content-type"] ?? "").includes("text/event-stream");
      const passthroughResponses = typeof req.url === "string" && req.url.includes("/responses");
      if (passthroughResponses) {
        let buf = "";
        let dumped = false;
        let thinkLive = false;
        const dumpPath = join(homedir(), ".grokdesk", "relay-last-responses.log");
        const dump = (s) => {
          try {
            if (!dumped) {
              writeFileSync2(dumpPath, `=== ${(/* @__PURE__ */ new Date()).toISOString()} ${req.url} ===
`);
              dumped = true;
            }
            appendFileSync(dumpPath, s);
          } catch {
          }
        };
        const sanitizeBlock = (text) => {
          if (!text.includes("error") && !text.includes('"response"')) return text;
          try {
            const j = JSON.parse(text);
            if (j && typeof j === "object") {
              if (completeRelayEventShape(j)) {
                console.log(`[grokcli] pin: \u8865\u5168\u975E\u6D41\u5F0F\u5D4C\u5957 error \u5B57\u6BB5 (${text.length}B)`);
                return JSON.stringify(j);
              }
              const o = j;
              if (o.type === "error" && (o.code === void 0 || o.message === void 0)) {
                o.code = o.code ?? "relay_error";
                o.message = o.message ?? "relay error without code";
                console.log(`[grokcli] pin: \u8865\u5168\u975E\u6D41\u5F0F error \u4E8B\u4EF6\u5B57\u6BB5 (${text.length}B)`);
                return JSON.stringify(j);
              }
            }
          } catch {
          }
          return text;
        };
        upRes.on("data", (d) => {
          const chunk = d.toString("utf8");
          dump(chunk);
          buf += chunk;
          let idx;
          const out = [];
          while ((idx = buf.indexOf("\n")) >= 0) {
            const rawLine = buf.slice(0, idx);
            buf = buf.slice(idx + 1);
            const line = rawLine.trim();
            if (!line.startsWith("data:")) {
              out.push(rawLine + "\n");
              continue;
            }
            const payload = line.slice(5).trim();
            if (!payload || payload === "[DONE]") {
              out.push(rawLine + "\n");
              continue;
            }
            try {
              const ev = JSON.parse(payload);
              if (typeof ev.type !== "string") {
                console.log(`[grokcli] pin: \u4E22\u5F03\u65E0 type \u884C responses (${payload.length}B) ${payload.slice(0, 60)}`);
                continue;
              }
              const extra = [];
              let replaced = false;
              if (ev.type === "response.reasoning_summary_text.delta" && typeof ev.delta === "string" && ev.delta.length > 0) {
                const openTag = thinkLive ? "" : "<think>";
                thinkLive = true;
                const fake = { content_index: 0, type: "response.output_text.delta", delta: openTag + ev.delta, item_id: ev.item_id ?? "rs_live", output_index: 0, sequence_number: typeof ev.sequence_number === "number" ? ev.sequence_number : 0 };
                extra.push("data: " + JSON.stringify(fake) + "\n\n");
              } else if (ev.type === "response.output_text.delta" && typeof ev.delta === "string" && thinkLive && ev.delta.length > 0) {
                thinkLive = false;
                const fake = { content_index: 0, type: "response.output_text.delta", delta: "</think>" + ev.delta, item_id: ev.item_id ?? "msg_live", output_index: 0, sequence_number: typeof ev.sequence_number === "number" ? ev.sequence_number : 0 };
                extra.push("data: " + JSON.stringify(fake) + "\n\n");
                replaced = true;
              }
              let forward = rawLine + "\n";
              if (ev.type === "error" && (ev.code === void 0 || ev.message === void 0)) {
                ev.code = ev.code ?? "relay_error";
                ev.message = ev.message ?? "relay returned a malformed error event";
                forward = "data: " + JSON.stringify(ev) + "\n";
                console.log(`[grokcli] pin: \u8865\u5168 error \u4E8B\u4EF6\u5B57\u6BB5 (code=${String(ev.code)})`);
              }
              if (completeRelayEventShape(ev)) {
                forward = "data: " + JSON.stringify(ev) + "\n";
                console.log(`[grokcli] pin: \u8865\u5168\u5D4C\u5957 error \u5B57\u6BB5 (responses SSE ${payload.length}B)`);
              }
              if (ev.response?.usage) recordUsage(ev.response.usage);
              if (extra.length > 0) out.push(...extra);
              if (!replaced) out.push(forward);
            } catch {
              console.log(`[grokcli] pin: \u4E22\u5F03\u574F data \u884C responses (${payload.length}B)`);
            }
          }
          if (out.length > 0) res.write(out.join(""));
        });
        upRes.on("end", () => {
          if (buf.trim()) {
            dump("\n=== tail ===\n" + buf);
            res.write(sanitizeBlock(buf));
          }
          res.end();
        });
        upRes.on("error", () => res.end());
        return;
      }
      if (isSSE) {
        let sseBuf = "";
        let inThink = false;
        upRes.on("data", (d) => {
          sseBuf += d.toString("utf8");
          let idx;
          const out = [];
          while ((idx = sseBuf.indexOf("\n")) >= 0) {
            const rawLine = sseBuf.slice(0, idx);
            sseBuf = sseBuf.slice(idx + 1);
            const line = rawLine.trim();
            if (!line.startsWith("data:")) {
              out.push(rawLine + "\n");
              continue;
            }
            const payload = line.slice(5).trim();
            if (!payload || payload === "[DONE]") {
              out.push(rawLine + "\n");
              continue;
            }
            let rewritten = rawLine + "\n";
            try {
              const ev = JSON.parse(payload);
              if (!ev.choices && !ev.usage && !ev.id) {
                console.log(`[grokcli] pin: \u4E22\u5F03\u975E chunk \u884C (${payload.length}B) ${payload.slice(0, 60)}`);
                continue;
              }
              if (ev.usage) recordUsage(ev.usage);
              const maybeRc = ev.choices?.[0]?.delta?.reasoning_content ?? ev.choices?.[0]?.delta?.reasoning;
              if (typeof maybeRc === "string") {
                if (!ev.id) ev.id = "chatcmpl-pin";
                if (!ev.object) ev.object = "chat.completion.chunk";
                if (typeof ev.created !== "number") ev.created = Math.floor(Date.now() / 1e3);
                if (!ev.model) ev.model = target.model;
              }
              const delta = ev.choices?.[0]?.delta;
              if (delta && typeof delta === "object" && typeof (delta.reasoning_content ?? delta.reasoning) === "string") {
                rewritten = "data: " + JSON.stringify(ev) + "\n";
              }
            } catch {
              console.log(`[grokcli] pin: \u4E22\u5F03\u574F data \u884C (${payload.length}B)`);
              continue;
            }
            out.push(rewritten);
          }
          if (out.length > 0) res.write(out.join(""));
        });
        upRes.on("end", () => {
          if (inThink) res.write("data: " + JSON.stringify({ choices: [{ delta: { content: "</think>" } }] }) + "\n\n");
          res.end();
        });
        upRes.on("error", () => res.end());
      } else {
        const acc = [];
        upRes.on("data", (d) => {
          acc.push(d);
        });
        upRes.on("end", () => {
          let bodyBuf = Buffer.concat(acc);
          try {
            const parsed = JSON.parse(bodyBuf.toString("utf8"));
            if (parsed.usage) recordUsage(parsed.usage);
            const msg = parsed.choices?.[0]?.message;
            const rc = msg?.reasoning_content ?? msg?.reasoning;
            if (msg && typeof rc === "string" && rc.length > 0) {
              const prev = typeof msg.content === "string" ? msg.content : "";
              msg.content = `<think>${rc}</think>${prev}`;
              delete msg.reasoning_content;
              if (msg.reasoning !== void 0) delete msg.reasoning;
              bodyBuf = Buffer.from(JSON.stringify(parsed), "utf8");
              console.log(`[grokcli] pin (${req.url}): reasoning_content -> <think> (${rc.length}B)`);
            }
          } catch {
          }
          res.end(bodyBuf);
        });
        upRes.on("error", () => res.end());
      }
    });
    up.on("error", (e) => {
      console.log(`[grokcli] pin proxy upstream error: ${String(e).slice(0, 120)}`);
      if (!res.headersSent) res.writeHead(502, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: String(e) }));
    });
    up.end(body);
  });
}

// src/settings-bridge.ts
var BACKEND_MAP = {
  "openai-completions": "chat",
  "openai-responses": "responses",
  "anthropic-messages": "anthropic"
};
function deriveKeyRef(provider) {
  return `${provider.toUpperCase().replace(/[^A-Z0-9]+/g, "_")}_API_KEY`;
}
var LEVEL_VOCAB = /* @__PURE__ */ new Set(["off", "minimal", "low", "medium", "high", "xhigh", "max"]);
async function decorateEfforts(ctx, entries) {
  const get = ctx.get?.bind(ctx);
  let editor;
  try {
    editor = get?.("configEditor");
  } catch {
    return;
  }
  if (!editor?.entries) return;
  const entry = editor.entries().find((r) => r.options?.id === "llm-pi-ai");
  if (!entry) return;
  for (const e of entries) {
    if (!e.baseUrl || !e.apiKey) continue;
    let effortsByModel = null;
    try {
      const res = await fetch(`${e.baseUrl.replace(/\/+$/, "")}/models`, {
        headers: { authorization: `Bearer ${e.apiKey}` },
        signal: AbortSignal.timeout(15e3)
      });
      if (res.ok) {
        const data = await res.json();
        effortsByModel = /* @__PURE__ */ new Map();
        for (const m of data.data || []) {
          if (!m?.id || !m.supportsReasoningEffort) continue;
          const levels = (m.reasoningEfforts || []).map((l) => l.value || l.id).filter((v) => !!v && LEVEL_VOCAB.has(v));
          if (levels.length) effortsByModel.set(m.id, levels);
        }
      }
    } catch {
    }
    if (!effortsByModel || effortsByModel.size === 0) continue;
    let changed = false;
    try {
      await editor.edit(entry, (current) => {
        const providers = current.providers ?? {};
        const provider = providers[e.id];
        if (!provider?.models) return current;
        for (const model of provider.models) {
          if (!model?.id) continue;
          const levels = effortsByModel.get(model.id);
          if (!levels) continue;
          const desired = {};
          for (const lv of levels) desired[lv] = lv;
          const currentEfforts = model.reasoningEfforts;
          if (JSON.stringify(currentEfforts) === JSON.stringify(desired)) continue;
          model.reasoningEfforts = desired;
          changed = true;
        }
        return current;
      });
      if (changed) console.log(`[grokcli] efforts decorated on provider "${e.id}" (${[...effortsByModel.keys()].join(", ")})`);
    } catch (err) {
      console.log(`[grokcli] efforts decorate failed for "${e.id}": ${String(err).slice(0, 150)}`);
    }
  }
}
async function readPiAiProfiles(ctx) {
  const get = ctx.get?.bind(ctx);
  const editor = get?.("configEditor");
  if (!editor?.configuration) return [];
  let providers;
  try {
    const row = editor.configuration().find((r) => r.entry?.options?.id === "llm-pi-ai");
    providers = row?.override?.providers ?? row?.entry?.options?.config?.providers;
  } catch {
    return [];
  }
  if (!providers || typeof providers !== "object") return [];
  const credentials = get?.("credentials");
  const out = [];
  for (const [key, raw] of Object.entries(providers)) {
    if (!raw || typeof raw !== "object") continue;
    const baseUrl = raw.baseURL || raw.base_url;
    if (!baseUrl) continue;
    let apiKey = "";
    const ref = raw.apiKeyEnv || deriveKeyRef(key);
    if (credentials) {
      try {
        const hit = await credentials.resolve(ref);
        apiKey = hit?.value ?? "";
        if (!apiKey) console.log(`[grokcli] credential ${ref}: no value (\u5728\u8BBE\u7F6E\u2192\u6A21\u578B\u2192\u7F16\u8F91\u91CC\u586B\u4E00\u6B21 API \u5BC6\u94A5\u5373\u53EF)`);
      } catch (e) {
        console.log(`[grokcli] credential ${ref} resolve threw: ${String(e).slice(0, 120)}`);
        apiKey = "";
      }
    } else {
      console.log(`[grokcli] credential ${ref}: credentials service unavailable`);
    }
    out.push({
      id: key.replace(/[^a-zA-Z0-9_-]/g, "-"),
      name: raw.displayName || key,
      baseUrl,
      apiKey,
      backend: BACKEND_MAP[raw.api || ""] || "chat",
      models: (raw.models || []).map((m) => typeof m === "string" ? m : m?.id).filter((m) => Boolean(m))
    });
  }
  return out;
}

// src/subagent-mirror.ts
import { randomUUID as randomUUID2 } from "node:crypto";
import { mkdirSync } from "node:fs";
import { homedir as homedir2 } from "node:os";
import { join as join2 } from "node:path";
var SUBAGENTS_DIR = join2(homedir2(), ".grokdesk", "subagents");
function grokUsageToTokenUsage(u) {
  if (!u || typeof u !== "object") return void 0;
  const o = u;
  const num = (v) => typeof v === "number" && Number.isFinite(v) ? v : 0;
  const input = num(o.input_tokens ?? o.inputTokens);
  const output = num(o.output_tokens ?? o.outputTokens);
  const cacheRead = num(o.cache_read_input_tokens ?? o.cachedReadTokens);
  const cacheWrite = num(o.cache_creation_input_tokens ?? o.cacheWriteTokens ?? o.cacheCreationTokens);
  if (input === 0 && output === 0 && cacheRead === 0 && cacheWrite === 0) return void 0;
  const total = num(o.total_tokens ?? o.totalTokens) || input + output;
  return { inputTokens: Math.max(0, input - cacheRead), outputTokens: output, cacheReadTokens: cacheRead, cacheWriteTokens: cacheWrite, totalTokens: total };
}
var SubagentMirror = class _SubagentMirror {
  constructor(spec, ctx, parentDshSessionId, model) {
    this.spec = spec;
    this.ctx = ctx;
    this.parentDshSessionId = parentDshSessionId;
    this.model = model;
    this.subagentId = spec.subagentId;
  }
  spec;
  ctx;
  parentDshSessionId;
  model;
  /** 运行中的镜像实例（按 dsh 会话 id 索引，open 登记/finalize 撤销）：resume 分流用
   *  live() 识别「子代理仍在写」的会话（只读浏览），并拿实例走单本账 append 通道
   *  （2026-10-06 view-only 修复：第二本 seq/turn 账必然相撞，提示回合必须经 mirror 落）。 */
  static liveMirrors = /* @__PURE__ */ new Map();
  static live(dshSessionId) {
    return _SubagentMirror.liveMirrors.get(dshSessionId);
  }
  subagentId;
  /** dsh 侧子会话 id / 创建时间（父会话 subagent/catalog 事件要引用） */
  dshSessionId = null;
  dshSessionCreatedAt = 0;
  session = null;
  handle = null;
  detach = null;
  turn = 0;
  turnOpen = false;
  textBuf = "";
  thoughtBuf = "";
  pendingTools = /* @__PURE__ */ new Map();
  /** view-only 浏览期间用户发消息的提示回合队列：子代理回合进行中（turnOpen）不能落
   *  turn（嵌套 turn = 冷读取判损坏），排队等回合收尾后的安全窗口冲刷。 */
  pendingNotices = [];
  finalized = false;
  static async open(spec, ctx, parentDshSessionId, cwd, model) {
    const m = new _SubagentMirror(spec, ctx, parentDshSessionId, model);
    let mirrorCwd = cwd;
    try {
      mkdirSync(SUBAGENTS_DIR, { recursive: true });
      mirrorCwd = SUBAGENTS_DIR;
    } catch {
    }
    const id = `session-${randomUUID2()}`;
    const session = ctx.sessions.prepare(id, {
      meta: { parentSession: parentDshSessionId, cwd: mirrorCwd, delegationDepth: 1 }
    });
    m.session = session;
    m.dshSessionId = id;
    m.dshSessionCreatedAt = session.header?.createdAt ?? Date.now();
    m.detach = ctx.sessions.enter(session);
    const title = spec.description || spec.subagentType || `\u5B50\u4EE3\u7406 ${spec.subagentId.slice(0, 8)}`;
    try {
      session.append("session/title", { title, messageSeqs: [], source: { kind: "user" } });
    } catch (e) {
      console.log(`[grokcli] mirror title append failed: ${String(e).slice(0, 120)}`);
    }
    try {
      const persistence = ctx.get("sessionPersistence");
      if (persistence) {
        m.handle = await persistence.create(session.header);
        const suffix = session.snapshotEvents(0);
        if (suffix.length > 0) await m.handle.append(suffix);
      }
    } catch (e) {
      console.log(`[grokcli] mirror(${spec.subagentId.slice(0, 8)}) persistence create failed: ${String(e).slice(0, 120)}`);
    }
    ctx.sessions.announce?.(session);
    _SubagentMirror.liveMirrors.set(String(id), m);
    void _SubagentMirror.attachToWorkspace(ctx, id).catch(() => {
    });
    console.log(`[grokcli] mirror open: ${spec.description ?? spec.subagentType ?? "subagent"} -> ${id.slice(0, 18)} (grok child ${spec.childSessionId.slice(0, 8)})`);
    return m;
  }
  /** 把镜像子会话挂进 subagents 工作区（resolveByPath/create + attachSession 双条件；
   *  Cordis 铁律：服务获取与调用整体 try/catch，任何失败只降级回未分组）。 */
  static async attachToWorkspace(ctx, sessionId) {
    let registry;
    try {
      registry = ctx.get("workspaceRegistry");
    } catch {
      return;
    }
    if (!registry) return;
    try {
      let ws = await registry.resolveByPath(SUBAGENTS_DIR);
      if (!ws) ws = await registry.create(SUBAGENTS_DIR);
      await ws.attachSession(sessionId);
      console.log(`[grokcli] mirror ${sessionId.slice(0, 18)} attached to subagents workspace`);
    } catch (e) {
      console.log(`[grokcli] mirror workspace attach failed (fall back \u672A\u5206\u7EC4): ${String(e).slice(0, 120)}`);
    }
  }
  /** 子会话收到它的任务提示（user_message_chunk）→ 开 turn 落 user/message */
  onTask(text) {
    if (this.finalized || !this.session) return;
    this.flushPendingNotices();
    this.turn += 1;
    this.turnOpen = true;
    this.session.append("turn/start", { turn: this.turn });
    this.session.append("user/message", {
      id: `user-${randomUUID2().slice(0, 8)}`,
      role: "user",
      content: [{ type: "text", text }],
      source: { kind: "user" }
    }, { surfaceOp: "append" });
    this.session.append("step/start", { turn: this.turn, step: 1 });
  }
  onChunk(text, thought) {
    if (this.finalized) return;
    if (thought) this.thoughtBuf += text;
    else this.textBuf += text;
  }
  onToolCall(callId, title, rawInput) {
    if (this.finalized || !this.session) return;
    const name2 = title || "tool";
    const args = typeof rawInput === "string" ? rawInput : JSON.stringify(rawInput ?? {});
    const advMessage = {
      id: `asst-${randomUUID2().slice(0, 8)}`,
      role: "assistant",
      content: [{ type: "tool-call", id: callId, name: name2, arguments: args }],
      source: { kind: "model", provider: "grok", model: this.model }
    };
    this.session.append("assistant/message", { turn: this.turn, step: 1, message: advMessage, stream: [] }, { surfaceOp: "append" });
    const ev = this.session.append("tool/call", {
      turn: this.turn,
      step: 1,
      callId,
      name: name2,
      arguments: args
    });
    this.pendingTools.set(callId, { name: name2, arguments: args, callSeq: ev?.seq });
  }
  onToolUpdate(callId, status, title, content, locations) {
    if (this.finalized || !this.session) return;
    const rec = this.pendingTools.get(callId);
    if (!rec || status !== "completed" && status !== "failed") return;
    this.pendingTools.delete(callId);
    const failed = status === "failed";
    const text = acpToolContentToText(content) || (failed ? "tool failed" : "(no content)");
    const message = {
      id: `tool-${randomUUID2().slice(0, 8)}`,
      role: "tool",
      content: [{ type: "text", text }],
      source: { kind: "tool", callId },
      toolCallId: callId,
      ...failed ? { isError: true } : {}
    };
    this.session.append("tool/result", {
      turn: this.turn,
      step: 1,
      message,
      ...failed ? { error: { name: "GrokToolError", code: "GROK_TOOL_FAILED", ...title ? { reason: title } : {} } } : {},
      ...Array.isArray(locations) && locations.length > 0 ? { meta: { locations } } : {}
    }, {
      surfaceOp: "append",
      ...rec.callSeq !== void 0 ? { sourceEventSeqs: [rec.callSeq] } : {}
    });
  }
  /** 子会话一次响应收尾（response_completed）→ 落 assistant/message（带该次 usage） */
  onResponseCompleted(usage) {
    if (this.finalized || !this.session) return;
    if (!this.textBuf && !this.thoughtBuf) return;
    const content = [];
    if (this.thoughtBuf) content.push({ type: "reasoning", text: this.thoughtBuf });
    content.push({ type: "text", text: this.textBuf || "(empty)" });
    const message = {
      id: `asst-${randomUUID2().slice(0, 8)}`,
      role: "assistant",
      content,
      source: { kind: "model", provider: "grok", model: this.model }
    };
    const u = grokUsageToTokenUsage(usage);
    this.session.append("assistant/message", {
      turn: this.turn,
      step: 1,
      message,
      stream: [],
      ...u ? { usage: u } : {}
    }, { surfaceOp: "append" });
    this.textBuf = "";
    this.thoughtBuf = "";
  }
  /** 子回合收尾（turn_completed）→ 闭合 turn（严格生命周期，用量面板依赖完整边界） */
  onTurnCompleted(stopReason, usage) {
    if (this.finalized || !this.session || !this.turnOpen) return;
    this.onResponseCompleted(usage);
    for (const [callId] of this.pendingTools) {
      const message = {
        id: `tool-${randomUUID2().slice(0, 8)}`,
        role: "tool",
        content: [{ type: "text", text: "turn ended before tool completed" }],
        source: { kind: "tool", callId },
        toolCallId: callId,
        isError: true
      };
      this.session.append("tool/result", { turn: this.turn, step: 1, message }, { surfaceOp: "append" });
    }
    this.pendingTools.clear();
    this.session.append("step/end", { turn: this.turn, step: 1 });
    const stop = stopReason || "end_turn";
    this.session.append("turn/end", {
      turn: this.turn,
      reason: stop === "end_turn" ? { kind: "completed" } : stop === "cancelled" ? { kind: "interrupted" } : { kind: "completed" }
    });
    this.turnOpen = false;
    this.flushPendingNotices();
    console.log(`[grokcli] mirror(${this.subagentId.slice(0, 8)}) turn ${this.turn} closed (${stop})`);
  }
  /** view-only 浏览时用户发消息 → 落一个提示回合（单本账：turn/seq 全由 mirror 分配，
   *  落库走 mirror 的 active writer → follow 通道实时上屏；回放也看得见）。
   *  子代理回合进行中则排队（嵌套 turn = 损坏）。finalized 后返回 false（调用方降级）。 */
  appendOperatorNotice(userText, notice) {
    if (this.finalized || !this.session) return false;
    this.pendingNotices.push({ userText, notice });
    this.flushPendingNotices();
    return true;
  }
  flushPendingNotices() {
    if (this.finalized || !this.session) return;
    while (!this.turnOpen && this.pendingNotices.length > 0) {
      const { userText, notice } = this.pendingNotices.shift();
      this.turn += 1;
      this.session.append("turn/start", { turn: this.turn });
      this.session.append("user/message", {
        id: `user-${randomUUID2().slice(0, 8)}`,
        role: "user",
        content: [{ type: "text", text: userText }],
        source: { kind: "user" }
      }, { surfaceOp: "append" });
      this.session.append("step/start", { turn: this.turn, step: 1 });
      this.session.append("assistant/message", {
        turn: this.turn,
        step: 1,
        message: {
          id: `asst-${randomUUID2().slice(0, 8)}`,
          role: "assistant",
          content: [{ type: "text", text: notice }],
          source: { kind: "model", provider: "grok", model: this.model }
        },
        stream: []
      }, { surfaceOp: "append" });
      this.session.append("step/end", { turn: this.turn, step: 1 });
      this.session.append("turn/end", { turn: this.turn, reason: { kind: "completed" } });
      console.log(`[grokcli] mirror(${this.subagentId.slice(0, 8)}) operator notice turn ${this.turn} appended (view-only \u62E6\u622A)`);
    }
  }
  /** 父代理 dispose 时收尾：冲刷 + 退出 store + 关写句柄（continuable 子代理可再开新 turn 续写） */
  async finalize() {
    if (this.finalized) return;
    try {
      this.flushPendingNotices();
    } catch {
    }
    this.finalized = true;
    if (this.dshSessionId !== null) _SubagentMirror.liveMirrors.delete(String(this.dshSessionId));
    try {
      if (this.turnOpen) this.onTurnCompleted("end_turn", void 0);
    } catch {
    }
    try {
      await this.handle?.close();
    } catch {
    }
    this.handle = null;
    try {
      this.detach?.();
    } catch {
    }
    this.detach = null;
  }
};

// src/bridge.ts
var PROVIDER = "grok";
function looksLikeGrokModel(m) {
  return !!m && /^(grok|apikey|grokdesk|xai)/i.test(m);
}
var GrokBridgeAgent = class {
  constructor(loopCtx, id, options, session, config, modelSource, restoreBinding) {
    this.loopCtx = loopCtx;
    this.config = config;
    this.modelSource = modelSource;
    this.id = id;
    this.options = options;
    this.session = session;
    const self = this;
    this.inbox = {
      get nextTurn() {
        return [...self.queue];
      },
      get nextStep() {
        return [];
      },
      clear() {
        self.queue = [];
      },
      append(target, message) {
        if (target === "next-turn") self.queue.push(message);
      },
      prepend(target, message) {
        if (target === "next-turn") self.queue.unshift(message);
      },
      replace() {
        return false;
      },
      remove() {
        return false;
      },
      splice(target, start, deleteCount, inserted) {
        const removed = self.queue.splice(start, deleteCount, ...inserted);
        return removed;
      }
    };
    this.driver = this.makeDriver("");
    this.restoreBinding = restoreBinding ?? null;
  }
  loopCtx;
  config;
  modelSource;
  id;
  session;
  ctx = null;
  options;
  inbox;
  phase = "idle";
  queue = [];
  /** 计划修订循环计数（rejected+意见自动发回 grok 改计划再交审；上限 3 次防死循环，
   *  真实用户输入到队即复位） */
  revisionLoop = 0;
  /** 只读浏览模式（resume 到「子代理镜像仍在写」的会话时置位）：不占写句柄、
   *  runTurn 拦截发消息（落提示回合），镜像结束后正常 resume 恢复可写。 */
  viewOnlyMirror = false;
  idleWaiters = [];
  runPromise = Promise.resolve();
  disposed = false;
  cancelRequested = false;
  currentAttempt = 0;
  revision = 0;
  /** grok 侧会话绑定：null = 下个回合建立 */
  acpSessionId = null;
  boundSpawnKey = "";
  boundWantModel = "";
  driver;
  dispatch = null;
  scopeDispose = null;
  /** model/selection 事件落下的下一回合模型/档位 */
  nextModel = null;
  nextEffort = null;
  /** resume 时从持久化事件里扫出的 grok 会话绑定（首个回合尝试 session/load 恢复） */
  restoreBinding = null;
  /** 子代理镜像：grok child sessionId -> mirror（spawn_subagent 委派的子会话实时转写进 dsh） */
  subagentMirrors = /* @__PURE__ */ new Map();
  makeDriver(modelProfile) {
    return new AcpDriver(
      {
        onUpdate: (sid, u) => this.onAcpUpdate(sid, u),
        onExitPlanMode: (params) => this.protocolExitPlan(params),
        onAskUserQuestion: (params) => this.protocolAskUser(params),
        onPermission: (req) => this.onAcpPermission(req),
        onRetryState: (sid, r) => {
          console.log(`[grokcli] retry attempt=${r.attempt} reason=${r.reason} (session ${sid.slice(0, 8)})`);
          this.lastRetry = r;
        },
        log: (msg, extra) => console.log(`[grokcli] ${msg}`, extra ?? "")
      },
      {
        grokBin: this.config.grokBin,
        realHome: this.config.realHome,
        cwd: this.session.header?.cwd || this.config.cwd,
        modelProfile: modelProfile || void 0,
        reasoningEffort: this.explicitEffort(),
        retryAbortMs: this.config.retryAbortMs,
        promptIdleMs: this.config.promptIdleMs,
        promptHardMs: this.config.promptHardMs
      }
    );
  }
  /** 本回合最后一次 retry_state（止损时用于生成解释性错误） */
  lastRetry = null;
  /** 计划审批（老大 2026-10-05 提案）：exit_plan_mode 在 ACP 桥接下无 TUI 审批键 →
   *  桥读出 plan.md 弹 dsh 原生审批面板；「允许」→ 收口本回合并自动把计划作为新任务
   *  发给全新 grok 会话继续实施；「拒绝」→ 计划留档收口；15 分钟无人操作 → 看门狗兜底。
   *  （--no-plan/--disallowed-tools 在 agent stdio 被静默忽略，实测三连锄件。） */
  planStuck = false;
  planExitTimer = null;
  planDecision = null;
  planText = null;
  /** ask_user_question（TUI 提问工具在桥接环境无输入面）：**升级为原生问答面板**
   *  （老大 2026-10-05 选型：ZCode 式点选）——ctx.userQuestions.ask 弹带选项按钮的
   *  面板，点选即答；答案自动发回同一 grok 会话（restoreBinding）续跑。面板不可用/
   *  超时则回退：45s/15min 看门狗收口+问题上屏等打字。 */
  askStuck = false;
  askTimer = null;
  askRawInput = null;
  askAnswer = null;
  /** 真·协议路径已接管交互（收到 _x.ai/* 服务端请求）：旧看门狗/收口全部让位 */
  protocolInteractive = false;
  effectiveModel() {
    const pick = (m) => {
      if (!m) return false;
      return looksLikeGrokModel(m) || this.modelSource.isKnownModel(m);
    };
    if (pick(this.nextModel ?? void 0)) return this.nextModel;
    if (pick(this.options.model)) return this.options.model;
    const chosen = this.nextModel ?? this.options.model;
    if (chosen) console.log(`[grokcli] model "${chosen}" not servable by grok bridge (not a grok/relay-catalog model) -> falling back to ${this.config.defaultModel || "(default profile)"}`);
    return this.config.defaultModel || "";
  }
  /** 会话持久化的模型选择（modelSelection 投影；controller 建 agent 时只传 provider/model 不带档位） */
  persistedSelection() {
    try {
      const projections = this.loopCtx.sessionProjections;
      const sel = projections?.stateOf(this.session, "modelSelection");
      return sel?.pending ?? sel?.lastUsed ?? sel ?? null;
    } catch {
      return null;
    }
  }
  /**
   * 显式选择的档位（UI 档位菜单/modelSelection 投影）。选 Default（未显式选档）返回
   * undefined：不下发 --reasoning-effort、不注入 reasoning_effort——完全尊重 grok/模型
   * 原生默认（实测教训：硬塞 overlay 默认档会与 grok 内建目录已认识模型的默认值打架，
   * 如 grok-4.6 原生默认 high vs overlay low → 「思考等级不一致」）。
   */
  explicitEffort() {
    const persisted = this.persistedSelection();
    const e = this.nextEffort ?? this.options.reasoningEffort ?? persisted?.reasoningEffort;
    let e2 = e;
    if (e2 === void 0) {
      try {
        const editor = this.loopCtx.get("configEditor");
        const row = editor?.configuration?.().find((r) => r.entry?.options?.id === "agent-default-model");
        const effort = row?.override?.reasoningEffort;
        if (effort && /^(off|minimal|low|medium|high|xhigh|max)$/i.test(effort)) e2 = effort.toLowerCase();
      } catch {
      }
    }
    if (e && /^(off|minimal|low|medium|high|xhigh|max)/i.test(e)) return e.toLowerCase();
    return e2;
  }
  /** 动态注入运行期助手（createScope/agentEvents）后生效；失败则降级为裸事件 */
  async bindRuntimeHelpers() {
    const loadModule = async (name2) => {
      try {
        return await import(name2);
      } catch {
      }
      const resources = process.resourcesPath;
      if (resources) {
        try {
          const { createRequire } = await import("node:module");
          const { pathToFileURL } = await import("node:url");
          const anchor = `${resources}/app.asar/dsh/node_modules/@deepseek-ai/dsh-agent/package.json`;
          const req = createRequire(anchor);
          const resolved = req.resolve(name2);
          console.log(`[grokcli] runtime: ${name2} via asar (${resolved.slice(0, 90)})`);
          return await import(pathToFileURL(resolved).href);
        } catch (e) {
          console.log(`[grokcli] runtime: ${name2} asar resolve failed (${String(e).slice(0, 100)})`);
        }
      }
      try {
        const { createRequire } = await import("node:module");
        const agentUrl = import.meta.resolve("@deepseek-ai/dsh-agent");
        const resolve = createRequire(agentUrl);
        return await import(resolve.resolve(name2));
      } catch (e) {
        console.log(`[grokcli] runtime: ${name2} all resolution paths failed (${String(e).slice(0, 100)})`);
      }
      throw new Error(`cannot load ${name2}`);
    };
    try {
      const agent = await loadModule("@deepseek-ai/dsh-agent");
      console.log("[grokcli] runtime: dsh-agent imported");
      let scope = null;
      try {
        scope = await loadModule("@deepseek-ai/dsh-scope");
        console.log("[grokcli] runtime: dsh-scope imported");
      } catch {
        console.log("[grokcli] runtime: dsh-scope UNRESOLVABLE; ctx unscoped");
      }
      if (scope) {
        try {
          const created = scope.createScope(this.loopCtx, this);
          this.ctx = created.ctx;
          this.scopeDispose = () => created.dispose();
          console.log("[grokcli] runtime: agent scope created");
        } catch (e) {
          console.log(`[grokcli] runtime: createScope THREW (${String(e).slice(0, 150)}); ctx unscoped`);
          this.ctx = this.loopCtx;
        }
      } else {
        this.ctx = this.loopCtx;
      }
      this.dispatch = agent.agentEvents(this.loopCtx, this);
    } catch (e) {
      console.log(`[grokcli] runtime: helper import failed, degraded (${String(e).slice(0, 150)})`);
      this.ctx = this.loopCtx;
    }
  }
  get status() {
    return this.phase;
  }
  setStatus(next) {
    if (this.phase === next) return;
    this.phase = next;
    this.dispatch?.emit("agent/status", { status: next });
  }
  // ── Agent 运行时面 ────────────────────────────────────────────────────────
  send(message, _target, wakeup) {
    this.queue.push(message);
    this.revisionLoop = 0;
    if (wakeup) this.wake();
  }
  followup(message) {
    this.send(message, "next-turn", true);
  }
  /** steer = 回合运行中的转向（「更像 zcode」③）：入队 + cancel 当前回合（grok 侧
   *  prompt 以 cancelled 收口，turn tail 记 interrupted）→ drain 接续处理转向指令——
   *  不必等当前回合跑完。空闲态调用（无活跃回合）退化为普通 followup。 */
  steer(message) {
    this.send(message, "next-turn", false);
    if (this.activeTurnContext && this.acpSessionId && !this.cancelRequested) {
      console.log(`[grokcli] steer: \u4E2D\u65AD\u5F53\u524D\u56DE\u5408\uFF0C\u6CE8\u5165\u65B0\u6307\u4EE4\uFF08${message.content?.[0]?.text?.slice(0, 40) ?? ""}\uFF09`);
      void this.driver.cancel(this.acpSessionId).catch(() => {
      });
    } else {
      this.wake();
    }
  }
  inject(message) {
    this.send(message, "next-step", false);
  }
  /** 前端「释放引擎」按钮的服务端执行体：断开本会话的 grok 连接（杀进程），上下文在
   *  磁盘，下条消息自动重连恢复。多会话挂机时按需释放空闲引擎。 */
  shutdownEngine() {
    if (this.boundSpawnKey || this.acpSessionId) {
      console.log(`[grokcli] engine shutdown by user (session ${String(this.id).slice(0, 18)})`);
      try {
        this.driver.dispose();
      } catch {
      }
      this.acpSessionId = null;
      this.boundSpawnKey = "";
      this.restoreBinding = this.lookupBinding(this.id);
    }
  }
  cancel(_cause, _options) {
    this.cancelRequested = true;
    if (this.acpSessionId) {
      void this.driver.cancel(this.acpSessionId);
    }
  }
  whenIdle() {
    if (this.phase === "idle" && this.queue.length === 0) return Promise.resolve();
    return new Promise((resolve) => this.idleWaiters.push(resolve));
  }
  async runMaintenance(task) {
    return await task(new AbortController().signal);
  }
  wake() {
    this.runPromise = this.runPromise.then(() => this.drain());
  }
  async drain() {
    if (this.disposed) return;
    while (this.queue.length > 0 && !this.disposed) {
      const message = this.queue.shift();
      try {
        await this.runTurn(message);
      } catch (e) {
        this.loopCtx.logger.error(`[grokcli] turn crashed: ${String(e)}`);
      }
    }
    for (const w of this.idleWaiters.splice(0)) w();
  }
  // ── 回合执行 ──────────────────────────────────────────────────────────────
  async runTurn(userMessage) {
    if (this.viewOnlyMirror) {
      const mirror = SubagentMirror.live(String(this.id));
      const userText = blocksToText(userMessage.content);
      const notice = "\u5B50\u4EE3\u7406\u6B63\u5728\u6B64\u4F1A\u8BDD\u4E2D\u8FD0\u884C\uFF08\u53EA\u8BFB\u6D4F\u89C8\u6A21\u5F0F\uFF09\uFF1A\u4E3A\u4FDD\u62A4\u5199\u5165\u4E0D\u4E92\u76F8\u5E72\u6270\uFF0C\u6682\u4E0D\u63A5\u53D7\u65B0\u6D88\u606F\u3002\u8BF7\u56DE\u5230\u7236\u4F1A\u8BDD\u64CD\u4F5C\uFF0C\u6216\u7B49\u5B50\u4EE3\u7406\u5B8C\u6210\u540E\u91CD\u65B0\u6253\u5F00\u672C\u4F1A\u8BDD\u3002";
      const appended = mirror?.appendOperatorNotice(userText, notice) ?? false;
      console.log(`[grokcli] view-only turn intercepted: "${userText.slice(0, 50)}" (notice appended=${appended})`);
      if (!appended) await this.showViewOnlyNoticeCard("\u5B50\u4EE3\u7406\u5DF2\u7ED3\u675F\u6216\u4F1A\u8BDD\u5DF2\u5F52\u6863\uFF0C\u8BF7\u5173\u95ED\u540E\u91CD\u65B0\u6253\u5F00\u672C\u4F1A\u8BDD\u5373\u53EF\u6B63\u5E38\u5BF9\u8BDD\u3002");
      this.setStatus("idle");
      return;
    }
    this.closeOpenTurnIfAny();
    this.cancelRequested = false;
    this.planStuck = false;
    this.planDecision = null;
    this.planText = null;
    this.askStuck = false;
    this.askRawInput = null;
    this.askAnswer = null;
    this.protocolInteractive = false;
    if (this.planExitTimer) {
      clearTimeout(this.planExitTimer);
      this.planExitTimer = null;
    }
    if (this.askTimer) {
      clearTimeout(this.askTimer);
      this.askTimer = null;
    }
    this.setStatus("running");
    const text = blocksToText(userMessage.content);
    const turn = this.lastTurnNumber() + 1;
    this.session.append("turn/start", { turn });
    this.session.append("user/message", userMessage, { surfaceOp: "append" });
    const wantModel = this.effectiveModel();
    const entries = await this.modelSource.profiles();
    const route = decideRoute(entries, wantModel);
    let baseOverride;
    const ownerEntry = entries.find((e) => `grokdesk-${e.id}` === route.spawnKey);
    if (ownerEntry && ownerEntry.baseUrl && ownerEntry.apiKey && route.model) {
      baseOverride = await this.modelSource.pinTarget(route.model, ownerEntry.baseUrl, this.explicitEffort()) ?? void 0;
    }
    if (entries.length > 0 && syncGrokProfiles(this.config.realHome, entries, route.model, baseOverride) && this.boundSpawnKey) {
      console.log("[grokcli] grokdesk \u6863\u6848\u53D8\u5316 -> \u91CD\u5EFA grok \u8FDE\u63A5\uFF08\u65B0\u8FDB\u7A0B\u8BFB\u65B0\u914D\u7F6E\uFF09");
      this.driver.dispose();
      this.boundSpawnKey = "";
    }
    this.session.append("request/header", {
      header: { config: { provider: PROVIDER, model: route.model || route.spawnKey || "grok", ...this.explicitEffort() ? { reasoningEffort: this.explicitEffort() } : {} } },
      reason: this.acpSessionId === null ? "initial" : "series"
    });
    const spawnKey = `${route.spawnKey}|${this.explicitEffort() ?? ""}`;
    if (this.acpSessionId === null || this.boundSpawnKey !== spawnKey) {
      try {
        if (this.boundSpawnKey && this.boundSpawnKey !== spawnKey) {
          this.driver.dispose();
        }
        if (this.boundSpawnKey !== spawnKey) {
          this.driver = this.makeDriver(route.spawnKey);
        }
        let restoredId = null;
        if (this.acpSessionId === null && this.restoreBinding) {
          restoredId = await this.driver.loadSession(this.restoreBinding.grokSessionId);
          console.log(restoredId ? `[grokcli] session restored: ${restoredId.slice(0, 8)} (spawn ${spawnKey})` : "[grokcli] session restore failed, falling back to new session");
          this.restoreBinding = null;
        }
        this.acpSessionId = restoredId ?? await this.driver.newSession();
        this.boundSpawnKey = spawnKey;
        this.boundWantModel = wantModel;
        this.modelSource.saveBinding(this.id, this.acpSessionId, spawnKey, wantModel);
      } catch (e) {
        this.failTurn(turn, `grok session create failed: ${String(e)}`);
        return;
      }
    }
    let splitter = makeThinkSplitter();
    const turnTime0 = Date.now();
    this.currentAttempt += 1;
    const attemptBase = `grok-${this.currentAttempt}-${randomUUID3().slice(0, 8)}`;
    let attemptId = attemptBase;
    let attemptSeq = 0;
    this.revision += 1;
    const revision = this.revision;
    let textBuf = "";
    let thoughtBuf = "";
    const textChunks = [];
    const thoughtChunks = [];
    const textDt = [];
    let time0 = Date.now();
    let anyLiveThought = false;
    let streamStarted = false;
    let anyDurableContent = false;
    let frameSeq = 0;
    let chunkIdx = 0;
    let stepNo = 0;
    let stepOpen = false;
    const openStep = () => {
      if (!stepOpen) {
        stepNo += 1;
        this.session.append("step/start", { turn, step: stepNo });
        stepOpen = true;
      }
      return stepNo;
    };
    const maybeCloseStep = () => {
      if (!stepOpen || streamStarted || pendingTools.size > 0) return;
      this.session.append("step/end", { turn, step: stepNo });
      stepOpen = false;
    };
    const startFrame = () => {
      if (!streamStarted) {
        streamStarted = true;
        chunkIdx = 0;
        attemptSeq += 1;
        attemptId = `${attemptBase}#s${attemptSeq}`;
        frameSeq += 1;
        this.dispatch?.emit("agent/assistant-stream", { frame: { type: "start", attemptId, revision: frameSeq, turn, step: stepNo } });
      }
    };
    const frameMeta = () => {
      frameSeq += 1;
      chunkIdx += 1;
      return { revision: frameSeq, index: chunkIdx - 1 };
    };
    const usageQueue = [];
    let flushedUsageAny = false;
    const flushUsage = () => {
      if (usageQueue.length === 0) return;
      if (!stepOpen) {
        usageQueue.length = 0;
        return;
      }
      for (const u of usageQueue) {
        try {
          this.session.append("assistant/attempt", {
            turn,
            step: stepNo,
            stream: [{ type: "chunk", time: Date.now(), chunk: { type: "usage", usage: u } }]
          });
        } catch (e) {
          console.log(`[grokcli] live usage append failed: ${String(e).slice(0, 80)}`);
        }
      }
      usageQueue.length = 0;
      flushedUsageAny = true;
    };
    const settleSegment = (opts = {}) => {
      for (const piece of splitter.flush()) {
        if (piece.kind === "thought") {
          thoughtBuf += piece.text;
          thoughtChunks.push(piece.text);
        } else if (piece.text) {
          textBuf += piece.text;
          textChunks.push(piece.text);
          textDt.push(Date.now() - time0);
        }
      }
      if (!streamStarted) return;
      streamStarted = false;
      const content = [];
      if (thoughtBuf) content.push({ type: "reasoning", text: thoughtBuf });
      if (textBuf || content.length === 0) content.push({ type: "text", text: textBuf || "(empty segment)" });
      const assistantMessage = {
        id: `asst-${randomUUID3().slice(0, 8)}`,
        role: "assistant",
        content,
        source: { kind: "model", provider: PROVIDER, model: this.boundWantModel || route.model || "grok" }
      };
      const stream = [];
      if (thoughtChunks.length) stream.push({ type: "reasoning-chunks", time0, index: 1, dt: thoughtChunks.map((_, i) => i), texts: thoughtChunks });
      if (textChunks.length) stream.push({ type: "text-chunks", time0, index: 0, dt: textDt, texts: textChunks });
      const appended = this.session.append("assistant/message", {
        turn,
        step: stepNo,
        message: assistantMessage,
        stream,
        ...opts.interrupted ? { interrupted: true } : {}
      }, { surfaceOp: "append" });
      anyDurableContent = true;
      frameSeq += 1;
      this.dispatch?.emit("agent/assistant-stream", {
        frame: { type: "end", attemptId, revision: frameSeq, index: chunkIdx, outcome: { kind: "committed", eventType: "assistant/message", seq: appended.seq } }
      });
      console.log(`[grokcli] segment#${attemptSeq} settled: step=${stepNo} text=${textBuf.length}B think=${thoughtBuf.length}B seq=${appended.seq}`);
      flushUsage();
      maybeCloseStep();
      splitter = makeThinkSplitter();
      textBuf = "";
      thoughtBuf = "";
      textChunks.length = 0;
      thoughtChunks.length = 0;
      textDt.length = 0;
      time0 = Date.now();
    };
    const pendingTools = /* @__PURE__ */ new Map();
    this.activeTurnContext = {
      turn,
      step: 0,
      attemptId: attemptBase,
      // step 实际由 openStep 懒分配（见上）
      onText: (t) => {
        console.log(`[grokcli] text-chunk ${(/* @__PURE__ */ new Date()).toISOString().slice(14, 23)} +${t.length}B`);
        openStep();
        startFrame();
        for (const piece of splitter.feed(t)) {
          if (piece.kind === "thought") {
            anyLiveThought = true;
            thoughtBuf += piece.text;
            thoughtChunks.push(piece.text);
            this.dispatch?.emit("agent/assistant-stream", {
              frame: { type: "chunk", attemptId, ...frameMeta(), time: Date.now(), chunk: { type: "reasoning-delta", index: 1, text: piece.text } }
            });
          } else if (piece.text) {
            textBuf += piece.text;
            textChunks.push(piece.text);
            textDt.push(Date.now() - time0);
            this.dispatch?.emit("agent/assistant-stream", {
              frame: { type: "chunk", attemptId, ...frameMeta(), time: Date.now(), chunk: { type: "text-delta", index: 0, text: piece.text } }
            });
          }
        }
      },
      onThought: (t) => {
        if (anyLiveThought) {
          console.log(`[grokcli] thought \u53BB\u91CD\uFF1A\u4E22\u5F03\u56DE\u5408\u672B\u91CD\u590D\u6458\u8981 ${t.length}B\uFF08\u76F4\u64AD\u5DF2\u5C55\u793A\uFF09`);
          return;
        }
        openStep();
        startFrame();
        thoughtBuf += t;
        thoughtChunks.push(t);
        this.dispatch?.emit("agent/assistant-stream", {
          frame: { type: "chunk", attemptId, ...frameMeta(), time: Date.now(), chunk: { type: "reasoning-delta", index: 1, text: t } }
        });
      },
      onToolCall: (callId, title, rawInput) => {
        settleSegment();
        openStep();
        const name2 = title || "tool";
        const args = typeof rawInput === "string" ? rawInput : JSON.stringify(rawInput ?? {});
        if (name2 === "ask_user_question" && !this.askTimer && !this.protocolInteractive) {
          this.askRawInput = args;
          const keepSid = this.acpSessionId;
          const cancelTurn = () => {
            if (keepSid) this.restoreBinding = { grokSessionId: keepSid };
            if (this.acpSessionId) void this.driver.cancel(this.acpSessionId).catch(() => {
            });
          };
          this.askTimer = setTimeout(() => {
            this.askTimer = null;
            if (this.activeTurnContext && !this.cancelRequested && this.askAnswer === null && !this.askStuck && !this.protocolInteractive) {
              console.log("[grokcli] ask_user_question fallback (\u534F\u8BAE\u8BF7\u6C42 45s \u672A\u5230) -> cancel \u6536\u53E3");
              this.askStuck = true;
              cancelTurn();
            }
          }, 45e3);
          this.askTimer.unref?.();
        }
        if (name2 === "exit_plan_mode" && !this.planExitTimer && !this.protocolInteractive) {
          this.planExitTimer = setTimeout(() => {
            this.planExitTimer = null;
            if (this.activeTurnContext && !this.cancelRequested && this.planDecision === null) {
              console.log("[grokcli] plan approval timeout (15min) -> auto close turn");
              this.planStuck = true;
              if (this.acpSessionId) void this.driver.cancel(this.acpSessionId).catch(() => {
              });
            }
          }, 15 * 6e4);
          this.planExitTimer.unref?.();
        }
        const advMessage = {
          id: `asst-${randomUUID3().slice(0, 8)}`,
          role: "assistant",
          content: [{ type: "tool-call", id: callId, name: name2, arguments: args }],
          source: { kind: "model", provider: PROVIDER, model: this.boundWantModel || route.model || "grok" }
        };
        this.session.append("assistant/message", { turn, step: stepNo, message: advMessage, stream: [] }, { surfaceOp: "append" });
        const ev = this.session.append("tool/call", { turn, step: stepNo, callId, name: name2, arguments: args });
        pendingTools.set(callId, { name: name2, arguments: args, callSeq: ev?.seq });
      },
      onToolUpdate: (callId, status, title, content, locations) => {
        const rec = pendingTools.get(callId);
        if (!rec || !status) return;
        if (status !== "completed" && status !== "failed") return;
        pendingTools.delete(callId);
        const failed = status === "failed";
        const text2 = acpToolContentToText(content) || (failed ? "tool failed" : "(no content)");
        const message = {
          id: `tool-${randomUUID3().slice(0, 8)}`,
          role: "tool",
          content: [{ type: "text", text: text2 }],
          source: { kind: "tool", callId },
          toolCallId: callId,
          ...failed ? { isError: true } : {}
        };
        this.session.append("tool/result", {
          turn,
          step: stepNo,
          message,
          ...failed ? { error: { name: "GrokToolError", code: "GROK_TOOL_FAILED", ...title ? { reason: title } : {} } } : {},
          ...Array.isArray(locations) && locations.length > 0 ? { meta: { locations } } : {}
        }, {
          surfaceOp: "append",
          ...rec.callSeq !== void 0 ? { sourceEventSeqs: [rec.callSeq] } : {}
        });
        console.log(`[grokcli] tool/result ${String(callId).slice(0, 8)} status=${status} text=${text2.length}B${Array.isArray(locations) && locations.length ? ` loc=${locations.length}` : ""}`);
        maybeCloseStep();
      },
      // 交互工具（ask_user_question/exit_plan_mode）走 _x.ai 协议应答收口，grok 不再发
      // 终态 tool_call_update → tool/call 永远 pending：UI 工具卡「运行中」不收、问答投影
      // 卡不关、composer 被锁（实测 1.0.49）。协议应答后按工具名补写配对 tool/result。
      settleToolByName: (name2, text2) => {
        openStep();
        for (const [callId, rec] of [...pendingTools]) {
          if (rec.name !== name2) continue;
          pendingTools.delete(callId);
          const message = {
            id: `tool-${randomUUID3().slice(0, 8)}`,
            role: "tool",
            content: [{ type: "text", text: text2 }],
            source: { kind: "tool", callId },
            toolCallId: callId
          };
          this.session.append("tool/result", { turn, step: stepNo, message }, {
            surfaceOp: "append",
            ...rec.callSeq !== void 0 ? { sourceEventSeqs: [rec.callSeq] } : {}
          });
          console.log(`[grokcli] interactive tool settled: ${name2} ${String(callId).slice(0, 8)} text=${text2.length}B`);
        }
        maybeCloseStep();
      },
      // 单次模型请求边界（协议实录：文本流 → response_completed → tool_call → …）：
      // 收口当前流式段。grok 侧工具执行与其后下一轮文本都从新 step 开始。
      onResponseCompleted: (_usage) => {
        settleSegment();
      }
    };
    const unlistenUsage = onPinUsage((u) => {
      usageQueue.push(u);
    });
    let stopReason = "end_turn";
    let failure = null;
    this.lastRetry = null;
    try {
      stopReason = await this.driver.prompt(this.acpSessionId, text);
    } catch (e) {
      failure = String(e?.message || e);
      if (this.acpSessionId && /connection lost|unknown session|-32602|Invalid params/i.test(failure)) {
        const dead = this.acpSessionId;
        if (!/unknown session|-32602|Invalid params/i.test(failure)) {
          console.log(`[grokcli] connection lost -> invalidate session ${dead.slice(0, 8)} (\u4E0B\u56DE\u5408\u7ECF\u7ED1\u5B9A\u6062\u590D)`);
          this.acpSessionId = null;
          this.boundSpawnKey = "";
        } else {
          const bindingId = this.lookupBinding(this.id)?.grokSessionId;
          console.log(`[grokcli] session ${dead.slice(0, 8)} unknown on connection -> self-heal (reload from disk)`);
          try {
            this.acpSessionId = await this.driver.loadSession(dead) ?? (bindingId && bindingId !== dead ? await this.driver.loadSession(bindingId) : null) ?? await this.driver.newSession();
            console.log(`[grokcli] self-heal session: ${this.acpSessionId.slice(0, 8)} (was ${dead.slice(0, 8)})`);
            failure = null;
            stopReason = await this.driver.prompt(this.acpSessionId, text);
          } catch (e2) {
            failure = String(e2?.message || e2);
            stopReason = "cancelled";
          }
        }
      }
      stopReason = stopReason === "end_turn" && failure ? "cancelled" : stopReason;
      if (!this.cancelRequested && this.acpSessionId && failure) {
        void this.driver.cancel(this.acpSessionId).catch(() => {
        });
      }
      if (failure && /ECONNRESET|ECONNREFUSED|socket hang up|EPIPE/i.test(failure)) {
        console.log("[grokcli] \u8FDE\u63A5\u7C7B\u81F4\u547D\u9519\u8BEF -> dispose \u6740 grok \u8FDB\u7A0B\uFF08\u9632\u5B64\u513F\uFF09");
        try {
          this.driver.dispose();
        } catch {
        }
        this.acpSessionId = null;
        this.boundSpawnKey = "";
      }
      if (/^ACP session\/prompt timeout/.test(failure)) {
        failure = "\u56DE\u5408\u8D85\u65F6\uFF1A\u957F\u65F6\u95F4\u65E0\u4EFB\u4F55\u6A21\u578B\u8F93\u51FA\uFF0C\u5DF2\u653E\u5F03\u7B49\u5F85\u5E76\u81EA\u52A8\u505C\u6B62 grok \u4FA7\u4EFB\u52A1\uFF08\u6A21\u578B\u6B63\u5E38\u5DE5\u4F5C\u65F6\u6709\u6301\u7EED\u8F93\u51FA\u4E0D\u4F1A\u89E6\u53D1\uFF1B\u6B64\u60C5\u51B5\u591A\u4E3A\u4E2D\u8F6C/\u7F51\u7EDC\u5361\u6B7B\uFF0C\u53EF\u91CD\u8BD5\u6216\u65B0\u5EFA\u4F1A\u8BDD\uFF09";
      }
      if (this.cancelRequested) stopReason = "cancelled";
    } finally {
      unlistenUsage();
      this.activeTurnContext = null;
    }
    const abortedRetry = this.driver.retryAbortedOf(this.acpSessionId);
    if (abortedRetry) {
      const transient = isTransientRetryReason(abortedRetry.reason);
      if (transient) {
        const keepSid = this.acpSessionId;
        failure = `\u6A21\u578B\u901A\u9053\u4E34\u65F6\u6545\u969C\u5DF2\u81EA\u52A8\u6B62\u635F\uFF1A${abortedRetry.reason}\uFF08\u5DF2\u91CD\u8BD5 ${abortedRetry.attempt} \u6B21\u4ECD\u65E0\u6062\u590D\uFF09\u3002\u4E0A\u4E0B\u6587\u5DF2\u4FDD\u7559\u2014\u2014\u8BF7\u7A0D\u540E**\u76F4\u63A5\u91CD\u53D1\u8FD9\u6761\u6D88\u606F**\uFF0C\u4F1A\u81EA\u52A8\u91CD\u8FDE\u5E76\u4ECE\u539F\u8FDB\u5EA6\u7EE7\u7EED\uFF08\u65E0\u9700\u65B0\u5EFA\u4F1A\u8BDD\uFF1B\u82E5\u6301\u7EED\u5931\u8D25\u518D\u8003\u8651\u65B0\u5EFA\u6216\u68C0\u67E5\u4E2D\u8F6C\uFF09\u3002`;
        stopReason = "cancelled";
        this.acpSessionId = null;
        try {
          this.driver.dispose();
        } catch {
        }
        this.boundSpawnKey = "";
        if (keepSid) this.restoreBinding = { grokSessionId: keepSid };
        console.log(`[grokcli] transient abort: binding kept for reconnect (grok session ${keepSid?.slice(0, 8)})`);
      } else {
        failure = `\u6A21\u578B\u901A\u9053\u5F02\u5E38\u5DF2\u81EA\u52A8\u6B62\u635F\uFF1A${abortedRetry.reason}\uFF08\u91CD\u8BD5 ${abortedRetry.attempt} \u6B21\u65E0\u8FDB\u5C55\uFF09\u3002\u591A\u4E3A\u4E2D\u8F6C/\u5BC6\u94A5/\u534F\u8BAE\u4E0D\u5339\u914D\u6216\u4F1A\u8BDD\u4E0A\u4E0B\u6587\u8FC7\u5927\u2014\u2014\u8BF7\u5230 \u8BBE\u7F6E\u2192\u6A21\u578B\u2192\u7F16\u8F91 \u63D0\u4F9B\u5546 \u6838\u5BF9\u5BC6\u94A5/\u6362\u534F\u8BAE\uFF0C\u6216\u70B9\u300C\u65B0\u5EFA\u4F1A\u8BDD\u300D\u91CD\u5F00\uFF08grok \u4FA7\u4F1A\u8BDD\u5DF2\u81EA\u52A8\u91CD\u7F6E\uFF09\u3002`;
        stopReason = "cancelled";
        this.acpSessionId = null;
        try {
          this.driver.dispose();
        } catch {
        }
        this.boundSpawnKey = "";
      }
    }
    settleSegment({ interrupted: stopReason === "cancelled" });
    if (this.planExitTimer) {
      clearTimeout(this.planExitTimer);
      this.planExitTimer = null;
    }
    if (this.askTimer) {
      clearTimeout(this.askTimer);
      this.askTimer = null;
    }
    let usage = null;
    try {
      usage = this.modelSource.usageSince?.(turnTime0) ?? null;
    } catch {
      usage = null;
    }
    if (usage) console.log(`[grokcli] usage: in=${usage.inputTokens} cacheR=${usage.cacheReadTokens} out=${usage.outputTokens} total=${usage.totalTokens}`);
    const appendTailMessage = (text2) => {
      openStep();
      const message = {
        id: `asst-${randomUUID3().slice(0, 8)}`,
        role: "assistant",
        content: [{ type: "text", text: text2 }],
        source: { kind: "model", provider: PROVIDER, model: this.boundWantModel || route.model || "grok" }
      };
      this.session.append("assistant/message", {
        turn,
        step: stepNo,
        message,
        // 本回合已冲刷过段级 usage 时不带总量（tokenUsage 投影按 turn:step 槽位替换累加，
        // 尾消息若开新 step 再带总量会把已计的段用量重复叠加）
        ...!flushedUsageAny && usage ? { usage } : {}
      }, { surfaceOp: "append" });
      anyDurableContent = true;
    };
    if (this.askAnswer !== null || this.askStuck) {
      stopReason = "end_turn";
      failure = null;
      this.acpSessionId = null;
      this.boundSpawnKey = "";
      const q = extractAskQuestion(this.askRawInput);
      if (this.askAnswer !== null) {
        appendTailMessage(`\u2753\u2192\u2705 grok \u7684\u63D0\u95EE\uFF08\u5DF2\u5728\u9762\u677F\u70B9\u9009\u4F5C\u7B54\uFF09\uFF1A

---
${q}
---

\u4F60\u7684\u9009\u62E9\uFF1A**${this.askAnswer}**
\uFF08\u5DF2\u81EA\u52A8\u53D1\u56DE\u540C\u4E00 grok \u4F1A\u8BDD\uFF0C\u4EFB\u52A1\u7EE7\u7EED\u2014\u2014\u89C1\u4E0B\u4E00\u6761\u6D88\u606F\uFF09`);
        const impl = {
          id: `user-ask-${randomUUID3().slice(0, 8)}`,
          role: "user",
          content: [{ type: "text", text: `\u7528\u6237\u521A\u521A\u5728\u95EE\u7B54\u9762\u677F\u5BF9\u4F60\u63D0\u51FA\u7684\u95EE\u9898\u4F5C\u51FA\u4E86\u9009\u62E9\uFF0C\u8BF7\u636E\u6B64\u7EE7\u7EED\u4EFB\u52A1\uFF1A
${this.askAnswer}` }],
          source: { kind: "user" }
        };
        this.queue.push(impl);
        console.log("[grokcli] ask answered -> auto-continue queued (same grok session)");
      } else {
        appendTailMessage("\u2753 grok \u5728\u7B49\u4F60\u56DE\u7B54\u4EE5\u4E0B\u95EE\u9898\uFF08\u8BE5\u5DE5\u5177\u5728\u6865\u63A5\u73AF\u5883\u6CA1\u6709\u8F93\u5165\u6846\uFF0C\u56DE\u5408\u5DF2\u81EA\u52A8\u6536\u53E3\uFF09\u3002\n\n\u8BF7**\u76F4\u63A5\u5728\u8F93\u5165\u6846\u56DE\u590D**\uFF0C\u4F60\u7684\u56DE\u7B54\u4F1A\u5E26\u56DE\u540C\u4E00\u4E2A grok \u4F1A\u8BDD\u7EE7\u7EED\u4EFB\u52A1\uFF1A\n\n---\n" + q + "\n---");
      }
      console.log(`[grokcli] ask_user_question closed: ${this.askAnswer !== null ? "answered" : "fallback-text"}`);
    } else if (this.planDecision !== null || this.planStuck) {
      const plan = this.planText;
      stopReason = "end_turn";
      failure = null;
      this.acpSessionId = null;
      this.boundSpawnKey = "";
      if (this.planDecision === "approved") {
        appendTailMessage(`\u2705 \u8BA1\u5212\u5DF2\u6279\u51C6\uFF08\u539F grok \u4F1A\u8BDD\u5DF2\u6536\u53E3\uFF0C\u81EA\u52A8\u5F00\u59CB\u5B9E\u65BD\u2014\u2014\u89C1\u4E0B\u4E00\u6761\u6D88\u606F\uFF09\uFF1A

---
${plan ?? "\uFF08\u672A\u627E\u5230 plan.md\uFF09"}
---`);
        if (plan) {
          const impl = {
            id: `user-plan-${randomUUID3().slice(0, 8)}`,
            role: "user",
            content: [{ type: "text", text: `\u8BA1\u5212\u5DF2\u83B7\u7528\u6237\u6279\u51C6\uFF0C\u8BF7\u4E25\u683C\u6309\u4EE5\u4E0B\u8BA1\u5212\u5F00\u59CB\u5B9E\u65BD\uFF08\u5168\u65B0\u4F1A\u8BDD\uFF0C\u8BA1\u5212\u5373\u5168\u90E8\u4E0A\u4E0B\u6587\uFF09\uFF1A

${plan}` }],
            source: { kind: "user" }
          };
          this.queue.push(impl);
        }
      } else {
        const why = this.planStuck ? "\u7B49\u5F85\u5BA1\u6279\u8D85\u65F6\uFF0815 \u5206\u949F\uFF09\uFF0C\u5DF2\u81EA\u52A8\u6536\u53E3" : "\u4F60\u62D2\u7EDD\u4E86\u8BE5\u8BA1\u5212";
        appendTailMessage(`\u26A0\uFE0F grok \u8FDB\u5165\u8BA1\u5212\u6A21\u5F0F\u5E76\u7B49\u5F85\u5BA1\u6279\uFF0C${why}\u3002

\u8BA1\u5212\u5168\u6587\u7559\u6863\uFF08\u8BE5 grok \u4F1A\u8BDD\u5DF2\u5F03\u7528\uFF0C\u4E0B\u6761\u6D88\u606F\u4ECE\u5168\u65B0\u4F1A\u8BDD\u5F00\u59CB\uFF1B\u8981\u6267\u884C\u8BF7\u628A\u8981\u70B9\u8D34\u56DE\u6765\uFF09\uFF1A

---
${plan ?? "\uFF08\u672A\u627E\u5230 plan.md\uFF09"}
---`);
      }
      console.log(`[grokcli] plan flow closed: decision=${this.planDecision ?? "timeout"} plan=${plan ? `${plan.length}B` : "missing"}${this.planDecision === "approved" ? " -> auto-implement queued" : ""}`);
    } else if (!anyDurableContent) {
      appendTailMessage(failure ? `\u26A0\uFE0F ${failure}` : "(empty)");
    }
    console.log(`[grokcli] turn tail: stop=${stopReason} failure=${failure ? failure.slice(0, 60) : "-"} segments=${attemptSeq} steps=${stepNo}`);
    flushUsage();
    for (const [callId] of pendingTools) {
      const message = {
        id: `tool-${randomUUID3().slice(0, 8)}`,
        role: "tool",
        content: [{ type: "text", text: "turn ended before tool completed" }],
        source: { kind: "tool", callId },
        toolCallId: callId,
        isError: true
      };
      this.session.append("tool/result", { turn, step: stepNo, message }, { surfaceOp: "append" });
    }
    pendingTools.clear();
    maybeCloseStep();
    this.session.append("turn/end", { turn, reason: stopReasonToEndReason(stopReason, failure) });
    console.log(`[grokcli] turn tail: turn/end appended`);
    this.setStatus("idle");
  }
  activeTurnContext = null;
  failTurn(turn, message) {
    this.session.append("turn/end", { turn, reason: { kind: "error", error: { message, code: "grok_bridge_error" } } });
    this.setStatus("idle");
  }
  lastTurnNumber() {
    try {
      const projections = this.loopCtx.sessionProjections;
      return projections?.stateOf(this.session, "turnBoundary")?.lastTurn ?? 0;
    } catch {
      return 0;
    }
  }
  /** 未闭合回合兜底（2026-10-06 嵌套 turn 事故）：被强杀/崩溃的回合没有 turn/end——
   *  resume 的冷读不校验嵌套，新回合直接 turn/start 会写出嵌套 turn（dsh relationships
   *  校验器随后判整个会话损坏）。开新回合前若投影里还有 open turn，先补 step/end +
   *  turn/end（interrupted）闭合它。
   *  2026-10-08 修复（实锄件 session-f9e42ad5）：段化 step 后一个回合可有任意多个
   *  step，且 step 可能全部已闭（仅 turn 悬空）——旧版硬编码补 step/end{step:1} 在
   *  「无开启 step」时落野事件，直接毒化日志（step/end does not match an open turn
   *  and step）。改为回放事件流算出真实开启 step：有则按实际号闭合，无则只闭 turn。 */
  closeOpenTurnIfAny() {
    try {
      const projections = this.loopCtx.sessionProjections;
      const st = projections?.stateOf(this.session, "turnBoundary");
      if (!st || st.openTurnStartSeq == null) return;
      const turn = st.lastTurn ?? 0;
      if (turn <= 0) return;
      console.log(`[grokcli] open turn ${turn} detected (\u88AB\u5F3A\u6740/\u5D29\u6E83\u7684\u56DE\u5408) -> \u8865\u95ED\u5408`);
      let openStep = null;
      try {
        const events = this.session.snapshotEvents(0);
        for (const ev of events) {
          if (ev.type === "turn/start" || ev.type === "turn/end") openStep = null;
          else if (ev.type === "step/start") openStep = ev.data?.step ?? null;
          else if (ev.type === "step/end") openStep = null;
        }
      } catch (e) {
        console.log(`[grokcli] open-step scan failed (${String(e).slice(0, 80)}) -> \u53EA\u95ED turn`);
        openStep = null;
      }
      if (openStep !== null) {
        this.session.append("step/end", { turn, step: openStep });
        console.log(`[grokcli] \u8865\u95ED\u5408 step/end turn=${turn} step=${openStep}`);
      }
      this.session.append("turn/end", { turn, reason: { kind: "interrupted" } });
    } catch {
    }
  }
  // ── ACP 投影 & 权限桥 ─────────────────────────────────────────────────────
  /** ACP 事件路由（2026-10-05 抓线实证：params.sessionId 干净区分父子——父会话事件带父
   *  id，spawn_subagent 派生的子会话事件带子 id；subagent_* 生命周期在父流上）。 */
  onAcpUpdate(sid, u) {
    const kind = u.sessionUpdate;
    if (sid && this.acpSessionId && sid !== this.acpSessionId) {
      const mirror = this.subagentMirrors.get(sid);
      if (mirror) {
        this.routeToMirror(mirror, u);
      } else if (kind !== "available_commands_update" && kind !== "session_info_update" && kind !== "tool_call_delta_chunk") {
        console.log(`[grokcli] unattributed child update sid=${sid.slice(0, 8)} kind=${kind} ignored`);
      }
      return;
    }
    if (kind === "subagent_spawned") {
      void this.openSubagentMirror(u);
      return;
    }
    if (kind === "subagent_progress") return;
    const c = this.activeTurnContext;
    if (!c) return;
    switch (kind) {
      case "agent_message_chunk": {
        const text = u.content?.text || "";
        if (text) c.onText(text);
        return;
      }
      case "agent_thought_chunk": {
        const text = u.content?.text || "";
        if (text) c.onThought(text);
        return;
      }
      case "tool_call": {
        const { toolCallId, title, rawInput } = u;
        c.onToolCall(toolCallId || "unknown", title || "tool", typeof rawInput === "string" ? rawInput : JSON.stringify(rawInput ?? {}));
        return;
      }
      case "tool_call_update": {
        const { toolCallId, status, title, content, locations } = u;
        c.onToolUpdate(toolCallId, status, title, content, locations);
        return;
      }
      case "response_completed": {
        c.onResponseCompleted(u.usage);
        return;
      }
      default:
        return;
    }
  }
  async openSubagentMirror(f) {
    try {
      const spec = {
        subagentId: String(f.subagent_id ?? f.child_session_id ?? ""),
        childSessionId: String(f.child_session_id ?? f.subagent_id ?? ""),
        description: typeof f.description === "string" ? f.description : void 0,
        subagentType: typeof f.subagent_type === "string" ? f.subagent_type : void 0
      };
      if (!spec.childSessionId || this.subagentMirrors.has(spec.childSessionId)) return;
      const cwd = this.session.header?.cwd || this.config.cwd;
      const mirror = await SubagentMirror.open(spec, this.loopCtx, this.id, cwd, this.boundWantModel || "grok");
      this.subagentMirrors.set(spec.childSessionId, mirror);
    } catch (e) {
      console.log(`[grokcli] subagent mirror open failed: ${String(e).slice(0, 150)}`);
    }
  }
  routeToMirror(m, u) {
    switch (u.sessionUpdate) {
      case "user_message_chunk": {
        const text = u.content?.text || "";
        if (text) m.onTask(text);
        return;
      }
      case "agent_message_chunk": {
        const text = u.content?.text || "";
        if (text) m.onChunk(text, false);
        return;
      }
      case "agent_thought_chunk": {
        const text = u.content?.text || "";
        if (text) m.onChunk(text, true);
        return;
      }
      case "tool_call": {
        const { toolCallId, title, rawInput } = u;
        m.onToolCall(toolCallId || "unknown", title || "tool", typeof rawInput === "string" ? rawInput : JSON.stringify(rawInput ?? {}));
        return;
      }
      case "tool_call_update": {
        const { toolCallId, status, title, content, locations } = u;
        m.onToolUpdate(toolCallId, status, title, content, locations);
        return;
      }
      case "response_completed":
        m.onResponseCompleted(u.usage);
        return;
      case "turn_completed": {
        const tu = u;
        m.onTurnCompleted(tu.stop_reason, tu.usage);
        return;
      }
      default:
        return;
    }
  }
  // ── grok 私有扩展的真协议应答（_x.ai/*：回合内继续，不收口不换会话）────────
  /** `_x.ai/exit_plan_mode`：planContent 在请求参数里，弹审批面板；approved→grok
   *  同回合退出计划模式继续实施；rejected→修订闭环（④：弹可选意见卡，意见随
   *  rejected 的 comments 回给 grok——线格式支持 comments?: string，实证自
   *  @1agents bridge.js:1647；grok 可带意见修订计划再交审）。 */
  async protocolExitPlan(params) {
    this.protocolInteractive = true;
    if (this.planExitTimer) {
      clearTimeout(this.planExitTimer);
      this.planExitTimer = null;
    }
    if (this.askTimer) {
      clearTimeout(this.askTimer);
      this.askTimer = null;
    }
    this.planText = params.planContent ?? null;
    const decision = await this.requestPlanApproval();
    let comments;
    if (decision === "rejected") {
      comments = await this.askRevisionComments();
    }
    console.log(`[grokcli] exit_plan_mode protocol answer: ${decision}${comments ? ` comments=${comments.slice(0, 60)}` : ""}`);
    this.activeTurnContext?.settleToolByName(
      "exit_plan_mode",
      decision === "approved" ? "\u8BA1\u5212\u5DF2\u6279\u51C6\uFF08approved\uFF09\uFF0C\u672C\u56DE\u5408\u5F00\u59CB\u5B9E\u65BD" : comments ? `\u8BA1\u5212\u5DF2\u62D2\u7EDD\uFF08rejected\uFF09\uFF0C\u4FEE\u6539\u610F\u89C1\uFF1A${comments}` : "\u8BA1\u5212\u5DF2\u62D2\u7EDD\uFF08rejected\uFF09\uFF0C\u653E\u5F03\u8BA1\u5212\u7EE7\u7EED\u5BF9\u8BDD"
    );
    if (decision === "rejected" && comments && this.revisionLoop < 3) {
      this.revisionLoop += 1;
      this.queue.push({
        id: `user-${randomUUID3().slice(0, 8)}`,
        role: "user",
        content: [{ type: "text", text: `\u8BA1\u5212\u4FEE\u6539\u610F\u89C1\uFF08\u81EA\u52A8\u8F6C\u8FBE\uFF09\uFF1A${comments}
\u8BF7\u6309\u4E0A\u8FF0\u610F\u89C1\u4FEE\u8BA2\u8BA1\u5212\uFF0C\u7136\u540E\u91CD\u65B0\u63D0\u4EA4\u5BA1\u6279\uFF08exit_plan_mode\uFF09\u3002` }],
        source: { kind: "user" }
      });
      this.wake();
      console.log(`[grokcli] revision loop #${this.revisionLoop}: \u610F\u89C1\u5DF2\u81EA\u52A8\u53D1\u56DE grok`);
    }
    return { outcome: decision === "approved" ? "approved" : "rejected", ...comments ? { comments } : {} };
  }
  /** view-only 拦截的降级通知（mirror 已 finalize，提示回合落不了库）：弹一张不落
   *  会话事件的说明卡引导重新打开；面板不可用则只打日志（消息本身已保证不进 grok）。 */
  async showViewOnlyNoticeCard(text) {
    const svc = this.getUserQuestionsSvc();
    if (!svc) {
      console.log(`[grokcli] view-only notice (no panel): ${text}`);
      return;
    }
    try {
      await svc.ask({
        questions: [{ id: "view-only-notice", question: text, options: [{ id: "ok", label: "\u77E5\u9053\u4E86" }] }],
        agent: this
      });
    } catch (e) {
      console.log(`[grokcli] view-only notice panel failed: ${String(e).slice(0, 120)}`);
    }
  }
  /** 修订闭环的意见入口：拒绝计划后弹一张纯文本问答卡（无选项，custom 即意见；
   *  跳过/留空/面板不可用 = 无意见，直接 rejected）。 */
  async askRevisionComments() {
    const svc = this.getUserQuestionsSvc();
    if (!svc) return void 0;
    try {
      const ans = await svc.ask({
        questions: [{ id: "revision-comments", question: "\u5BF9\u8FD9\u4EFD\u8BA1\u5212\u7684\u4FEE\u6539\u610F\u89C1\uFF1F\uFF08\u7559\u7A7A\u6216\u8DF3\u8FC7 = \u76F4\u63A5\u653E\u5F03\u8BA1\u5212\uFF09" }],
        agent: this
      });
      const a = ans?.answers?.find((x) => x.id === "revision-comments");
      const text = (a?.custom ?? "").trim();
      return text || void 0;
    } catch (e) {
      console.log(`[grokcli] revision comments panel failed: ${String(e).slice(0, 120)}`);
      return void 0;
    }
  }
  /** `_x.ai/ask_user_question`：questions 在请求参数里，弹原生问答面板；
   *  accepted+answers→grok 同回合拿到选择继续干活。 */
  async protocolAskUser(params) {
    this.protocolInteractive = true;
    if (this.askTimer) {
      clearTimeout(this.askTimer);
      this.askTimer = null;
    }
    if (this.planExitTimer) {
      clearTimeout(this.planExitTimer);
      this.planExitTimer = null;
    }
    const items = parseAskItems(JSON.stringify({ questions: params.questions ?? [] }));
    const svc = this.getUserQuestionsSvc();
    if (!svc || items.length === 0) {
      console.log("[grokcli] ask_user_question: \u65E0\u9762\u677F\u670D\u52A1\u6216\u95EE\u9898\u89E3\u6790\u5931\u8D25 -> cancelled");
      return { outcome: "cancelled" };
    }
    try {
      const ans = await svc.ask({ questions: items, agent: this });
      const answers = {};
      for (const it of items) {
        const a = ans?.answers?.find((x) => x.id === it.id);
        const picked = [...a?.selected ?? [], ...a?.custom ? [a.custom] : []];
        if (picked.length > 0) answers[it.question] = picked.length > 1 ? picked : picked[0];
      }
      console.log(`[grokcli] ask_user_question protocol answer: ${JSON.stringify(answers).slice(0, 120)}`);
      this.activeTurnContext?.settleToolByName("ask_user_question", `\u7528\u6237\u5DF2\u4F5C\u7B54\uFF1A${JSON.stringify(answers)}`);
      return { outcome: "accepted", answers };
    } catch (e) {
      console.log(`[grokcli] ask_user_question panel failed: ${String(e).slice(0, 120)} -> cancelled`);
      this.activeTurnContext?.settleToolByName("ask_user_question", "\u95EE\u9898\u9762\u677F\u5DF2\u53D6\u6D88\uFF08cancelled\uFF09");
      return { outcome: "cancelled" };
    }
  }
  /** userQuestions 服务获取（Cordis 铁律：未声明 inject 的服务 get 会抛；双路 try/catch） */
  getUserQuestionsSvc() {
    try {
      return this.loopCtx.userQuestions;
    } catch {
    }
    try {
      return this.loopCtx.get?.("userQuestions");
    } catch {
      return void 0;
    }
  }
  /** approval 服务获取（Cordis 铁律：未 inject 的服务属性/get 都会抛——实测炸过宿主，
   *  必须整体包 try/catch；userQuestions 同款处理见 getUserQuestionsSvc） */
  getApprovalSvc() {
    try {
      return this.loopCtx.approval;
    } catch {
    }
    try {
      return this.loopCtx.get?.("approval");
    } catch {
      return void 0;
    }
  }
  /** 弹 dsh 原生**计划审阅卡**审计划（2026-10-06 问题1修复：审批面板 reason 只能塞
   *  260 字预览、长计划被截成省略号看不完——换 userQuestions 的 plan-review intent：
   *  PlanReviewPanel 渲染 detail=计划全文 markdown，提交的文档可在侧边栏打开；
   *  intent.approve 指名批准选项，其余选项=拒绝。UI 不认识 intent 时回退普通选项卡，
   *  应答编码相同（intent 只改展示不改协议）。 */
  async requestPlanApproval() {
    const svc = this.getUserQuestionsSvc();
    if (!svc) return "rejected";
    try {
      const plan = this.planText ?? "\uFF08\u8BA1\u5212\u5185\u5BB9\u672A\u968F\u8BF7\u6C42\u5E26\u4E0A\uFF09";
      const APPROVE = "\u6279\u51C6\uFF0C\u6309\u8BA1\u5212\u5B9E\u65BD";
      const ans = await svc.ask({
        questions: [{
          id: "plan-review",
          question: "grok \u5DF2\u5236\u5B9A\u8BA1\u5212\uFF0C\u8BF7\u5BA1\u9605",
          detail: plan,
          options: [{ label: APPROVE }, { label: "\u62D2\u7EDD" }],
          intent: { kind: "plan-review", approve: APPROVE }
        }],
        agent: this
      });
      const a = ans?.answers?.find((x) => x.id === "plan-review");
      const ok = !!a?.selected?.includes(APPROVE);
      console.log(`[grokcli] plan review card: ${ok ? "approved" : "rejected"} (plan ${plan.length}B \u5168\u6587)`);
      return ok ? "approved" : "rejected";
    } catch (e) {
      console.log(`[grokcli] plan review card failed: ${String(e).slice(0, 120)} -> rejected`);
      return "rejected";
    }
  }
  async onAcpPermission(req) {
    const approval = this.getApprovalSvc();
    if (!approval) {
      const reject = req.options.find((o) => /^reject/i.test(o.kind) || /^reject/i.test(o.optionId));
      return reject ? { outcome: "selected", optionId: reject.optionId } : { outcome: "rejected" };
    }
    try {
      const outcome = await approval.request({
        agent: this,
        toolName: req.title || "grok tool",
        callId: req.toolCallId,
        reason: typeof req.rawInput === "string" ? req.rawInput.slice(0, 200) : JSON.stringify(req.rawInput ?? {}).slice(0, 200)
      });
      if (outcome === "allowed-once") {
        const allow = req.options.find((o) => o.kind === "allow_once" || o.kind === "allow_always");
        if (allow) return { outcome: "selected", optionId: allow.optionId };
      }
      const reject = req.options.find((o) => /^reject/i.test(o.kind) || /^reject/i.test(o.optionId));
      if (reject) return { outcome: "selected", optionId: reject.optionId };
      return { outcome: "rejected" };
    } catch (e) {
      this.loopCtx.logger.warn(`[grokcli] approval failed: ${String(e)}`);
      return { outcome: "rejected" };
    }
  }
  /** 接收 model/selection 会话事件（UI 会话内切模型/档位） */
  onSessionEvent(event) {
    if (event.type === "model/selection") {
      const data = event.data;
      if (looksLikeGrokModel(data.model) || this.modelSource.isKnownModel(data.model ?? "")) this.nextModel = data.model;
      if (data.reasoningEffort) this.nextEffort = data.reasoningEffort;
      else if (data.model && data.model !== this.nextModel) this.nextEffort = null;
    }
  }
  async dispose() {
    this.disposed = true;
    this.cancel({ kind: "disposed" });
    await this.whenIdle().catch(() => {
    });
    this.driver.dispose();
    for (const m of this.subagentMirrors.values()) void m.finalize();
    this.subagentMirrors.clear();
    await this.scopeDispose?.().catch(() => {
    });
  }
};
var GROKDESK_HOME = join3(homedir3(), ".grokdesk");
var BINDING_MAP = join3(GROKDESK_HOME, "grok-session-map.json");
function readBindings() {
  try {
    return JSON.parse(readFileSync2(BINDING_MAP, "utf-8"));
  } catch {
    return {};
  }
}
function writeBindings(map) {
  try {
    writeFileSync3(BINDING_MAP, JSON.stringify(map, null, 2), "utf-8");
  } catch (e) {
    console.log(`[grokcli] binding map write failed: ${String(e).slice(0, 120)}`);
  }
}
var GrokBridgeFactory = class {
  constructor(ctx, config) {
    this.ctx = ctx;
    this.config = config;
  }
  ctx;
  config;
  settingsProfiles = [];
  refreshing = null;
  pinProxy = null;
  /** 重读「设置 → 模型 → 自定义模型 API」（llm-pi-ai 段）并解析密钥；settings/document-updated 触发 */
  async refreshProfiles() {
    this.refreshing ??= (async () => {
      try {
        const list = await readPiAiProfiles(this.ctx);
        if (list.length) {
          this.settingsProfiles = list;
          console.log(`[grokcli] settings profiles: ${list.map((e) => `${e.id}(${e.models.length}m${e.apiKey ? "" : ",NO-KEY"})`).join(", ")}`);
          setTimeout(() => {
            void decorateEfforts(this.ctx, list).catch((e) => console.log(`[grokcli] decorateEfforts error: ${String(e).slice(0, 120)}`));
          }, 300);
        }
      } catch (e) {
        console.log(`[grokcli] settings profile read failed: ${String(e).slice(0, 150)}`);
      } finally {
        this.refreshing = null;
      }
    })();
    return this.refreshing;
  }
  async currentProfiles() {
    if (!this.settingsProfiles.length) await this.refreshProfiles();
    return this.settingsProfiles.length ? this.settingsProfiles : this.config.profiles ?? [];
  }
  modelSource() {
    return {
      profiles: () => this.currentProfiles(),
      isKnownModel: (m) => this.settingsProfiles.some((e) => e.models.includes(m)),
      pinTarget: async (model, upstreamBase, effort) => {
        if (this.config.pinModel === false) return null;
        if (!this.pinProxy) this.pinProxy = await ModelPinningProxy.start();
        this.pinProxy.setTarget({ upstreamBase, model, ...effort ? { effort } : {} });
        return this.pinProxy.baseUrl;
      },
      saveBinding: (dshSessionId, grokSessionId, spawnKey, model) => {
        const map = readBindings();
        map[dshSessionId] = { grokSessionId, ...spawnKey ? { spawnKey } : {}, ...model ? { model } : {} };
        writeBindings(map);
      },
      usageSince: (ts) => this.pinProxy ? usageSince(ts) : null
    };
  }
  /** 边车查绑定（resume 用） */
  lookupBinding(dshSessionId) {
    return readBindings()[dshSessionId] ?? null;
  }
  async createAgent(ownerCtx, options) {
    const origin = options.meta?.origin;
    if (origin === "subagent" || options.parentAgent !== void 0) {
      const msg = `[grokcli-bridge] \u672C\u6865\u4E0D\u627F\u8F7D dsh \u5B50\u4EE3\u7406\u4F1A\u8BDD\uFF08origin=${origin ?? "none"}${options.parentAgent !== void 0 ? "\uFF0C\u5E26\u7236\u4EE3\u7406" : ""}\uFF09\u3002\u5B50\u4EE3\u7406\u8BF7\u8D70\u5DF2\u63D2\u5165\u7684 dsh-subagent-acp\uFF08out-of-process\uFF0C\u4E0D\u7ECF\u8FC7\u672C\u6865\uFF09\uFF1B\u5982\u9700 in-process \u5B50\u4EE3\u7406\u5F15\u64CE\uFF0C\u8BF7\u6062\u590D agent-loop \u7C7B\u5F15\u64CE\u4E3A\u672C factory\u3002`;
      console.log(`[grokcli] reject subagent createAgent: session=${options.sessionId} origin=${origin ?? "none"}`);
      throw new Error(msg);
    }
    try {
      return await this.createAgentInner(ownerCtx, options);
    } catch (e) {
      dumpError("createAgent", e);
      throw e;
    }
  }
  async createAgentInner(ownerCtx, options) {
    console.log(`[grokcli] createAgent session=${options.sessionId}`);
    const session = this.ctx.sessions.prepare(options.sessionId, {
      ...options.seed !== void 0 ? { seed: options.seed } : {},
      ...options.meta !== void 0 ? { meta: options.meta } : {},
      ...options.inheritedEventCount !== void 0 ? { inheritedEventCount: options.inheritedEventCount } : {}
    });
    return await this.publish(ownerCtx, session, options.agentOptions ?? {}, options.setup, options.signal, "startup", options.parentAgent);
  }
  async resume(ownerCtx, options) {
    let seed = [];
    let meta;
    let keepHandle;
    let viewOnlyMirror = false;
    let mirrorSession;
    console.log(`[grokcli] resume session=${options.resumeSessionId}`);
    const mirrorLive = SubagentMirror.live(String(options.resumeSessionId));
    try {
      const persistence = this.persistence();
      if (persistence && !mirrorLive) {
        const handle2 = await persistence.open(options.resumeSessionId, "write");
        const cold = await handle2.read(0, void 0);
        seed = [...cold.events];
        meta = { ...handle2.header };
        keepHandle = handle2;
        console.log(`[grokcli] resume cold-read ok events=${seed.length}`);
        if (meta?.origin === "subagent") {
          console.log(`[grokcli] reject subagent resume: session=${options.resumeSessionId}`);
          await handle2.close().catch(() => {
          });
          keepHandle = void 0;
          throw new Error("[grokcli-bridge] \u62D2\u7EDD\u6062\u590D\u5B50\u4EE3\u7406\u4F1A\u8BDD\uFF08header.origin=subagent\uFF09\uFF1A\u5B50\u4EE3\u7406\u8BF7\u8D70 subagent-acp \u901A\u9053\u3002");
        }
      } else if (!persistence) {
        console.log(`[grokcli] resume: no sessionPersistence service`);
      }
      if (mirrorLive) {
        if (!mirrorLive.session) throw new Error("[grokcli-bridge] mirror-live \u4F1A\u8BDD\u7F3A session \u5B9E\u4F8B\uFF0C\u62D2\u7EDD\u53EA\u8BFB\u6D4F\u89C8\u3002");
        console.log(`[grokcli] resume mirror-live session -> view-only browse (\u590D\u7528\u955C\u50CF Session \u5355\u672C\u8D26\uFF0C\u4E0D\u5360\u5199\u53E5\u67C4)`);
        viewOnlyMirror = true;
        mirrorSession = mirrorLive.session;
      }
    } catch (e) {
      keepHandle = void 0;
      this.ctx.logger.warn(`[grokcli] resume cold-read failed, starting empty: ${String(e)}`);
      dumpError("resume-cold-read", e);
    }
    const restoreBinding = viewOnlyMirror ? null : this.lookupBinding(options.resumeSessionId);
    console.log(`[grokcli] resume binding: ${restoreBinding ? restoreBinding.grokSessionId.slice(0, 8) : "(none)"}`);
    const session = mirrorSession ?? this.ctx.sessions.prepare(options.resumeSessionId, {
      seed,
      ...meta !== void 0 ? { meta } : {}
    });
    return await this.publish(ownerCtx, session, options.agentOptions ?? {}, options.setup, options.signal, "resume", options.parentAgent, keepHandle, seed.length, restoreBinding, viewOnlyMirror);
  }
  persistence() {
    return this.ctx.get("sessionPersistence");
  }
  async publish(ownerCtx, session, agentOptions, setup, signal, source, parentAgent, resumedHandle, seedCount = 0, restoreBinding = null, viewOnlyMirror = false) {
    const agent = new GrokBridgeAgent(this.ctx, session.id, agentOptions, session, this.config, this.modelSource(), restoreBinding);
    agent.viewOnlyMirror = viewOnlyMirror;
    await agent.bindRuntimeHelpers();
    if (setup && !viewOnlyMirror) {
      const commit = await setup(agent.ctx, agent);
      if (commit && typeof commit.commit === "function") commit.commit();
    }
    let storedHandle = resumedHandle;
    if (source === "startup" && !storedHandle) {
      try {
        const persistence = this.persistence();
        storedHandle = await persistence?.create(session.header);
      } catch (e) {
        storedHandle = void 0;
        this.ctx.logger.warn(`[grokcli] persistence create failed: ${String(e)}`);
        dumpError("persistence-create", e);
      }
    }
    if (storedHandle) {
      const storedCount = source === "resume" ? seedCount : 0;
      const suffix = session.snapshotEvents(storedCount);
      if (suffix.length > 0) await storedHandle.append(suffix);
      console.log(`[grokcli] unstored suffix flushed: ${suffix.length} events`);
    }
    const unfollowModel = this.ctx.on("session/event", (s, event) => {
      if (s.id === agent.id) agent.onSessionEvent(event);
    });
    let detachSession;
    if (!viewOnlyMirror) {
      detachSession = agent.ctx.sessions.enter(session);
      agent.ctx.sessions.announce(session);
    }
    const detachAgent = this.ctx.agents.enter(agent, parentAgent);
    await this.ctx.agents.announce(agent, source, signal);
    return {
      agent,
      dispose: async () => {
        unfollowModel?.();
        await agent.dispose();
        detachAgent();
        detachSession?.();
        await storedHandle?.close?.().catch?.(() => {
        });
      }
    };
  }
};
var OPEN = "<think>";
var CLOSE = "</think>";
var HOLD = 8;
function makeThinkSplitter() {
  let pending = "";
  let inThink = false;
  let sawAnyTag = false;
  return {
    feed(chunk) {
      pending += chunk;
      const out = [];
      for (; ; ) {
        if (!inThink) {
          if (!sawAnyTag && pending.length <= OPEN.length && OPEN.startsWith(pending)) {
            if (!pending.startsWith("<")) {
              out.push({ kind: "text", text: pending });
              pending = "";
            }
            break;
          }
          const openAt = pending.indexOf(OPEN);
          if (openAt === 0) {
            inThink = true;
            sawAnyTag = true;
            pending = pending.slice(OPEN.length);
            continue;
          }
          if (openAt > 0) {
            out.push({ kind: "text", text: pending.slice(0, openAt) });
            pending = pending.slice(openAt);
            continue;
          }
          const hold = findPartialSuffix(pending, OPEN);
          out.push({ kind: "text", text: pending.slice(0, pending.length - hold) });
          pending = pending.slice(pending.length - hold);
          break;
        } else {
          const closeAt = pending.indexOf(CLOSE);
          if (closeAt >= 0) {
            out.push({ kind: "thought", text: pending.slice(0, closeAt) });
            pending = pending.slice(closeAt + CLOSE.length);
            inThink = false;
            continue;
          }
          const hold = findPartialSuffix(pending, CLOSE);
          out.push({ kind: "thought", text: pending.slice(0, pending.length - hold) });
          pending = pending.slice(pending.length - hold);
          break;
        }
      }
      return out.filter((p) => p.text);
    },
    /** 流结束后冲刷残余（未闭合的 think 整段按 thought 计） */
    flush() {
      const rest = pending;
      pending = "";
      if (!rest) return [];
      return [{ kind: inThink ? "thought" : "text", text: rest }];
    },
    get inThink() {
      return inThink;
    }
  };
}
function findPartialSuffix(s, tag) {
  for (let k = Math.min(HOLD, s.length); k > 0; k--) {
    const tail = s.slice(s.length - k);
    if (tag.startsWith(tail) && tail.length < tag.length) return k;
  }
  return 0;
}
var ERR_LOG = join3(GROKDESK_HOME, "bridge-errors.log");
function dumpError(where, e) {
  try {
    appendFileSync2(ERR_LOG, `
[${(/* @__PURE__ */ new Date()).toISOString()}] ${where}
${e instanceof Error ? e.stack : String(e)}
`);
  } catch {
  }
}
function parseAskItems(raw) {
  try {
    const j = JSON.parse(raw);
    const src = Array.isArray(j.questions) ? j.questions : j.question !== void 0 ? [{ question: j.question, options: j.options }] : [];
    return src.map((qq, i) => {
      const q = qq;
      const options = Array.isArray(q.options) ? q.options.map((o) => {
        if (typeof o === "string") return { label: o };
        const oo = o;
        const label = String(oo.label ?? oo.text ?? "");
        return label ? { label, ...oo.description ? { description: String(oo.description) } : {} } : null;
      }).filter((o) => o !== null) : void 0;
      return { id: `q${i + 1}`, question: String(q.question ?? q.prompt ?? "").slice(0, 500), ...options && options.length ? { options } : {} };
    }).filter((q) => q.question);
  } catch {
    return [];
  }
}
function extractAskQuestion(raw) {
  if (!raw) return "\uFF08\u672A\u6355\u83B7\u5230\u95EE\u9898\u5185\u5BB9\uFF09";
  const renderOptions = (options) => {
    if (!Array.isArray(options) || options.length === 0) return "";
    const lines = options.map((o, i) => {
      if (typeof o === "string") return `${String.fromCharCode(65 + i)}. ${o}`;
      const oo = o;
      const label = String(oo.label ?? oo.text ?? JSON.stringify(o));
      return oo.description ? `${String.fromCharCode(65 + i)}. ${label} \u2014\u2014 ${String(oo.description)}` : `${String.fromCharCode(65 + i)}. ${label}`;
    });
    return `
\u9009\u9879\uFF1A
${lines.join("\n")}`;
  };
  try {
    const j = JSON.parse(raw);
    if (Array.isArray(j.questions)) {
      const qs = j.questions.map((q, i) => `**\u95EE\u9898${j.questions.length > 1 ? i + 1 : ""}**\uFF1A${String(q.question ?? "")}${renderOptions(q.options)}`);
      if (qs.length) return qs.join("\n\n");
    }
    if (typeof j.question === "string" || typeof j.prompt === "string") {
      return `${String(j.question ?? j.prompt)}${renderOptions(j.options)}`;
    }
  } catch {
  }
  return raw.slice(0, 2e3);
}
function blocksToText(content) {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content.filter((b) => b.type === "text").map((b) => b.text || "").join("");
}
var MAX_TOOL_RESULT_CHARS = 64e3;
function acpToolContentToText(content) {
  if (!Array.isArray(content)) return "";
  const parts = [];
  for (const item of content) {
    if (!item || typeof item !== "object") continue;
    const kind = String(item.type ?? "");
    try {
      if (kind === "content") {
        const c = item.content;
        const ctype = String(c?.type ?? "");
        if (ctype === "text" && typeof c?.text === "string") parts.push(c.text);
        else if (ctype === "image") parts.push(`[image ${String(c?.mimeType ?? "")}]`);
        else if (ctype === "audio") parts.push("[audio]");
        else if (ctype === "resource_link") parts.push(`
[resource_link name=${String(c?.name ?? "")} uri=${String(c?.uri ?? "")}]
`);
        else if (ctype === "resource") parts.push(`
[resource uri=${String(c?.uri ?? "")}]
`);
        else parts.push(JSON.stringify(c ?? item));
      } else if (kind === "diff") {
        const path = String(item.path ?? "");
        const oldText = typeof item.oldText === "string" ? item.oldText : "";
        const newText = typeof item.newText === "string" ? item.newText : "";
        parts.push(`diff ${path}
${oldText.split("\n").map((l) => `-${l}`).join("\n")}
${newText.split("\n").map((l) => `+${l}`).join("\n")}`);
      } else if (kind === "terminal") {
        parts.push(JSON.stringify(item));
      } else {
        parts.push(JSON.stringify(item));
      }
    } catch {
    }
  }
  const text = parts.join("\n").trim();
  if (text.length <= MAX_TOOL_RESULT_CHARS) return text;
  return `${text.slice(0, MAX_TOOL_RESULT_CHARS)}
\u2026\uFF08\u622A\u65AD\uFF1A\u539F\u6587 ${text.length} \u5B57\u7B26\uFF09`;
}
function stopReasonToEndReason(stop, failure) {
  if (failure && !/cancel/i.test(failure)) {
    return { kind: "error", error: { message: failure, code: "grok_bridge_error" } };
  }
  switch (stop) {
    case "end_turn":
      return { kind: "completed" };
    case "max_tokens":
      return { kind: "max-tokens" };
    case "cancelled":
      return { kind: "interrupted" };
    case "refusal":
      return { kind: "completed" };
    default:
      return { kind: "completed" };
  }
}

// src/index.ts
var name = "grokcli-bridge";
var inject = ["agents", "sessions", "sessionProjections", "credentials"];
var turnBoundaryProjection = {
  key: "turnBoundary",
  stateVersion: 2,
  init: () => ({
    openTurnStartSeq: null,
    lastStepStartSeq: null,
    lastStepBoundary: null,
    lastTurn: 0
  }),
  apply: (state, event) => {
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
  }
};
function apply(ctx, config) {
  const conf = {
    grokBin: config?.grokBin || (process.platform === "win32" ? join4(homedir4(), ".grok", "bin", "grok.exe") : join4(homedir4(), ".grok", "bin", "grok")),
    realHome: config?.realHome || homedir4(),
    cwd: config?.cwd || process.cwd(),
    defaultModel: config?.defaultModel,
    reasoningEffort: config?.reasoningEffort,
    profiles: config?.profiles,
    pinModel: config?.pinModel,
    retryAbortMs: config?.retryAbortMs,
    promptIdleMs: config?.promptIdleMs,
    promptHardMs: config?.promptHardMs
  };
  console.log(`[grokcli-bridge] loaded (grok=${conf.grokBin}, model=${conf.defaultModel || "(default profile)"}, effort=${conf.reasoningEffort || "(default)"})`);
  const projections = ctx.sessionProjections;
  projections?.register(turnBoundaryProjection);
  const factory = new GrokBridgeFactory(ctx, conf);
  ctx.effect(() => ctx.agents.setFactory(factory), "grokcli.setFactory()");
  void factory.refreshProfiles();
  ctx.on("settings/document-updated", ((ns) => {
    if (ns === "llm-pi-ai") void factory.refreshProfiles();
  }));
  ctx.inject(["webServer"], (webCtx) => {
    webCtx.effect(() => webCtx.webServer.register({
      kind: "exact",
      path: "/grokdesk/engine-control",
      handler: async (req, res) => {
        let body = "";
        for await (const chunk of req) body += String(chunk);
        let sessionId = "";
        try {
          sessionId = String((JSON.parse(body) || {}).sessionId ?? "");
        } catch {
        }
        res.setHeader("content-type", "application/json; charset=utf-8");
        if (!/^session-[0-9a-f-]{30,}$/i.test(sessionId)) {
          res.statusCode = 400;
          res.end(JSON.stringify({ ok: false, error: "bad sessionId" }));
          return;
        }
        try {
          const agent = webCtx.agents.get(sessionId);
          if (!agent?.shutdownEngine) {
            res.end(JSON.stringify({ ok: false, error: "\u4F1A\u8BDD\u65E0\u6D3B\u8DC3\u5F15\u64CE\uFF08\u53EF\u80FD\u672A\u53D1\u8FC7\u6D88\u606F\uFF09" }));
            return;
          }
          agent.shutdownEngine();
          res.end(JSON.stringify({ ok: true }));
        } catch (e) {
          res.end(JSON.stringify({ ok: false, error: String(e).slice(0, 120) }));
        }
      }
    }), "grokcli.engine-control route");
    webCtx.effect(() => webCtx.webServer.register({
      kind: "exact",
      path: "/grokdesk/delete-session",
      handler: async (req, res) => {
        let body = "";
        for await (const chunk of req) body += String(chunk);
        let sessionId = "";
        try {
          sessionId = String((JSON.parse(body) || {}).sessionId ?? "");
        } catch {
        }
        res.setHeader("content-type", "application/json; charset=utf-8");
        if (!/^session-[0-9a-f-]{30,}$/i.test(sessionId)) {
          res.statusCode = 400;
          res.end(JSON.stringify({ ok: false, error: "bad sessionId" }));
          return;
        }
        res.end(JSON.stringify(await deleteSessionCompletely(ctx, sessionId)));
      }
    }), "grokcli.delete-session route");
  });
  const commandsSvc = (() => {
    try {
      return ctx.get("commands");
    } catch {
      return void 0;
    }
  })();
  if (commandsSvc?.register) {
    const versionCache = /* @__PURE__ */ new Map();
    const grokVersion = (bin) => {
      const hit = versionCache.get(bin);
      if (hit) return hit;
      try {
        const out = execFileSync2(bin, ["--version"], { encoding: "utf8", timeout: 1e4 }).trim();
        versionCache.set(bin, out);
        return out;
      } catch (e) {
        return `\u7248\u672C\u83B7\u53D6\u5931\u8D25: ${String(e).slice(0, 60)}`;
      }
    };
    ctx.effect(() => commandsSvc.register({
      name: "grokstatus",
      description: "grok \u5F15\u64CE\u72B6\u6001\uFF1A\u7248\u672C/\u8DEF\u5F84/\u5F53\u524D\u4F1A\u8BDD\u7ED1\u5B9A/\u5B50\u4EE3\u7406\u955C\u50CF/pin \u914D\u7F6E",
      handler: (inv) => {
        const lines = [grokVersion(conf.grokBin), `grokBin: ${conf.grokBin}`];
        const profileNames = Object.keys(conf.profiles ?? {});
        lines.push(profileNames.length ? `\u6863\u6848\u901A\u9053: ${profileNames.join(", ")}` : "\u6863\u6848\u901A\u9053: \uFF08\u65E0\uFF0C\u8D70\u7528\u6237\u9ED8\u8BA4\u6863\u6848\uFF09");
        const a = inv.agent;
        if (a && typeof a === "object" && "acpSessionId" in a) {
          lines.push(a.acpSessionId ? `\u5F53\u524D grok \u4F1A\u8BDD: ${String(a.acpSessionId).slice(0, 8)}` : "\u5F53\u524D grok \u4F1A\u8BDD: \uFF08\u672A\u5EFA\u7ACB\uFF09");
          lines.push(`\u6A21\u578B: ${a.boundWantModel || conf.defaultModel || "(\u9ED8\u8BA4)"}`);
          lines.push(`\u5B50\u4EE3\u7406\u955C\u50CF: ${a.subagentMirrors?.size ?? 0} \u4E2A\u8FD0\u884C\u4E2D`);
        }
        lines.push(`pin \u6A21\u578B\u8F6C\u53D1: ${conf.pinModel === false ? "\u5173\u95ED" : "\u5F00\u542F"}`);
        return { kind: "success", text: lines.join("\n") };
      }
    }), "grokcli.commands()");
    ctx.effect(() => commandsSvc.register({
      name: "grokclean",
      description: "\u4F1A\u8BDD\u6E05\u7406\uFF1A\u65E0\u53C2=\u7EDF\u8BA1\uFF08\u603B\u6570/\u5F52\u6863\u6570\uFF09\uFF1Barchived=\u5220\u9664\u5168\u90E8\u5DF2\u5F52\u6863\u4F1A\u8BDD\uFF08\u78C1\u76D8+projcache+\u6CE8\u518C\u8868\u2014\u2014dsh \u539F\u751F\u53EA\u6709\u5F52\u6863\u6CA1\u6709\u5220\u9664\uFF0C\u672C\u547D\u4EE4\u8865\u4F4D\uFF09",
      input: { hint: "archived = \u5220\u9664\u5168\u90E8\u5DF2\u5F52\u6863\u4F1A\u8BDD" },
      handler: async (inv) => {
        const arg = (inv.rawInput ?? "").trim();
        let registry;
        try {
          registry = ctx.get("workspaceRegistry");
        } catch {
          registry = void 0;
        }
        const archived = registry?.archivedSessionIds ?? [];
        const sessionsRoot = join4(homedir4(), ".dsh", "sessions");
        const projcacheDir = join4(homedir4(), ".dsh", "storages", "session_projcache", "sessions");
        const all = [];
        try {
          for (const ws of readdirSync2(sessionsRoot)) {
            for (const d of readdirSync2(`${sessionsRoot}/${ws}`)) if (d.startsWith("session-")) all.push(d);
          }
        } catch {
        }
        if (arg !== "archived") {
          return { kind: "success", text: [`\u4F1A\u8BDD\u603B\u6570: ${all.length}`, `\u5DF2\u5F52\u6863: ${archived.length}${archived.length ? "\n  " + archived.map((id) => id.slice(0, 18)).join("\n  ") : ""}`, "", "\u5220\u9664\u5168\u90E8\u5F52\u6863\u4F1A\u8BDD\uFF1A/grokclean archived"].join("\n") };
        }
        const deleted = [];
        for (const id of archived) {
          const uuid = id.replace(/^session-/, "");
          let removedDir = false;
          try {
            for (const ws of readdirSync2(sessionsRoot)) {
              const dir = `${sessionsRoot}/${ws}/${id}`;
              if (existsSync3(dir)) {
                rmSync(dir, { recursive: true, force: true });
                removedDir = true;
              }
            }
          } catch {
          }
          try {
            rmSync(`${projcacheDir}/${uuid}.json`, { force: true });
          } catch {
          }
          try {
            rmSync(`${projcacheDir}/${id}.json`, { force: true });
          } catch {
          }
          try {
            await registry?.unarchiveSession(id);
          } catch (e) {
            console.log(`[grokcli] grokclean unarchive ${id.slice(0, 14)} failed: ${String(e).slice(0, 140)}`);
          }
          deleted.push(`${id.slice(0, 18)}${removedDir ? "" : "\uFF08\u76EE\u5F55\u5DF2\u4E0D\u5728\uFF0C\u4EC5\u6E05\u6CE8\u518C\u8868\uFF09"}`);
        }
        return { kind: "success", text: [`\u5DF2\u5220\u9664 ${deleted.length} \u4E2A\u5F52\u6863\u4F1A\u8BDD\uFF1A`, ...deleted.map((d) => `  ${d}`), "", "\uFF08workspace \u6210\u5458\u8D26\u76EE\u968F\u4E0B\u6B21\u53D8\u66F4\u81EA\u52A8\u526A\u9664\u6B8B\u9879\uFF09"].join("\n") };
      }
    }), "grokcli.commands.grokclean()");
    console.log("[grokcli-bridge] command registered: /grokstatus, /grokclean");
  }
}
async function deleteSessionCompletely(ctx, sessionId) {
  const sessionsRoot = join4(homedir4(), ".dsh", "sessions");
  const projcacheDir = join4(homedir4(), ".dsh", "storages", "session_projcache", "sessions");
  let registry;
  try {
    registry = ctx.get("workspaceRegistry");
  } catch {
    registry = void 0;
  }
  const notes = [];
  try {
    await registry?.archiveSession?.(sessionId);
  } catch (e) {
    notes.push(`archive: ${String(e).slice(0, 90)}`);
  }
  let removedDir = false;
  try {
    for (const ws of readdirSync2(sessionsRoot)) {
      const dir = join4(sessionsRoot, ws, sessionId);
      if (existsSync3(dir)) {
        rmSync(dir, { recursive: true, force: true });
        removedDir = true;
      }
    }
  } catch {
  }
  for (const fn of [`${sessionId}.json`, `${sessionId.replace(/^session-/, "")}.json`]) {
    try {
      rmSync(join4(projcacheDir, fn), { force: true });
    } catch {
    }
  }
  try {
    const wsFile = join4(homedir4(), ".dsh", "storages", "workspace.json");
    const data = JSON.parse(readFileSync3(wsFile, "utf-8"));
    for (const w of Object.values(data.tables?.workspaces ?? {})) {
      if (Array.isArray(w?.sessionIds) && w.sessionIds.includes(sessionId) && w.path) {
        const entity = await registry?.resolveByPath?.(w.path);
        await entity?.detachSession?.(sessionId);
        console.log(`[grokcli] session detached from workspace ${String(w.path).slice(-30)}`);
      }
    }
  } catch (e) {
    notes.push(`detach: ${String(e).slice(0, 90)}`);
  }
  console.log(`[grokcli] session deleted: ${sessionId.slice(0, 18)} dir=${removedDir}${notes.length ? ` notes=${notes.join("; ")}` : ""}`);
  return { ok: true, removedDir, notes };
}
export {
  AcpDriver,
  ModelPinningProxy,
  apply,
  inject,
  name,
  usageSince
};
