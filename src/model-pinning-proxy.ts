/**
 * 「单模型钉死」本地转发器：grok 的辅助请求（会话起题 session_title、摘要、建议等）
 * 用的是它内建的 utility model（实测 grok-4.6），无配置键可改——中转后台会看到
 * 混合模型流量。本转发器作为 drop-in base_url：出网前把任何 model ≠ 当前所选模型
 * 的请求体改写为所选模型，其余原样转发（含 SSE 流式与 /v1/models 目录）。
 */
import { createServer, request as httpRequest } from "node:http";
import { appendFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { request as httpsRequest } from "node:https";

export interface PinTarget {
  /** 真实上游 base（含 /v1，如 https://xcmapi.org/v1） */
  upstreamBase: string;
  /** 期望的唯一出网模型 id */
  model: string;
  /** 补注入的思考档位（grok 侧目录不认识新模型时会静默丢档，这里出网前补上） */
  effort?: string;
}

export class ModelPinningProxy {
  private server: ReturnType<typeof createServer>;
  readonly port: number;
  /** 给 grok 当 base_url 用（与真实 base 同构，含 /v1） */
  readonly baseUrl: string;

  private constructor(port: number, server: ReturnType<typeof createServer>) {
    this.port = port;
    this.server = server;
    this.baseUrl = `http://127.0.0.1:${port}/v1`;
  }

  setTarget(target: PinTarget | null): void {
    // 请求处理器读模块级 pinState（单例状态），必须写这里而非实例字段
    pinState.target = target;
  }

  static start(): Promise<ModelPinningProxy> {
    return new Promise((resolve, reject) => {
      const server = createServer((req, res) => handle(req, res, () => pinState.target));
      server.on("error", reject);
      server.listen(0, "127.0.0.1", () => {
        const addr = server.address();
        if (addr == null || typeof addr === "string") {
          reject(new Error("pinning proxy failed to bind"));
          return;
        }
        resolve(new ModelPinningProxy(addr.port, server));
      });
    });
  }

  close(): void {
    try { this.server.close(); } catch {}
  }
}

// 模块级单例状态（一个 dsh 进程一个转发器；并发多 provider 时以最后 set 的目标为准——spike 级取舍）
const pinState: { target: PinTarget | null } = { target: null };

// ── usage 采集（2026-10-05 第二步③）────────────────────────────────────────
// 环形日志（进程单例）：桥在回合收口按时间窗取走求和。grok 的辅助请求（起题/摘要）
// 也计入——它们同样烧钱，用户在面板看到的应是整回合真实开销。
interface OpenAiUsage { prompt_tokens?: number; completion_tokens?: number; total_tokens?: number; prompt_tokens_details?: { cached_tokens?: number } }
const usageLog: Array<{ ts: number; usage: OpenAiUsage }> = [];
type UsageListener = (usage: { inputTokens: number; outputTokens: number; cacheReadTokens: number; cacheWriteTokens: number }) => void;
const usageListeners = new Set<UsageListener>();
/** 注册逐次 usage 回调（桥用于回合中实时推送 assistant/attempt 流帧）。返回注销器。 */
export function onPinUsage(cb: UsageListener): () => void {
  usageListeners.add(cb);
  return () => { usageListeners.delete(cb); };
}

function recordUsage(u: unknown): void {
  if (!u || typeof u !== "object") return;
  const o = u as OpenAiUsage & { input_tokens?: number; output_tokens?: number; input_tokens_details?: { cached_tokens?: number } };
  // responses 协议形状（input_tokens/output_tokens）归一成 chat 形状再入库
  if (o.prompt_tokens === undefined && o.completion_tokens === undefined
    && (o.input_tokens !== undefined || o.output_tokens !== undefined)) {
    (o as Record<string, unknown>).prompt_tokens = o.input_tokens;
    (o as Record<string, unknown>).completion_tokens = o.output_tokens;
    (o as Record<string, unknown>).total_tokens = o.total_tokens ?? (o.input_tokens ?? 0) + (o.output_tokens ?? 0);
    (o as Record<string, unknown>).prompt_tokens_details = { cached_tokens: o.input_tokens_details?.cached_tokens ?? 0 };
  }
  if (o.prompt_tokens === undefined && o.completion_tokens === undefined) return;
  usageLog.push({
    ts: Date.now(),
    usage: {
      prompt_tokens: Number(o.prompt_tokens ?? 0) || 0,
      completion_tokens: Number(o.completion_tokens ?? 0) || 0,
      total_tokens: Number(o.total_tokens ?? 0) || 0,
      prompt_tokens_details: { cached_tokens: Number(o.prompt_tokens_details?.cached_tokens ?? 0) || 0 },
    },
  });
  if (usageLog.length > 200) usageLog.splice(0, usageLog.length - 200);
  console.log(`[grokcli] pin-usage captured: +${o.prompt_tokens}in/+${o.completion_tokens}out`);
  // 逐次通知（实时用量推送）：四桶 = 未缓存输入（prompt 已减缓存）/输出/缓存读/缓存写 0
  const cached = Number(o.prompt_tokens_details?.cached_tokens ?? 0) || 0;
  const snapshot = {
    inputTokens: Math.max(0, (Number(o.prompt_tokens ?? 0) || 0) - cached),
    outputTokens: Number(o.completion_tokens ?? 0) || 0,
    cacheReadTokens: cached,
    cacheWriteTokens: 0,
  };
  for (const cb of [...usageListeners]) {
    try { cb(snapshot); } catch { /* 监听器异常不影响采集 */ }
  }
}

/** since(ms) 之后所有响应的 usage 求和 → dsh TokenUsage 口径
 *  （inputTokens=未缓存输入：OpenAI 的 prompt_tokens 已含缓存 token，须减） */
export function usageSince(sinceMs: number): { inputTokens: number; outputTokens: number; cacheReadTokens: number; cacheWriteTokens: number; totalTokens: number } | null {
  const rows = usageLog.filter(r => r.ts >= sinceMs);
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
    totalTokens: total || prompt + completion,
  };
}

/** 递归补全嵌套 error 对象缺失的 code/message（2026-10-08 变体：上游故障时中转把
 *  502 包成 response.failed 类事件，response.error 只有 message/type 没有 code，grok
 *  的 serde 反序列化照炸 missing field `code`。顶层 type:"error" 事件的补全管不到
 *  嵌套层，这里对任意层级的 "error" 键对象做同样补全）。返回是否发生改写。 */
function completeNestedErrors(v: unknown): boolean {
  let changed = false;
  const walk = (o: Record<string, unknown>): void => {
    for (const [k, val] of Object.entries(o)) {
      if (k === "error" && val !== null && typeof val === "object" && !Array.isArray(val)) {
        const e = val as Record<string, unknown>;
        if (e.code === undefined) { e.code = "relay_error"; changed = true; }
        if (e.message === undefined) { e.message = "relay error without message"; changed = true; }
        walk(e); // error 对象内部再嵌套也补
      } else if (Array.isArray(val)) {
        for (const item of val) {
          if (item !== null && typeof item === "object" && !Array.isArray(item)) walk(item as Record<string, unknown>);
        }
      } else if (val !== null && typeof val === "object") {
        walk(val as Record<string, unknown>);
      }
    }
  };
  if (v !== null && typeof v === "object" && !Array.isArray(v)) walk(v as Record<string, unknown>);
  return changed;
}

function handle(req: import("node:http").IncomingMessage, res: import("node:http").ServerResponse, getTarget: () => PinTarget | null): void {
  const chunks: Buffer[] = [];
  req.on("data", (c: Buffer) => chunks.push(c));
  req.on("end", () => {
    const target = getTarget();
    if (target == null) {
      res.writeHead(503, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "pinning proxy: no target configured" }));
      return;
    }
    let body = Buffer.concat(chunks);
    console.log(`[grokcli] pin-req ${req.method} ${req.url} ${body.length}B model=${((): string => { try { return JSON.parse(body.toString("utf-8")).model || "-"; } catch { return "-"; } })()}`);
    // 请求体改写：POST 且 JSON——① model 与期望不符 → 钉成期望模型；② 缺 reasoning_effort
    // 且配了档位 → 补注入（grok 对其内建目录不认识的模型会静默丢档，实测 grok-4.7 即如此）
    if (req.method === "POST" && body.length > 0) {
      try {
        const parsed = JSON.parse(body.toString("utf-8"));
        if (parsed && typeof parsed === "object") {
          const notes: string[] = [];
          if (typeof parsed.model === "string" && parsed.model !== target.model) {
            notes.push(`model ${parsed.model} -> ${target.model}`);
            parsed.model = target.model;
          }
          const isResponsesApi = typeof req.url === "string" && req.url.includes("/responses");
          if (!isResponsesApi && target.effort && parsed.reasoning_effort === undefined && parsed.reasoning === undefined) {
            parsed.reasoning_effort = target.effort;
            notes.push(`reasoning_effort=${target.effort} injected`);
          }
          // usage 采集（2026-10-05）：OpenAI 兼容体只在显式要求时才在流式终包带 usage；
          // 非流式响应默认带，无需注入。若上游拒认此参数会 4xx——冒烟/回归即暴露。
          // responses 请求体补 reasoning.effort（2026-10-06）：grok 的 responses 客户端只发
          // reasoning.summary（实测），spawn 旗标的档位不上请求体——服务端见不到等级。转发器
          // 保底注入（已有 effort 时不碰；summary 等既有字段保留）。
          if (isResponsesApi && target.effort) {
            const r = parsed.reasoning as { effort?: string } | undefined;
            if ((r === undefined || r.effort === undefined) && parsed.reasoning_effort === undefined) {
              parsed.reasoning = { ...(r ?? {}), effort: target.effort };
              notes.push(`reasoning.effort=${target.effort} injected`);
            }
          }
          // stream_options 是 chat 协议参数——responses API 不认（实测 500），只对 chat 注入
          if (!isResponsesApi && parsed.stream === true && parsed.stream_options?.include_usage !== true) {
            parsed.stream_options = { ...(parsed.stream_options || {}), include_usage: true };
            notes.push("stream_options.include_usage injected");
          }
          if (notes.length > 0) {
            body = Buffer.from(JSON.stringify(parsed), "utf-8");
            console.log(`[grokcli] pin (${req.url}): ${notes.join(", ")}`);
          }
        }
      } catch { /* 非 JSON 体原样转发 */ }
    }
    // 目标 URL：把真实 base 的路径拼回（base 含 /v1；进来的 path 也带 /v1）。
    // 端点型 upstreamBase（老大 2026-10-06 指正：xcmapi 的 responses 接入地址是
    // .../v1/responses 完整端点）——直接打该端点不再拼路径，否则 /responses 会叠成
    // /responses/responses。
    const m = /^(https?):\/\/([^/]+)(\/.*)?$/.exec(target.upstreamBase.replace(/\/+$/, ""));
    if (!m) {
      res.writeHead(502, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "pinning proxy: bad upstream base" }));
      return;
    }
    const [, scheme, hostport, basePath = ""] = m;
    const endpointTyped = /\/(responses|chat\/completions)$/.test(basePath);
    const path = endpointTyped
      ? basePath
      : req.url && req.url.startsWith("/v1") ? basePath + req.url.slice(3) : basePath + (req.url || "");
    const send = scheme === "https" ? httpsRequest : httpRequest;
    const headers = { ...req.headers, host: hostport, "content-length": String(body.length) };
    const up = send({ hostname: hostport.split(":")[0], port: Number(hostport.split(":")[1] ?? (scheme === "https" ? 443 : 80)), path, method: req.method, headers }, upRes => {
      const outHeaders: Record<string, string | number> = {};
      for (const [k, v] of Object.entries(upRes.headers)) {
        // content-length 一并剥离：非流式响应体可能被思维链翻译改写变长
        if (v == null || k === "transfer-encoding" || k === "connection" || k === "keep-alive" || k === "content-length") continue;
        outHeaders[k] = Array.isArray(v) ? v.join(", ") : v;
      }
      res.writeHead(upRes.statusCode ?? 502, outHeaders);
      // usage 采集 + 思维链翻译（2026-10-06）：中转 chat 通道把推理放 DeepSeek 风格
      // delta.reasoning_content，grok CLI 不认识直接丢（实测 UI 只剩 <think> 碎片 20B）。
      // 这里改写成 content 里的 <think>…</think> 流（首个 rc delta 开标签、首个正文 delta
      // 闭标签、流末兜底闭合），桥的 makeThinkSplitter 拆到推理流 → UI 完整思维链。
      const isSSE = String(upRes.headers["content-type"] ?? "").includes("text/event-stream");
      // responses 端点的响应是 responses 协议事件流（response.*），grok 原生解析——
      // 转发器只透传+采 usage（response.completed 事件带 usage），不做 chat 层的杂行
      // 丢弃/形状补全（那会把正常 responses 事件全扔掉）。
      const passthroughResponses = typeof req.url === "string" && req.url.includes("/responses");
      if (passthroughResponses) {
        // 全覆盖校验转发（2026-10-06 彻查版）：中转坏流一族（控制字符行/缺 type 行/缺 code
        // 的 error——含非流式与流尾残块两条绕过路径）。规则：① data: 行 JSON.parse 失败丢弃；
        // ② data: 行缺 string 型 type 丢弃；③ error 事件缺 code/message 补全；④ 非 data 的
        // 大块（非流式 JSON 整体或流尾残块）含 error 且缺 code 的补全重写；⑤ 其余原样转发。
        // 另：响应全量落盘 ~/.grokdesk/relay-last-responses.log（覆盖式，复现时看坏数据真身）。
        let buf = "";
        let dumped = false;
        // 思维链外显直播（2026-10-06）：grok CLI 对 responses 的 reasoning 增量事件不转发为
        // ACP thought 流（实测推理全程零 thought chunk，回合末才一次性给摘要）。这里把
        // response.reasoning_summary_text.delta 翻译成附加的 output_text.delta 正文流并包
        // <think> 标签（首个推理 delta 开标签、首个真正文 delta 闭标签），桥的 makeThinkSplitter
        // 拆到推理流 → UI 思考区实时增长。原事件保留双发（grok 侧摘要逻辑不受影响）。
        let thinkLive = false;
        const dumpPath = join(homedir(), ".grokdesk", "relay-last-responses.log");
        const dump = (s: string): void => {
          try {
            if (!dumped) { writeFileSync(dumpPath, `=== ${new Date().toISOString()} ${req.url} ===\n`); dumped = true; }
            appendFileSync(dumpPath, s);
          } catch { /* dump 失败无妨 */ }
        };
        const sanitizeBlock = (text: string): string => {
          if (!text.includes("error")) return text;
          try {
            const j = JSON.parse(text) as unknown;
            if (j && typeof j === "object") {
              // 嵌套 error 对象（含顶层 {"error":{...}} 形态）递归补全——grok serde 要求
              // error.code 必在，中转上游故障时的非流式错误体常常只有 message/type。
              if (completeNestedErrors(j)) {
                console.log(`[grokcli] pin: 补全非流式嵌套 error 字段 (${text.length}B)`);
                return JSON.stringify(j);
              }
              // 顶层 type:"error" 事件的 code/message 在事件根上（不在 error 键下），单独补
              const o = j as { type?: unknown; code?: unknown; message?: unknown };
              if (o.type === "error" && (o.code === undefined || o.message === undefined)) {
                o.code = o.code ?? "relay_error";
                o.message = o.message ?? "relay error without code";
                console.log(`[grokcli] pin: 补全非流式 error 事件字段 (${text.length}B)`);
                return JSON.stringify(j);
              }
            }
          } catch { /* 非 JSON 原样 */ }
          return text;
        };
        upRes.on("data", (d: Buffer) => {
          const chunk = d.toString("utf8");
          dump(chunk);
          buf += chunk;
          let idx: number;
          const out: string[] = [];
          while ((idx = buf.indexOf("\n")) >= 0) {
            const rawLine = buf.slice(0, idx);
            buf = buf.slice(idx + 1);
            const line = rawLine.trim();
            if (!line.startsWith("data:")) {
              out.push(rawLine + "\n");
              continue;
            }
            const payload = line.slice(5).trim();
            if (!payload || payload === "[DONE]") { out.push(rawLine + "\n"); continue; }
            try {
              const ev = JSON.parse(payload) as { type?: unknown; code?: unknown; message?: unknown; delta?: unknown; item_id?: unknown; response?: { usage?: unknown }; usage?: unknown };
              if (typeof ev.type !== "string") {
                console.log(`[grokcli] pin: 丢弃无 type 行 responses (${payload.length}B) ${payload.slice(0, 60)}`);
                continue;
              }
              // 思维链直播翻译：推理增量 → 附加 <think> 正文 delta；正文 delta 前补闭标签。
              // 首个真正文 delta 的闭标签 fake **替换**原事件转发（fake 已含其全部文本）——
              // 若再叠加原事件，grok 会把同一 delta 双转发 → 正文首 token 重复（2026-10-07
              // 实锄件：段首「先先」「任务任务」）。推理 delta 的开标签 fake 仍是双发（原
              // reasoning 事件 grok 不转发为正文、只用于其回合末摘要，保留无害）。
              const extra: string[] = [];
              let replaced = false;
              if (ev.type === "response.reasoning_summary_text.delta" && typeof ev.delta === "string" && ev.delta.length > 0) {
                const openTag = thinkLive ? "" : "<think>";
                thinkLive = true;
                const fake = { content_index: 0, type: "response.output_text.delta", delta: openTag + ev.delta, item_id: ev.item_id ?? "rs_live", output_index: 0, sequence_number: typeof (ev as { sequence_number?: unknown }).sequence_number === "number" ? (ev as { sequence_number: number }).sequence_number : 0 };
                extra.push("data: " + JSON.stringify(fake) + "\n\n");
              } else if (ev.type === "response.output_text.delta" && typeof ev.delta === "string" && thinkLive && ev.delta.length > 0) {
                thinkLive = false;
                const fake = { content_index: 0, type: "response.output_text.delta", delta: "</think>" + ev.delta, item_id: ev.item_id ?? "msg_live", output_index: 0, sequence_number: typeof (ev as { sequence_number?: unknown }).sequence_number === "number" ? (ev as { sequence_number: number }).sequence_number : 0 };
                extra.push("data: " + JSON.stringify(fake) + "\n\n");
                replaced = true;
              }
              let forward = rawLine + "\n";
              if (ev.type === "error" && (ev.code === undefined || ev.message === undefined)) {
                (ev as Record<string, unknown>).code = ev.code ?? "relay_error";
                (ev as Record<string, unknown>).message = ev.message ?? "relay returned a malformed error event";
                forward = "data: " + JSON.stringify(ev) + "\n";
                console.log(`[grokcli] pin: 补全 error 事件字段 (code=${String(ev.code)})`);
              }
              // 嵌套变体（2026-10-08）：response.failed 类事件里 response.error 缺 code——
              // 顶层补全管不到，递归补全后整行重写
              if (completeNestedErrors(ev)) {
                forward = "data: " + JSON.stringify(ev) + "\n";
                console.log(`[grokcli] pin: 补全嵌套 error 字段 (responses SSE ${payload.length}B)`);
              }
              if (ev.response?.usage) recordUsage(ev.response.usage);
              if (extra.length > 0) out.push(...extra);
              if (!replaced) out.push(forward);
            } catch {
              console.log(`[grokcli] pin: 丢弃坏 data 行 responses (${payload.length}B)`);
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
        upRes.on("data", (d: Buffer) => {
          sseBuf += d.toString("utf8");
          let idx: number;
          const out: string[] = [];
          while ((idx = sseBuf.indexOf("\n")) >= 0) {
            const rawLine = sseBuf.slice(0, idx);
            sseBuf = sseBuf.slice(idx + 1);
            const line = rawLine.trim();
            if (!line.startsWith("data:")) { out.push(rawLine + "\n"); continue; }
            const payload = line.slice(5).trim();
            if (!payload || payload === "[DONE]") { out.push(rawLine + "\n"); continue; }
            let rewritten = rawLine + "\n";
            try {
              const ev = JSON.parse(payload) as { id?: string; object?: string; created?: number; model?: string; usage?: unknown; choices?: Array<{ delta?: Record<string, unknown> }> };
              // 非 chat-chunk 形状的私货行（无 choices/usage/id——实测中转会夹 {"sequ...} 40KB
              // 杂行）透传必炸 grok 的严格反序列化：丢弃并记日志。
              if (!ev.choices && !ev.usage && !ev.id) {
                console.log(`[grokcli] pin: 丢弃非 chunk 行 (${payload.length}B) ${payload.slice(0, 60)}`);
                continue;
              }
              if (ev.usage) recordUsage(ev.usage);
              // 中转的 reasoning_content 行常是精简形状（缺 id 等必需字段）——grok 的
              // ChatCompletionChunk 严格反序列化会炸（实测 missing field `id`）。补全形状。
              const maybeRc = ev.choices?.[0]?.delta?.reasoning_content ?? ev.choices?.[0]?.delta?.reasoning;
              if (typeof maybeRc === "string") {
                if (!ev.id) ev.id = "chatcmpl-pin";
                if (!ev.object) ev.object = "chat.completion.chunk";
                if (typeof ev.created !== "number") ev.created = Math.floor(Date.now() / 1000);
                if (!ev.model) ev.model = target.model;
              }
              const delta = ev.choices?.[0]?.delta;
              // 2026-10-06 实测翻案：xAI chat 协议原生带 reasoning_content 字段，grok CLI
              // 自己处理（此前中转不吐该字段才误判需要 <think> 翻译——翻译反而破坏原生
              // 链路：grok 对 <think> 包裹的流报 no_visible_content）。此处只透传不翻译；
              // 命中 reasoning 字段的行经形状补全后重序列化（保证 id 等必需字段在）。
              if (delta && typeof delta === "object" && typeof (delta.reasoning_content ?? delta.reasoning) === "string") {
                rewritten = "data: " + JSON.stringify(ev) + "\n";
              }
            } catch {
              // 坏行防御（2026-10-06 实测踩坑：中转大负载下会夹带含裸控制字符 \u0000-\u001f
              // 的 data 行，grok 严格 JSON 解析直接炸整回合 serialization error）——解析失败
              // 的 data 行一律丢弃不透传（SSE 的 data 行按协议必须全是合法 JSON，丢的是坏行）。
              console.log(`[grokcli] pin: 丢弃坏 data 行 (${payload.length}B)`);
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
        // 非流式：整体缓冲改写（reasoning_content → content 前缀）再发——长度会变，
        // content-length 头已在 outHeaders 剥离阶段排除（chunked 自动适配）。
        const acc: Buffer[] = [];
        upRes.on("data", (d: Buffer) => { acc.push(d); });
        upRes.on("end", () => {
          let bodyBuf = Buffer.concat(acc);
          try {
            const parsed = JSON.parse(bodyBuf.toString("utf8")) as { usage?: unknown; choices?: Array<{ message?: Record<string, unknown> }> };
            if (parsed.usage) recordUsage(parsed.usage);
            const msg = parsed.choices?.[0]?.message;
            const rc = msg?.reasoning_content ?? msg?.reasoning;
            if (msg && typeof rc === "string" && rc.length > 0) {
              const prev = typeof msg.content === "string" ? msg.content : "";
              msg.content = `<think>${rc}</think>${prev}`;
              delete msg.reasoning_content;
              if (msg.reasoning !== undefined) delete msg.reasoning;
              bodyBuf = Buffer.from(JSON.stringify(parsed), "utf8");
              console.log(`[grokcli] pin (${req.url}): reasoning_content -> <think> (${rc.length}B)`);
            }
          } catch { /* 非 JSON 忽略 */ }
          res.end(bodyBuf);
        });
        upRes.on("error", () => res.end());
      }
    });
    up.on("error", e => {
      console.log(`[grokcli] pin proxy upstream error: ${String(e).slice(0, 120)}`);
      if (!res.headersSent) res.writeHead(502, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: String(e) }));
    });
    up.end(body);
  });
}
