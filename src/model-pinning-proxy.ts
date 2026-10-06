/**
 * 「单模型钉死」本地转发器：grok 的辅助请求（会话起题 session_title、摘要、建议等）
 * 用的是它内建的 utility model（实测 grok-4.6），无配置键可改——中转后台会看到
 * 混合模型流量。本转发器作为 drop-in base_url：出网前把任何 model ≠ 当前所选模型
 * 的请求体改写为所选模型，其余原样转发（含 SSE 流式与 /v1/models 目录）。
 */
import { createServer, request as httpRequest } from "node:http";
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
        // 行级校验转发（2026-10-06）：中转大负载下会夹带含裸控制字符的坏 data 行（grok
        // 严格解析炸 serialization error）——逐行 JSON 校验，坏行丢弃，好行原样转发；usage
        // 从 response.completed 事件采集。
        let buf = "";
        upRes.on("data", (d: Buffer) => {
          buf += d.toString("utf8");
          let idx: number;
          const out: string[] = [];
          while ((idx = buf.indexOf("\n")) >= 0) {
            const rawLine = buf.slice(0, idx);
            buf = buf.slice(idx + 1);
            const line = rawLine.trim();
            if (!line.startsWith("data:")) { out.push(rawLine + "\n"); continue; }
            const payload = line.slice(5).trim();
            if (!payload || payload === "[DONE]") { out.push(rawLine + "\n"); continue; }
            try {
              const ev = JSON.parse(payload) as { response?: { usage?: unknown }; usage?: unknown };
              if (ev.response?.usage) recordUsage(ev.response.usage);
              out.push(rawLine + "\n");
            } catch {
              console.log(`[grokcli] pin: 丢弃坏 data 行 responses (${payload.length}B)`);
            }
          }
          if (out.length > 0) res.write(out.join(""));
        });
        upRes.on("end", () => { if (buf.trim()) res.write(buf); res.end(); });
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
              // 坏行防御（2026-10-06 实测踩坑：中转大负载下会夹带含裸控制字符  -
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
