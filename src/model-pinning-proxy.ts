/**
 * 「单模型钉死」本地转发器：grok 的辅助请求（会话起题 session_title、摘要、建议等）
 * 用的是它内建的 utility model（实测 grok-4.6），无配置键可改——中转后台会看到
 * 混合模型流量。本转发器作为 drop-in base_url：出网前把任何 model ≠ 当前所选模型
 * 的请求体改写为所选模型，其余原样转发（含 SSE 流式与 /v1/models 目录）。
 */
import { createServer, request as httpRequest } from "node:http";
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
  const o = u as OpenAiUsage;
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
          if (target.effort && parsed.reasoning_effort === undefined && parsed.reasoning === undefined) {
            parsed.reasoning_effort = target.effort;
            notes.push(`reasoning_effort=${target.effort} injected`);
          }
          // usage 采集（2026-10-05）：OpenAI 兼容体只在显式要求时才在流式终包带 usage；
          // 非流式响应默认带，无需注入。若上游拒认此参数会 4xx——冒烟/回归即暴露。
          if (parsed.stream === true && parsed.stream_options?.include_usage !== true) {
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
    // 目标 URL：把真实 base 的路径拼回（base 含 /v1；进来的 path 也带 /v1）
    const m = /^(https?):\/\/([^/]+)(\/.*)?$/.exec(target.upstreamBase.replace(/\/+$/, ""));
    if (!m) {
      res.writeHead(502, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "pinning proxy: bad upstream base" }));
      return;
    }
    const [, scheme, hostport, basePath = ""] = m;
    const path = req.url && req.url.startsWith("/v1") ? basePath + req.url.slice(3) : basePath + (req.url || "");
    const send = scheme === "https" ? httpsRequest : httpRequest;
    const headers = { ...req.headers, host: hostport, "content-length": String(body.length) };
    const up = send({ hostname: hostport.split(":")[0], port: Number(hostport.split(":")[1] ?? (scheme === "https" ? 443 : 80)), path, method: req.method, headers }, upRes => {
      const outHeaders: Record<string, string | number> = {};
      for (const [k, v] of Object.entries(upRes.headers)) {
        if (v == null || k === "transfer-encoding" || k === "connection" || k === "keep-alive") continue;
        outHeaders[k] = Array.isArray(v) ? v.join(", ") : v;
      }
      res.writeHead(upRes.statusCode ?? 502, outHeaders);
      // usage 采集 tee：SSE 逐 data: 行解析终包 usage；JSON 响应整体缓冲后解析。
      // 边转发边扫，不落盘不改流（对 grok 侧完全透明）。
      const isSSE = String(upRes.headers["content-type"] ?? "").includes("text/event-stream");
      if (isSSE) {
        let sseBuf = "";
        upRes.on("data", (d: Buffer) => {
          res.write(d);
          sseBuf += d.toString("utf8");
          let idx: number;
          while ((idx = sseBuf.indexOf("\n")) >= 0) {
            const line = sseBuf.slice(0, idx).trim();
            sseBuf = sseBuf.slice(idx + 1);
            if (!line.startsWith("data:")) continue;
            const payload = line.slice(5).trim();
            if (!payload || payload === "[DONE]") continue;
            try { recordUsage((JSON.parse(payload) as { usage?: unknown }).usage); } catch { /* 半包/非 JSON 忽略 */ }
          }
        });
        upRes.on("end", () => res.end());
        upRes.on("error", () => res.end());
      } else {
        const acc: Buffer[] = [];
        upRes.on("data", (d: Buffer) => { acc.push(d); res.write(d); });
        upRes.on("end", () => {
          try { recordUsage((JSON.parse(Buffer.concat(acc).toString("utf8")) as { usage?: unknown }).usage); } catch { /* 非 JSON 忽略 */ }
          res.end();
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
