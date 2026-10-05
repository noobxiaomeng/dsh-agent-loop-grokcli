/**
 * 设置桥：读 dsh「设置 → 模型 → 自定义模型 API」表单（llm-pi-ai 的 providers 设置段），
 * 翻译成 grok 的模型档案清单（GrokProfileEntry[]）。
 *
 * 机制（全部源码核实）：
 * - Models 页把用户填的 baseURL/协议/模型写进 profile patch 的 `- id: llm-pi-ai` 行
 *   config.providers（settings 文档 = profile patch，SettingsForms.documentPath）；
 * - apiKey 不落 YAML，只存引用名（apiKeyEnv），真值在 credentials 服务
 *   （$DSH_HOME/.credentials.yaml，Models 页写入）；运行期 ctx.get('credentials').resolve(ref)
 *   按调用解析、不得缓存（credentials/src/index.ts:183 契约）；
 * - 变更通知：settings/document-updated 全局事件（ns=插件 entry id，即 'llm-pi-ai'）；
 * - 读取入口：ctx.get('configEditor').configuration() → 找 llm-pi-ai 行的 options.config.providers
 *   （override 层优先——那是用户 patch 层）。
 */
import type { Context } from "@deepseek-ai/cordis";
import type { GrokProfileEntry } from "./profile-router.ts";

const BACKEND_MAP: Record<string, string> = {
  "openai-completions": "chat",
  "openai-responses": "responses",
  "anthropic-messages": "anthropic",
};

/** 与 Models 页同款派生规则（ui-settings-models/store.ts deriveKeyRef）：`<ROUTE>_API_KEY` */
function deriveKeyRef(provider: string): string {
  return `${provider.toUpperCase().replace(/[^A-Z0-9]+/g, "_")}_API_KEY`;
}

interface PiAiProviderRaw {
  displayName?: string;
  apiKeyEnv?: string;
  api?: string;
  baseURL?: string;
  base_url?: string;
  models?: Array<{ id?: string } | string>;
}

/** 与 dsh 档位词表对齐的值（THINKING_LEVELS：off/minimal/low/medium/high/xhigh/max） */
const LEVEL_VOCAB = new Set(["off", "minimal", "low", "medium", "high", "xhigh", "max"]);

/**
 * effort 装饰：查中转 /models 目录，把带 reasoningEfforts 元数据的模型装饰进
 * llm-pi-ai providers 的 models 条目（只补 reasoningEfforts 字段，不改清单成员）。
 * 这样 dsh 模型选择器才会为这些模型亮出档位菜单（否则 UI 层根本选不了）。
 * 注意：modelOverrides 不能与手写 models 共存（pi-ai schema 会拒），故直接装饰条目。
 */
export async function decorateEfforts(ctx: Context, entries: GrokProfileEntry[]): Promise<void> {
  const get = (ctx as unknown as { get?: (name: string) => unknown }).get?.bind(ctx);
  const editor = get?.("configEditor") as
    | { entries(): Array<{ options: { id?: string } }>; edit(entry: unknown, change: (current: Record<string, unknown>, inherited: Record<string, unknown>) => Record<string, unknown>): Promise<void> }
    | undefined;
  if (!editor?.entries) return;
  const entry = editor.entries().find(r => r.options?.id === "llm-pi-ai");
  if (!entry) return;

  for (const e of entries) {
    if (!e.baseUrl || !e.apiKey) continue;
    // 1) 拉中转目录
    let effortsByModel: Map<string, string[]> | null = null;
    try {
      const res = await fetch(`${e.baseUrl.replace(/\/+$/, "")}/models`, {
        headers: { authorization: `Bearer ${e.apiKey}` },
        signal: AbortSignal.timeout(15000),
      });
      if (res.ok) {
        const data = (await res.json()) as { data?: Array<{ id?: string; supportsReasoningEffort?: boolean; reasoningEfforts?: Array<{ value?: string; id?: string }> }> };
        effortsByModel = new Map();
        for (const m of data.data || []) {
          if (!m?.id || !m.supportsReasoningEffort) continue;
          const levels = (m.reasoningEfforts || []).map(l => l.value || l.id).filter((v): v is string => !!v && LEVEL_VOCAB.has(v));
          if (levels.length) effortsByModel.set(m.id, levels);
        }
      }
    } catch { /* 目录拉不到就跳过装饰 */ }
    if (!effortsByModel || effortsByModel.size === 0) continue;

    // 2) 差分写回（仅补缺失/不同的 reasoningEfforts，绝不动清单成员与其他字段）
    let changed = false;
    try {
      await editor.edit(entry, current => {
        const providers = (current.providers ?? {}) as Record<string, { models?: Array<{ id?: string; reasoningEfforts?: Record<string, string> }> }>;
        const provider = providers[e.id];
        if (!provider?.models) return current;
        for (const model of provider.models) {
          if (!model?.id) continue;
          const levels = effortsByModel!.get(model.id);
          if (!levels) continue;
          const desired: Record<string, string> = {};
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

/** 读 llm-pi-ai 设置段的 providers → grok 档案清单（含密钥解析）。失败返回 []（走 overlay 手写档案兜底）。 */
export async function readPiAiProfiles(ctx: Context): Promise<GrokProfileEntry[]> {
  const get = (ctx as unknown as { get?: (name: string) => unknown }).get?.bind(ctx);
  const editor = get?.("configEditor") as
    | { configuration(): Array<{ entry?: { options?: { id?: string; config?: unknown } }; override?: Record<string, unknown> }> }
    | undefined;
  if (!editor?.configuration) return [];
  let providers: unknown;
  try {
    const row = editor.configuration().find(r => r.entry?.options?.id === "llm-pi-ai");
    providers = (row?.override as { providers?: unknown } | undefined)?.providers
      ?? (row?.entry?.options?.config as { providers?: unknown } | undefined)?.providers;
  } catch {
    return [];
  }
  if (!providers || typeof providers !== "object") return [];

  const credentials = get?.("credentials") as
    | { resolve(ref: string): Promise<{ value?: string } | undefined> }
    | undefined;
  const out: GrokProfileEntry[] = [];
  for (const [key, raw] of Object.entries(providers as Record<string, PiAiProviderRaw>)) {
    if (!raw || typeof raw !== "object") continue;
    const baseUrl = raw.baseURL || raw.base_url;
    if (!baseUrl) continue;
    let apiKey = "";
    // 表单留空密钥时 profile 不带 apiKeyEnv；回退到页面的派生名（同名凭据已存在时可直接解析）
    const ref = raw.apiKeyEnv || deriveKeyRef(key);
    if (credentials) {
      try {
        const hit = await credentials.resolve(ref);
        apiKey = hit?.value ?? "";
        if (!apiKey) console.log(`[grokcli] credential ${ref}: no value (在设置→模型→编辑里填一次 API 密钥即可)`);
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
      models: (raw.models || [])
        .map(m => (typeof m === "string" ? m : m?.id))
        .filter((m): m is string => Boolean(m)),
    });
  }
  return out;
}
