/**
 * grok 模型档案路由器（dsh 插件用）
 * 移植自 grok-app-server.cjs 的 readProviderForm/syncGrokProfiles。
 *
 * 事实依据（实测）：
 * - grok 认证/模型接入走 ~/.grok/config.toml 的 [model.<id>] 档案（api_backend/api_key/
 *   base_url/model/name）；无 auth.json 也能跑；
 * - 通道陷阱：中转对部分模型只开 chat-completions；裸模型走默认档案 responses 通道对
 *   grok-4.6 会 60s 挂死（无限 retry_state）。所以有档案时一切模型走档案通道：
 *   spawn -m <档案id>，当前所选模型动态写进档案 model 字段；
 * - 只管理 grokdesk- 前缀的段，绝不碰用户手写档案（如 apikey-fun）——主权归用户。
 */
import { existsSync, readFileSync, writeFileSync } from "node:fs";

export interface GrokProfileEntry {
  /** 档案 id（写入 config.toml 时带 grokdesk- 前缀） */
  id: string;
  name: string;
  baseUrl: string;
  apiKey: string;
  backend: "chat" | "responses" | "anthropic" | string;
  models: string[];
}

export interface RouteDecision {
  /** spawn -m 参数：档案 id（走档案通道）或裸模型名（走默认档案） */
  spawnKey: string;
  /** 写进档案 model 字段的目标模型 */
  model: string | null;
}

function configTomlPath(realHome: string) {
  return realHome + "/.grok/config.toml";
}

/**
 * 把插件配置的档案清单同步进真实 ~/.grok/config.toml。
 * - 剔除旧的 [model.grokdesk-*] 段（我们管理的），保留其余原样；
 * - modelOverride：当前要用的模型，写进第一个可用档案的 model 字段；
 * - baseUrlOverride：写入档案 base_url 的覆盖值（单模型钉死转发器的本地地址），
 *   真实上游由转发器内部持有。
 */
export function syncGrokProfiles(realHome: string, entries: GrokProfileEntry[], modelOverride?: string | null, baseUrlOverride?: string): void {
  if (!entries.length) return;
  const path = configTomlPath(realHome);
  try {
    const raw = existsSync(path) ? readFileSync(path, "utf-8") : "";
    const lines = raw.split("\n");
    const kept: string[] = [];
    let inManaged = false;
    for (const line of lines) {
      const h = line.match(/^\s*\[([^\]]+)\]\s*$/);
      if (h) inManaged = /^model\.grokdesk-/.test(h[1]);
      if (!inManaged) kept.push(line);
    }
    let out = kept.join("\n").replace(/\n{3,}$/, "\n");
    for (const e of entries) {
      out += `\n[model.grokdesk-${e.id}]\n`;
      out += `api_backend = ${JSON.stringify(e.backend)}\n`;
      out += `api_key = ${JSON.stringify(e.apiKey)}\n`;
      out += `base_url = ${JSON.stringify(baseUrlOverride || e.baseUrl)}\n`;
      const profModel = modelOverride || (e.models.length ? e.models[0] : null);
      if (profModel) out += `model = ${JSON.stringify(profModel)}\n`;
      out += `name = ${JSON.stringify(String(e.name))}\n`;
    }
    writeFileSync(path, out, "utf-8");
  } catch (e) {
    // 同步失败不致命：grok 还能用手写档案/默认档案跑
    console.warn("[grokcli] config.toml sync failed:", e);
  }
}

/**
 * 路由决策（2026-10-05 实锤版 + 设置桥版）：优先选「拥有当前所选模型」的档案；
 * 否则第一个完整档案（baseUrl+apiKey）；无档案退回裸模型（默认档案/responses，
 * 可能挂重试——有档案就别落到这条）。
 */
export function decideRoute(entries: GrokProfileEntry[], wantModel: string | null | undefined): RouteDecision {
  const want = wantModel || null;
  const usable = entries.filter(e => e.baseUrl && e.apiKey);
  const owner = want ? usable.find(e => e.models.includes(want)) : undefined;
  const profile = owner ?? usable[0] ?? null;
  if (profile) {
    return { spawnKey: `grokdesk-${profile.id}`, model: want };
  }
  return { spawnKey: want || "", model: want };
}

/** 读取 ~/.grok/config.toml 里默认档案指定的模型（展示用） */
export function defaultModelOf(realHome: string): string | null {
  try {
    const raw = readFileSync(configTomlPath(realHome), "utf-8");
    const m = raw.match(/^\s*default\s*=\s*"([^"]+)"/m);
    if (m) {
      // default 指向档案 id，再取该档案的 model
      const prof = raw.match(new RegExp(`^\\[model\\.${m[1].replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\]\\s*$([\\s\\S]*?)(?=^\\[|\\z)`, "m"));
      if (prof) {
        const mm = prof[1].match(/^\s*model\s*=\s*"([^"]+)"/m);
        if (mm) return mm[1];
      }
      return m[1];
    }
  } catch {}
  return null;
}
