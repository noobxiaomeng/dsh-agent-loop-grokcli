/**
 * 子代理会话镜像（2026-10-05 第二梯队 · 架构定案：grok 原生 spawn_subagent + 本镜像层）。
 *
 * 事实依据（全量抓线 reports/subagent-wire.jsonl，2026-10-05）：
 * - grok 主代理经 `spawn_subagent` 工具起子会话，事件全走父 ACP 流：
 *   `_x.ai/session_notification` 带 `subagent_spawned {subagent_id, child_session_id,
 *   description, subagent_type}`；子会话的 user_message_chunk / agent_*_chunk / tool_call(_update)
 *   / response_completed 都以**子 sessionId** 归因（params.sessionId 干净区分父子）；
 *   子回合收尾 `turn_completed`（snake usage）；父回合收尾亦然（camel usage）。
 * - 本层把这些事件实时写成 dsh 子会话（header.origin='subagent'，侧栏带标记），
 *   用户可在 UI 点开看子代理全程转写；父会话工具行 OUT=子代理汇报（bridge 原有映射）。
 */
import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import type { Context } from "@deepseek-ai/cordis";
import type { Session, SessionId } from "@deepseek-ai/dsh-session/types.ts";
import type { AssistantMessage, ToolResultMessage, ContentBlock } from "@deepseek-ai/dsh-llm/types.ts";
import { acpToolContentToText } from "./bridge.ts";

/** 子代理会话的专属分组载体（2026-10-06 问题2修复）：dsh 侧栏分组=workspace 注册索引，
 *  且 membership 要求「attach + 会话 header 的 canonical cwd == workspace path」双条件。
 *  镜像子会话的 cwd 指到本目录并 attach 进本目录的 workspace → 侧栏出现独立分组
 *  （title 默认=目录名 subagents，可在 UI 重命名）；服务不可用时静默回未分组。 */
const SUBAGENTS_DIR = join(homedir(), ".grokdesk", "subagents");

interface PersistenceHandleLike {
  append(events: readonly unknown[], o?: unknown): Promise<void>;
  close(): Promise<void>;
}
interface PersistenceLike {
  create(header: unknown, o?: unknown): Promise<PersistenceHandleLike>;
}

/** grok 两套 usage 命名（response_completed=snake / turn_completed=camel）→ dsh TokenUsage */
function grokUsageToTokenUsage(u: unknown): { inputTokens: number; outputTokens: number; cacheReadTokens: number; cacheWriteTokens: number; totalTokens: number } | undefined {
  if (!u || typeof u !== "object") return undefined;
  const o = u as Record<string, unknown>;
  const num = (v: unknown) => (typeof v === "number" && Number.isFinite(v) ? v : 0);
  const input = num(o.input_tokens ?? o.inputTokens);
  const output = num(o.output_tokens ?? o.outputTokens);
  const cacheRead = num(o.cache_read_input_tokens ?? o.cachedReadTokens);
  const cacheWrite = num(o.cache_creation_input_tokens ?? o.cacheWriteTokens ?? o.cacheCreationTokens);
  if (input === 0 && output === 0 && cacheRead === 0 && cacheWrite === 0) return undefined;
  const total = num(o.total_tokens ?? o.totalTokens) || input + output;
  return { inputTokens: Math.max(0, input - cacheRead), outputTokens: output, cacheReadTokens: cacheRead, cacheWriteTokens: cacheWrite, totalTokens: total };
}

export interface SubagentSpawnSpec {
  subagentId: string;
  childSessionId: string;
  description?: string;
  subagentType?: string;
}

export class SubagentMirror {
  /** 运行中的镜像实例（按 dsh 会话 id 索引，open 登记/finalize 撤销）：resume 分流用
   *  live() 识别「子代理仍在写」的会话（只读浏览），并拿实例走单本账 append 通道
   *  （2026-10-06 view-only 修复：第二本 seq/turn 账必然相撞，提示回合必须经 mirror 落）。 */
  private static readonly liveMirrors = new Map<string, SubagentMirror>();
  static live(dshSessionId: string): SubagentMirror | undefined {
    return SubagentMirror.liveMirrors.get(dshSessionId);
  }

  readonly subagentId: string;
  /** dsh 侧子会话 id / 创建时间（父会话 subagent/catalog 事件要引用） */
  dshSessionId: SessionId | null = null;
  dshSessionCreatedAt = 0;
  private session: Session | null = null;
  private handle: PersistenceHandleLike | null = null;
  private detach: (() => void) | null = null;
  private turn = 0;
  private turnOpen = false;
  private textBuf = "";
  private thoughtBuf = "";
  private pendingTools = new Map<string, { name: string; arguments: string; callSeq?: number }>();
  /** view-only 浏览期间用户发消息的提示回合队列：子代理回合进行中（turnOpen）不能落
   *  turn（嵌套 turn = 冷读取判损坏），排队等回合收尾后的安全窗口冲刷。 */
  private pendingNotices: Array<{ userText: string; notice: string }> = [];
  private finalized = false;

  private constructor(
    readonly spec: SubagentSpawnSpec,
    private ctx: Context,
    private parentDshSessionId: SessionId,
    private model: string,
  ) {
    this.subagentId = spec.subagentId;
  }

  static async open(
    spec: SubagentSpawnSpec,
    ctx: Context,
    parentDshSessionId: SessionId,
    cwd: string,
    model: string,
  ): Promise<SubagentMirror> {
    const m = new SubagentMirror(spec, ctx, parentDshSessionId, model);
    // 分组归属：cwd 指子代理专属目录（进侧栏 subagents 分组），失败不影响镜像本体
    let mirrorCwd = cwd;
    try {
      mkdirSync(SUBAGENTS_DIR, { recursive: true });
      mirrorCwd = SUBAGENTS_DIR;
    } catch { /* 保持父 cwd */ }
    const id = `session-${randomUUID()}` as SessionId;
    // 可见性裁决（老大 2026-10-05：要看得见）：dsh 侧栏树无条件排除 origin='subagent'
    // 会话（ui-workspace/tree.ts sessionVisible），原生入口只服务 dsh 自家 subagent 工具卡。
    // 故镜像子会话按普通会话建（不打 origin 标记，parentSession 保留归树信息），
    // 任务文本自动成为标题，直接出现在侧栏，点开即看全程转写。
    const session = ctx.sessions.prepare(id, {
      meta: { parentSession: parentDshSessionId, cwd: mirrorCwd, delegationDepth: 1 },
    });
    m.session = session;
    m.dshSessionId = id;
    m.dshSessionCreatedAt = (session.header as { createdAt?: number } | undefined)?.createdAt ?? Date.now();
    // prepare() 产物「NOT yet in the store」（core/session 源码注释原话）——必须先 enter
    // 注册成 live 会话，persistence.create 的 liveness 检查才过（2026-10-05 实测踩坑）
    m.detach = ctx.sessions.enter(session);
    // 标题先行（原生 session/title 事件）：老大会话都有 title 投影，无标题会话在列表/排序
    // 侧不可见（实测对照）；description 即子代理任务标签，正适合做会话名
    const title = spec.description || spec.subagentType || `子代理 ${spec.subagentId.slice(0, 8)}`;
    try {
      session.append("session/title", { title, messageSeqs: [], source: { kind: "user" } } as never);
    } catch (e) {
      console.log(`[grokcli] mirror title append failed: ${String(e).slice(0, 120)}`);
    }
    try {
      const persistence = (ctx as unknown as { get(name: string): unknown }).get("sessionPersistence") as PersistenceLike | undefined;
      if (persistence) {
        m.handle = await persistence.create((session as unknown as { header: unknown }).header);
        // 对齐 cursor（照抄工厂 publish 的 appendUnstoredSuffix 契约：create 前已 append 的事件刷进句柄）
        const suffix = (session as unknown as { snapshotEvents(from: number): readonly unknown[] }).snapshotEvents(0);
        if (suffix.length > 0) await m.handle.append(suffix);
      }
    } catch (e) {
      console.log(`[grokcli] mirror(${spec.subagentId.slice(0, 8)}) persistence create failed: ${String(e).slice(0, 120)}`);
    }
    ctx.sessions.announce?.(session);
    SubagentMirror.liveMirrors.set(String(id), m);
    void SubagentMirror.attachToWorkspace(ctx, id).catch(() => {});
    console.log(`[grokcli] mirror open: ${spec.description ?? spec.subagentType ?? "subagent"} -> ${id.slice(0, 18)} (grok child ${spec.childSessionId.slice(0, 8)})`);
    return m;
  }

  /** 把镜像子会话挂进 subagents 工作区（resolveByPath/create + attachSession 双条件；
   *  Cordis 铁律：服务获取与调用整体 try/catch，任何失败只降级回未分组）。 */
  private static async attachToWorkspace(ctx: Context, sessionId: SessionId): Promise<void> {
    let registry: {
      resolveByPath(p: string): Promise<unknown>;
      create(p: string): Promise<{ attachSession(id: string): Promise<void> }>;
    } | undefined;
    try {
      registry = (ctx as unknown as { get(n: string): unknown }).get("workspaceRegistry") as typeof registry;
    } catch { return; }
    if (!registry) return;
    try {
      let ws = await registry.resolveByPath(SUBAGENTS_DIR) as { attachSession(id: string): Promise<void> } | undefined;
      if (!ws) ws = await registry.create(SUBAGENTS_DIR);
      await ws.attachSession(sessionId as unknown as string);
      console.log(`[grokcli] mirror ${sessionId.slice(0, 18)} attached to subagents workspace`);
    } catch (e) {
      console.log(`[grokcli] mirror workspace attach failed (fall back 未分组): ${String(e).slice(0, 120)}`);
    }
  }

  /** 子会话收到它的任务提示（user_message_chunk）→ 开 turn 落 user/message */
  onTask(text: string): void {
    if (this.finalized || !this.session) return;
    this.flushPendingNotices(); // 单 turn 计数器下，先落排队的提示再开任务 turn（防撞号）
    this.turn += 1;
    this.turnOpen = true;
    this.session.append("turn/start", { turn: this.turn });
    // MessageBase 契约：id + content + source（kind 必带——读取侧折叠要读 source.kind，
    // 缺了整条历史回放炸 "Cannot read properties of undefined (reading 'kind')"，实测踩坑）
    this.session.append("user/message", {
      id: `user-${randomUUID().slice(0, 8)}` as never,
      role: "user",
      content: [{ type: "text", text }],
      source: { kind: "user" },
    } as never, { surfaceOp: "append" });
    this.session.append("step/start", { turn: this.turn, step: 1 });
  }

  onChunk(text: string, thought: boolean): void {
    if (this.finalized) return;
    if (thought) this.thoughtBuf += text; else this.textBuf += text;
  }

  onToolCall(callId: string, title: string, rawInput: string): void {
    if (this.finalized || !this.session) return;
    const name = title || "tool";
    const args = typeof rawInput === "string" ? rawInput : JSON.stringify(rawInput ?? {});
    // v4 契约同父会话：先以 assistant/message 的 tool-call 块预告，再写 tool/call（血泪档案
    // 2026-10-05：无预告=冷读取判损坏）
    const advMessage: AssistantMessage = {
      id: `asst-${randomUUID().slice(0, 8)}` as never,
      role: "assistant",
      content: [{ type: "tool-call", id: callId as never, name, arguments: args } as ContentBlock],
      source: { kind: "model", provider: "grok", model: this.model },
    };
    this.session.append("assistant/message", { turn: this.turn, step: 1, message: advMessage, stream: [] } as never, { surfaceOp: "append" });
    const ev = this.session.append("tool/call", {
      turn: this.turn, step: 1, callId: callId as never, name, arguments: args,
    }) as unknown as { seq?: number };
    this.pendingTools.set(callId, { name, arguments: args, callSeq: ev?.seq });
  }

  onToolUpdate(callId: string, status?: string, title?: string, content?: unknown, locations?: unknown): void {
    if (this.finalized || !this.session) return;
    const rec = this.pendingTools.get(callId);
    if (!rec || (status !== "completed" && status !== "failed")) return;
    this.pendingTools.delete(callId);
    const failed = status === "failed";
    const text = acpToolContentToText(content) || (failed ? "tool failed" : "(no content)");
    const message: ToolResultMessage = {
      id: `tool-${randomUUID().slice(0, 8)}` as never,
      role: "tool",
      content: [{ type: "text", text } as ContentBlock],
      source: { kind: "tool", callId: callId as never },
      toolCallId: callId as never,
      ...(failed ? { isError: true } : {}),
    };
    this.session.append("tool/result", {
      turn: this.turn, step: 1, message,
      ...(failed ? { error: { name: "GrokToolError", code: "GROK_TOOL_FAILED", ...(title ? { reason: title } : {}) } } : {}),
      ...(Array.isArray(locations) && locations.length > 0 ? { meta: { locations } } : {}),
    } as never, {
      surfaceOp: "append",
      ...(rec.callSeq !== undefined ? { sourceEventSeqs: [rec.callSeq] as never } : {}),
    });
  }

  /** 子会话一次响应收尾（response_completed）→ 落 assistant/message（带该次 usage） */
  onResponseCompleted(usage: unknown): void {
    if (this.finalized || !this.session) return;
    if (!this.textBuf && !this.thoughtBuf) return;
    const content: ContentBlock[] = [];
    if (this.thoughtBuf) content.push({ type: "reasoning", text: this.thoughtBuf } as ContentBlock);
    content.push({ type: "text", text: this.textBuf || "(empty)" } as ContentBlock);
    const message: AssistantMessage = {
      id: `asst-${randomUUID().slice(0, 8)}` as never,
      role: "assistant",
      content,
      source: { kind: "model", provider: "grok", model: this.model },
    };
    const u = grokUsageToTokenUsage(usage);
    this.session.append("assistant/message", {
      turn: this.turn, step: 1, message, stream: [], ...(u ? { usage: u } : {}),
    } as never, { surfaceOp: "append" });
    this.textBuf = "";
    this.thoughtBuf = "";
  }

  /** 子回合收尾（turn_completed）→ 闭合 turn（严格生命周期，用量面板依赖完整边界） */
  onTurnCompleted(stopReason: string | undefined, usage: unknown): void {
    if (this.finalized || !this.session || !this.turnOpen) return;
    this.onResponseCompleted(usage); // 保险冲刷（正常已在 response_completed 落过）
    // 未闭合工具补失败结果（turn 结束前必须闭合）
    for (const [callId] of this.pendingTools) {
      const message: ToolResultMessage = {
        id: `tool-${randomUUID().slice(0, 8)}` as never,
        role: "tool",
        content: [{ type: "text", text: "turn ended before tool completed" } as ContentBlock],
        source: { kind: "tool", callId: callId as never },
        toolCallId: callId as never,
        isError: true,
      };
      this.session.append("tool/result", { turn: this.turn, step: 1, message } as never, { surfaceOp: "append" });
    }
    this.pendingTools.clear();
    this.session.append("step/end", { turn: this.turn, step: 1 });
    const stop = stopReason || "end_turn";
    this.session.append("turn/end", {
      turn: this.turn,
      reason: stop === "end_turn" ? { kind: "completed" } : stop === "cancelled" ? { kind: "interrupted" } : { kind: "completed" },
    } as never);
    this.turnOpen = false;
    this.flushPendingNotices(); // 回合已闭合：浏览期间排队的提示现在落（安全窗口）
    console.log(`[grokcli] mirror(${this.subagentId.slice(0, 8)}) turn ${this.turn} closed (${stop})`);
  }

  /** view-only 浏览时用户发消息 → 落一个提示回合（单本账：turn/seq 全由 mirror 分配，
   *  落库走 mirror 的 active writer → follow 通道实时上屏；回放也看得见）。
   *  子代理回合进行中则排队（嵌套 turn = 损坏）。finalized 后返回 false（调用方降级）。 */
  appendOperatorNotice(userText: string, notice: string): boolean {
    if (this.finalized || !this.session) return false;
    this.pendingNotices.push({ userText, notice });
    this.flushPendingNotices();
    return true;
  }

  private flushPendingNotices(): void {
    if (this.finalized || !this.session) return;
    while (!this.turnOpen && this.pendingNotices.length > 0) {
      const { userText, notice } = this.pendingNotices.shift()!;
      this.turn += 1;
      this.session.append("turn/start", { turn: this.turn });
      this.session.append("user/message", {
        id: `user-${randomUUID().slice(0, 8)}` as never,
        role: "user",
        content: [{ type: "text", text: userText }],
        source: { kind: "user" },
      } as never, { surfaceOp: "append" });
      this.session.append("step/start", { turn: this.turn, step: 1 });
      this.session.append("assistant/message", {
        turn: this.turn, step: 1,
        message: {
          id: `asst-${randomUUID().slice(0, 8)}` as never,
          role: "assistant",
          content: [{ type: "text", text: notice } as ContentBlock],
          source: { kind: "model", provider: "grok", model: this.model },
        },
        stream: [],
      } as never, { surfaceOp: "append" });
      this.session.append("step/end", { turn: this.turn, step: 1 });
      this.session.append("turn/end", { turn: this.turn, reason: { kind: "completed" } } as never);
      console.log(`[grokcli] mirror(${this.subagentId.slice(0, 8)}) operator notice turn ${this.turn} appended (view-only 拦截)`);
    }
  }

  /** 父代理 dispose 时收尾：冲刷 + 退出 store + 关写句柄（continuable 子代理可再开新 turn 续写） */
  async finalize(): Promise<void> {
    if (this.finalized) return;
    try { this.flushPendingNotices(); } catch { /* 关闭前尽力冲刷 */ }
    this.finalized = true;
    if (this.dshSessionId !== null) SubagentMirror.liveMirrors.delete(String(this.dshSessionId));
    try { if (this.turnOpen) this.onTurnCompleted("end_turn", undefined); } catch {}
    try { await this.handle?.close(); } catch {}
    this.handle = null;
    try { this.detach?.(); } catch {}
    this.detach = null;
  }
}
