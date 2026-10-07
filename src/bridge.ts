/**
 * GrokCLI→dsh 桥驱动核心：AgentFactory + Agent 实现。
 *
 * 契约依据（file:line 见 reports/dsh-spike-*.md 侦察速查表）：
 * - factory 单槽（agent/src/index.ts:358 setFactory 互斥）→ overlay 里 disable agent-loop；
 * - Agent 必需面：id(=sessionId)/session/ctx/options/status/inbox + followup/steer/inject/
 *   send/cancel/whenIdle/runMaintenance（runtime-types.ts:163-242）；
 * - UI 发消息入口 = agent.followup/steer（session-controller/commands.ts:364）；
 * - 事件提交 = session.append(type, data, surfaceIntent?)（session/src/index.ts:718）；
 * - 流式帧 = agent/assistant-stream start→chunk*→end(committed)（dispatch.ts agentEvents）；
 * - 审批 = ctx.approval.request（user-approval/src/index.ts:215，须在 open turn 内）。
 *
 * 运行期依赖策略：静态 import 只用 node 内建；dsh 运行期助手（dsh-scope 的 createScope、
 * dsh-agent 的 agentEvents）经动态 import 解析（repo 内跑 dsh 时 PluginPackages 拦截层
 * 对任意 importer 生效）；类型全部 import type（tsx 擦除，运行期零解析）。
 */
import { randomUUID } from "node:crypto";
import { appendFileSync, existsSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import type { Context } from "@deepseek-ai/cordis";
import type { Agent, AgentHandle, AgentFactory, AgentOptions, AgentStatus, Inbox, InboxTarget } from "@deepseek-ai/dsh-agent/types.ts";
import type { Session, SessionEvent, SessionId } from "@deepseek-ai/dsh-session/types.ts";
import type { UserMessage, AssistantMessage, ToolResultMessage, ContentBlock } from "@deepseek-ai/dsh-llm/types.ts";
import { AcpDriver, type AcpSessionUpdate, type AcpPermissionRequest, type AcpPermissionDecision } from "./acp-driver.ts";
import { decideRoute, syncGrokProfiles, type GrokProfileEntry } from "./profile-router.ts";
import { onPinUsage } from "./model-pinning-proxy.ts";
import { readPiAiProfiles, decorateEfforts } from "./settings-bridge.ts";
import { ModelPinningProxy, usageSince } from "./model-pinning-proxy.ts";
import { SubagentMirror, type SubagentSpawnSpec } from "./subagent-mirror.ts";

/** 模型来源：settings 档案优先，overlay 手写 profiles 兜底 */
export interface ModelSource {
  profiles(): Promise<GrokProfileEntry[]>;
  isKnownModel(model: string): boolean;
  /** 单模型钉死：登记目标（所选模型 + 真实上游 + 档位），返回给 grok 用的本地 base_url；null=关闭 */
  pinTarget(model: string, upstreamBase: string, effort?: string): Promise<string | null>;
  /** grok 会话绑定落盘（边车 map） */
  saveBinding(dshSessionId: string, grokSessionId: string, spawnKey: string, model?: string): void;
  /** 本回合窗口内的 token 用量汇总（pin 代理请求层采集；null=未启用/无数据） */
  usageSince?(sinceMs: number): { inputTokens: number; outputTokens: number; cacheReadTokens: number; cacheWriteTokens: number; totalTokens: number } | null;
}

// ─── 插件配置（spike 期无 schema，patch 行原样透传） ─────────────────────────
export interface BridgeConfig {
  grokBin: string;
  realHome: string;
  /** 默认工作目录（grok 会话落 ~/.grok/sessions/<encoded(cwd)>） */
  cwd: string;
  /** 无有效模型选择时的兜底模型（写进档案 model 字段） */
  defaultModel?: string;
  /** @deprecated 不再强制下发（Default 语义：未显式选档时尊重 grok/模型原生默认）；仅留作字段兼容 */
  reasoningEffort?: string;
  /** 模型档案清单（OpenAI 兼容中转等），写入 ~/.grok/config.toml 的 [model.grokdesk-*] */
  profiles?: GrokProfileEntry[];
  /** 关闭单模型钉死转发（默认开启：辅助请求统一改写为所选模型出网） */
  pinModel?: boolean;
  /** retry 止损阈值 ms（默认 120s） */
  retryAbortMs?: number;
  /** prompt 空闲超时 ms（默认 600s；会话有流量即续期，长回合不误杀） */
  promptIdleMs?: number;
  /** prompt 绝对上限 ms（默认 43200s=12h） */
  promptHardMs?: number;
}

const PROVIDER = "grok";
/** 看起来像 grok 侧模型/档案名的选择才透传，deepseek-official 等注册表模型一律走默认 */
function looksLikeGrokModel(m: string | undefined): m is string {
  return !!m && /^(grok|apikey|grokdesk|xai)/i.test(m);
}

type AnyEventDispatch = {
  emit(name: string, payload: Record<string, unknown>): void;
};

/** grok `subagent_spawned` 事件载荷（2026-10-05 抓线实证字段） */
interface SubagentSpawnFields {
  subagent_id?: string;
  child_session_id?: string;
  description?: string;
  subagent_type?: string;
  parent_session_id?: string;
  attempt_id?: string;
}

// ─── Agent 实现 ─────────────────────────────────────────────────────────────
export class GrokBridgeAgent implements Agent {
  readonly id: SessionId;
  readonly session: Session;
  ctx: Context = null as never;
  readonly options: AgentOptions;
  readonly inbox: Inbox;

  private phase: "idle" | "running" = "idle";
  private queue: UserMessage[] = [];
  /** 计划修订循环计数（rejected+意见自动发回 grok 改计划再交审；上限 3 次防死循环，
   *  真实用户输入到队即复位） */
  private revisionLoop = 0;
  /** 只读浏览模式（resume 到「子代理镜像仍在写」的会话时置位）：不占写句柄、
   *  runTurn 拦截发消息（落提示回合），镜像结束后正常 resume 恢复可写。 */
  viewOnlyMirror = false;
  private idleWaiters: Array<() => void> = [];
  private runPromise: Promise<void> = Promise.resolve();
  private disposed = false;
  private cancelRequested = false;
  private currentAttempt = 0;
  private revision = 0;

  /** grok 侧会话绑定：null = 下个回合建立 */
  private acpSessionId: string | null = null;
  private boundSpawnKey = "";
  private boundWantModel = "";
  private driver: AcpDriver;
  private dispatch: AnyEventDispatch | null = null;
  private scopeDispose: (() => Promise<void>) | null = null;
  /** model/selection 事件落下的下一回合模型/档位 */
  private nextModel: string | null = null;
  private nextEffort: string | null = null;
  /** resume 时从持久化事件里扫出的 grok 会话绑定（首个回合尝试 session/load 恢复） */
  private restoreBinding: { grokSessionId: string } | null = null;
  /** 子代理镜像：grok child sessionId -> mirror（spawn_subagent 委派的子会话实时转写进 dsh） */
  private subagentMirrors = new Map<string, SubagentMirror>();

  constructor(
    private loopCtx: Context,
    id: SessionId,
    options: AgentOptions,
    session: Session,
    private config: BridgeConfig,
    private modelSource: ModelSource,
    restoreBinding?: { grokSessionId: string } | null,
  ) {
    this.id = id;
    this.options = options;
    this.session = session;
    const self = this;
    this.inbox = {
      get nextTurn() { return [...self.queue]; },
      get nextStep() { return []; },
      clear() { self.queue = []; },
      append(target, message) { if (target === "next-turn") self.queue.push(message); },
      prepend(target, message) { if (target === "next-turn") self.queue.unshift(message); },
      replace() { return false; },
      remove() { return false; },
      splice(target, start, deleteCount, inserted) {
        const removed = self.queue.splice(start, deleteCount, ...inserted);
        return removed;
      },
    } as Inbox;
    this.driver = this.makeDriver("");
    this.restoreBinding = restoreBinding ?? null;
  }

  private makeDriver(modelProfile: string): AcpDriver {
    return new AcpDriver(
      {
        onUpdate: (sid, u) => this.onAcpUpdate(sid, u),
        onExitPlanMode: params => this.protocolExitPlan(params),
        onAskUserQuestion: params => this.protocolAskUser(params),
        onPermission: req => this.onAcpPermission(req),
        onRetryState: (sid, r) => {
          // 必须走 console：ctx.logger 在 web 模式不落 stdout，重试会完全不可见（实测踩坑）
          console.log(`[grokcli] retry attempt=${r.attempt} reason=${r.reason} (session ${sid.slice(0, 8)})`);
          this.lastRetry = r;
        },
        log: (msg, extra) => console.log(`[grokcli] ${msg}`, extra ?? ""),
      },
      {
        grokBin: this.config.grokBin,
        realHome: this.config.realHome,
        cwd: this.session.header?.cwd || this.config.cwd,
        modelProfile: modelProfile || undefined,
        reasoningEffort: this.explicitEffort(),
        retryAbortMs: this.config.retryAbortMs,
        promptIdleMs: this.config.promptIdleMs,
        promptHardMs: this.config.promptHardMs,
      },
    );
  }

  /** 本回合最后一次 retry_state（止损时用于生成解释性错误） */
  private lastRetry: { attempt: number; reason: string } | null = null;
  /** 计划审批（老大 2026-10-05 提案）：exit_plan_mode 在 ACP 桥接下无 TUI 审批键 →
   *  桥读出 plan.md 弹 dsh 原生审批面板；「允许」→ 收口本回合并自动把计划作为新任务
   *  发给全新 grok 会话继续实施；「拒绝」→ 计划留档收口；15 分钟无人操作 → 看门狗兜底。
   *  （--no-plan/--disallowed-tools 在 agent stdio 被静默忽略，实测三连锄件。） */
  private planStuck = false;
  private planExitTimer: NodeJS.Timeout | null = null;
  private planDecision: "approved" | "rejected" | null = null;
  private planText: string | null = null;
  /** ask_user_question（TUI 提问工具在桥接环境无输入面）：**升级为原生问答面板**
   *  （老大 2026-10-05 选型：ZCode 式点选）——ctx.userQuestions.ask 弹带选项按钮的
   *  面板，点选即答；答案自动发回同一 grok 会话（restoreBinding）续跑。面板不可用/
   *  超时则回退：45s/15min 看门狗收口+问题上屏等打字。 */
  private askStuck = false;
  private askTimer: NodeJS.Timeout | null = null;
  private askRawInput: string | null = null;
  private askAnswer: string | null = null;
  /** 真·协议路径已接管交互（收到 _x.ai/* 服务端请求）：旧看门狗/收口全部让位 */
  private protocolInteractive = false;

  private effectiveModel(): string {
    const pick = (m: string | undefined): m is string => {
      if (!m) return false;
      return looksLikeGrokModel(m) || this.modelSource.isKnownModel(m);
    };
    if (pick(this.nextModel ?? undefined)) return this.nextModel!;
    if (pick(this.options.model)) return this.options.model!;
    // 桥是单槽引擎：dsh 原生 provider（如设置里的 DeepSeek 组）不经过本桥执行——
    // 选中非 grok/非中转目录的模型名时这里静默回落 defaultModel，用户可能毫无感知
    // （2026-10-06 老大问题3实证），至少留下日志痕迹。
    const chosen = this.nextModel ?? this.options.model;
    if (chosen) console.log(`[grokcli] model "${chosen}" not servable by grok bridge (not a grok/relay-catalog model) -> falling back to ${this.config.defaultModel || "(default profile)"}`);
    return this.config.defaultModel || "";
  }

  /** 会话持久化的模型选择（modelSelection 投影；controller 建 agent 时只传 provider/model 不带档位） */
  private persistedSelection(): { model?: string; reasoningEffort?: string } | null {
    try {
      const projections = (this.loopCtx as unknown as {
        sessionProjections?: { stateOf(s: Session, key: string): unknown };
      }).sessionProjections;
      const sel = projections?.stateOf(this.session, "modelSelection") as
        | { pending?: { model?: string; reasoningEffort?: string }; lastUsed?: { model?: string; reasoningEffort?: string }; model?: string; reasoningEffort?: string }
        | undefined;
      // 投影形状：{ pending, lastUsed }——pending 是下一请求待生效的选择，优先
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
  private explicitEffort(): string | undefined {
    const persisted = this.persistedSelection();
    const e = (this.nextEffort
      ?? this.options.reasoningEffort
      ?? persisted?.reasoningEffort) as string | undefined;
    // 第四来源（2026-10-06）：dsh 在「无会话时选档」会把选择写成 agent-default-model 的
    // 全局默认（reasoningEffort）——但 controller 建 agent 不传档位，这个用户意图原本悬空
    // （实测：新会话先选档再发消息，服务端请求体无 effort）。会话内显式选档（上面三段）
    // 优先，全局默认兜底。
    let e2 = e;
    if (e2 === undefined) {
      try {
        const editor = (this.loopCtx as unknown as { get(n: string): unknown }).get("configEditor") as
          | { configuration(): Array<{ entry?: { options?: { id?: string } }; override?: Record<string, unknown>; entry2?: never }> }
          | undefined;
        const row = editor?.configuration?.().find(r => r.entry?.options?.id === "agent-default-model");
        const effort = (row?.override as { reasoningEffort?: string } | undefined)?.reasoningEffort;
        if (effort && /^(off|minimal|low|medium|high|xhigh|max)$/i.test(effort)) e2 = effort.toLowerCase();
      } catch { /* configEditor 不可用则跳过 */ }
    }
    if (e && /^(off|minimal|low|medium|high|xhigh|max)/i.test(e)) return e.toLowerCase();
    return e2;
  }

  /** 动态注入运行期助手（createScope/agentEvents）后生效；失败则降级为裸事件 */
  async bindRuntimeHelpers(): Promise<void> {
    const loadModule = async (name: string): Promise<unknown> => {
      // 1) 裸名（源码树 web：tsx + 仓库模块图）
      try { return await import(name); } catch { /* fallthrough */ }
      // 2) 桌面版：运行期包在 app.asar 内（\dsh\node_modules\@deepseek-ai\*），
      //    Electron RunAsNode 有 asar-fs 补丁，经 resourcesPath 定位后 createRequire 解析
      const resources = (process as unknown as { resourcesPath?: string }).resourcesPath;
      if (resources) {
        try {
          const { createRequire } = await import("node:module");
          const { pathToFileURL } = await import("node:url");
          const anchor = `${resources}/app.asar/dsh/node_modules/@deepseek-ai/dsh-agent/package.json`;
          const req = createRequire(anchor);
          const resolved = req.resolve(name);
          console.log(`[grokcli] runtime: ${name} via asar (${resolved.slice(0, 90)})`);
          return await import(pathToFileURL(resolved).href);
        } catch (e) {
          console.log(`[grokcli] runtime: ${name} asar resolve failed (${String(e).slice(0, 100)})`);
        }
      }
      // 3) 裸名经 dsh-agent 模块位置（兜底）
      try {
        const { createRequire } = await import("node:module");
        const agentUrl = import.meta.resolve("@deepseek-ai/dsh-agent");
        const resolve = createRequire(agentUrl);
        return await import(resolve.resolve(name));
      } catch (e) {
        console.log(`[grokcli] runtime: ${name} all resolution paths failed (${String(e).slice(0, 100)})`);
      }
      throw new Error(`cannot load ${name}`);
    };
    try {
      const agent = (await loadModule("@deepseek-ai/dsh-agent")) as typeof import("@deepseek-ai/dsh-agent");
      console.log("[grokcli] runtime: dsh-agent imported");
      let scope: typeof import("@deepseek-ai/dsh-scope") | null = null;
      try {
        scope = (await loadModule("@deepseek-ai/dsh-scope")) as typeof import("@deepseek-ai/dsh-scope");
        console.log("[grokcli] runtime: dsh-scope imported");
      } catch {
        console.log("[grokcli] runtime: dsh-scope UNRESOLVABLE; ctx unscoped");
      }
      if (scope) {
        try {
          const created = scope.createScope(this.loopCtx, this as unknown as object);
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
      this.dispatch = agent.agentEvents(this.loopCtx, this as unknown as Agent) as unknown as AnyEventDispatch;
    } catch (e) {
      console.log(`[grokcli] runtime: helper import failed, degraded (${String(e).slice(0, 150)})`);
      this.ctx = this.loopCtx;
    }
  }

  get status(): AgentStatus {
    return this.phase;
  }

  private setStatus(next: "idle" | "running") {
    if (this.phase === next) return;
    this.phase = next;
    this.dispatch?.emit("agent/status", { status: next });
  }

  // ── Agent 运行时面 ────────────────────────────────────────────────────────
  send(message: UserMessage, _target: InboxTarget, wakeup: boolean): void {
    this.queue.push(message);
    this.revisionLoop = 0; // 真实用户输入到队：修订循环计数复位
    if (wakeup) this.wake();
  }
  followup(message: UserMessage): void { this.send(message, "next-turn", true); }
  /** steer = 回合运行中的转向（「更像 zcode」③）：入队 + cancel 当前回合（grok 侧
   *  prompt 以 cancelled 收口，turn tail 记 interrupted）→ drain 接续处理转向指令——
   *  不必等当前回合跑完。空闲态调用（无活跃回合）退化为普通 followup。 */
  steer(message: UserMessage): void {
    this.send(message, "next-turn", false);
    if (this.activeTurnContext && this.acpSessionId && !this.cancelRequested) {
      console.log(`[grokcli] steer: 中断当前回合，注入新指令（${(message.content?.[0] as { text?: string } | undefined)?.text?.slice(0, 40) ?? ""}）`);
      void this.driver.cancel(this.acpSessionId).catch(() => {});
    } else {
      this.wake();
    }
  }
  inject(message: UserMessage): void { this.send(message, "next-step", false); }

  /** 前端「释放引擎」按钮的服务端执行体：断开本会话的 grok 连接（杀进程），上下文在
   *  磁盘，下条消息自动重连恢复。多会话挂机时按需释放空闲引擎。 */
  shutdownEngine(): void {
    if (this.boundSpawnKey || this.acpSessionId) {
      console.log(`[grokcli] engine shutdown by user (session ${String(this.id).slice(0, 18)})`);
      try { this.driver.dispose(); } catch { /* 已销毁 */ }
      this.acpSessionId = null;
      this.boundSpawnKey = "";
      this.restoreBinding = this.lookupBinding(this.id);
    }
  }

  cancel(_cause: unknown, _options?: unknown): void {
    this.cancelRequested = true;
    if (this.acpSessionId) {
      void this.driver.cancel(this.acpSessionId);
    }
  }

  whenIdle(): Promise<void> {
    if (this.phase === "idle" && this.queue.length === 0) return Promise.resolve();
    return new Promise<void>(resolve => this.idleWaiters.push(resolve));
  }

  async runMaintenance<T>(task: (signal: AbortSignal) => Promise<T>): Promise<T> {
    return await task(new AbortController().signal);
  }

  private wake() {
    this.runPromise = this.runPromise.then(() => this.drain());
  }

  private async drain(): Promise<void> {
    if (this.disposed) return;
    while (this.queue.length > 0 && !this.disposed) {
      const message = this.queue.shift()!;
      try {
        await this.runTurn(message);
      } catch (e) {
        this.loopCtx.logger.error(`[grokcli] turn crashed: ${String(e)}`);
      }
    }
    for (const w of this.idleWaiters.splice(0)) w();
  }

  // ── 回合执行 ──────────────────────────────────────────────────────────────
  private async runTurn(userMessage: UserMessage): Promise<void> {
    // 只读浏览拦截（resume 到镜像运行中的会话）：不起 grok、agent 零 append——提示回合
    // 经 mirror 落（单本账：mirror 分配 turn/seq 并落库 → follow 通道实时上屏；2026-10-06
    // 修复前 agent 自己 append 会撞 mirror 的 active writer：assertContiguous 失配 →
    // drainPaused 卡死落库管道，且事件 seq 与 follow cursor 重复不上屏）。
    if (this.viewOnlyMirror) {
      const mirror = SubagentMirror.live(String(this.id));
      const userText = blocksToText(userMessage.content);
      const notice = "子代理正在此会话中运行（只读浏览模式）：为保护写入不互相干扰，暂不接受新消息。请回到父会话操作，或等子代理完成后重新打开本会话。";
      const appended = mirror?.appendOperatorNotice(userText, notice) ?? false;
      console.log(`[grokcli] view-only turn intercepted: "${userText.slice(0, 50)}" (notice appended=${appended})`);
      if (!appended) await this.showViewOnlyNoticeCard("子代理已结束或会话已归档，请关闭后重新打开本会话即可正常对话。");
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
    if (this.planExitTimer) { clearTimeout(this.planExitTimer); this.planExitTimer = null; }
    if (this.askTimer) { clearTimeout(this.askTimer); this.askTimer = null; }
    this.setStatus("running");
    const text = blocksToText(userMessage.content);
    const turn = this.lastTurnNumber() + 1;
    const step = 1;

    this.session.append("turn/start", { turn });
    this.session.append("user/message", userMessage, { surfaceOp: "append" });
    this.session.append("step/start", { turn, step });

    const wantModel = this.effectiveModel();
    const entries = await this.modelSource.profiles();
    const route = decideRoute(entries, wantModel);
    // 单模型钉死：档案 base_url 指向本地转发器，辅助请求（起题/摘要）出网前统一改写为所选模型
    let baseOverride: string | undefined;
    const ownerEntry = entries.find(e => `grokdesk-${e.id}` === route.spawnKey);
    if (ownerEntry && ownerEntry.baseUrl && ownerEntry.apiKey && route.model) {
      baseOverride = (await this.modelSource.pinTarget(route.model, ownerEntry.baseUrl, this.explicitEffort())) ?? undefined;
    }
    if (entries.length > 0 && syncGrokProfiles(this.config.realHome, entries, route.model, baseOverride) && this.boundSpawnKey) {
      // 档案变化（换 key/换中转地址）：grok 进程只在 spawn 时读一次 config.toml，旧进程
      // 内存里永远是旧 key（实测踩坑：换 key 后仍 403「换了没用」）。断开连接池强制下段
      // 重建——新 spawn 读新档案；grok 会话在磁盘，loadSession 跨进程恢复上下文不丢。
      console.log("[grokcli] grokdesk 档案变化 -> 重建 grok 连接（新进程读新配置）");
      this.driver.dispose();
      this.boundSpawnKey = ""; // 骗过下方复用判断：走 makeDriver + spawn 新进程
    }
    this.session.append("request/header", {
      header: { config: { provider: PROVIDER, model: route.model || route.spawnKey || "grok", ...this.explicitEffort() ? { reasoningEffort: this.explicitEffort() as never } : {} } },
      reason: this.acpSessionId === null ? "initial" : "series",
    } as never);

    // 会话绑定：模型/档位变化 = 换连接 = 换 grok 会话（进程级 -m/-e 所致）
    const spawnKey = `${route.spawnKey}|${this.explicitEffort() ?? ""}`;
    if (this.acpSessionId === null || this.boundSpawnKey !== spawnKey) {
      try {
        if (this.boundSpawnKey && this.boundSpawnKey !== spawnKey) {
          this.driver.dispose();
        }
        if (this.boundSpawnKey !== spawnKey) {
          this.driver = this.makeDriver(route.spawnKey);
        }
        // 优先恢复持久化绑定的 grok 会话（跨 dsh 重启不失忆）；失败落 newSession
        let restoredId: string | null = null;
        if (this.acpSessionId === null && this.restoreBinding) {
          restoredId = await this.driver.loadSession(this.restoreBinding.grokSessionId);
          console.log(restoredId
            ? `[grokcli] session restored: ${restoredId.slice(0, 8)} (spawn ${spawnKey})`
            : "[grokcli] session restore failed, falling back to new session");
          this.restoreBinding = null; // 只试一次
        }
        this.acpSessionId = restoredId ?? await this.driver.newSession();
        this.boundSpawnKey = spawnKey;
        this.boundWantModel = wantModel;
        // 绑定落边车 map（dsh 会话日志不接受未知事件类型——实测毒化会话，observe 直接拒绝）
        this.modelSource.saveBinding(this.id, this.acpSessionId, spawnKey, wantModel);
      } catch (e) {
        this.failTurn(turn, step, `grok session create failed: ${String(e)}`);
        return;
      }
    }

    // 流式累积器 + <think> 拆分状态机：部分中转（如 xcmapi.org 的 chat 通道）把推理
    // 内容以 <think>...</think> 形式混在 content 流里，这里实时拆分：think 段走推理流
    // （agent_thought_chunk 语义），闭合后的正文走消息流，UI 不再显示原始标签。
    const splitter = makeThinkSplitter();
    this.currentAttempt += 1;
    const attemptId = `grok-${this.currentAttempt}-${randomUUID().slice(0, 8)}` as never;
    this.revision += 1;
    const revision = this.revision;
    let textBuf = "";
    let thoughtBuf = "";
    const textChunks: string[] = [];
    const thoughtChunks: string[] = [];
    const textDt: number[] = [];
    const time0 = Date.now();
    let streamStarted = false;
    // 直播帧序号（2026-10-07 UI 整块问题真凶）：UI 的 SessionAssistantStreamAccumulator
    // 要求 dense frames 的 revision 与 index 每帧严格递增（start 后 chunk 依次 r+1/r+2...、
    // index 0,1,2...）——此前桥发固定 revision/index，第二帧起全被折叠器丢弃，UI 只能等
    // 回合落库整块渲染。按发射顺序计数。
    let frameSeq = 0;
    const startFrame = () => {
      if (!streamStarted) {
        streamStarted = true;
        frameSeq = 1;
        this.dispatch?.emit("agent/assistant-stream", { frame: { type: "start", attemptId, revision: frameSeq, turn, step } });
      }
    };
    const frameMeta = (): { revision: number; index: number } => { frameSeq += 1; return { revision: frameSeq, index: frameSeq - 2 }; };
    const pendingTools = new Map<string, { name: string; arguments: string; callSeq?: number }>();

    this.activeTurnContext = {
      turn, step, attemptId,
      onText: (t: string) => {
      console.log(`[grokcli] text-chunk ${new Date().toISOString().slice(14, 23)} +${t.length}B`); // 流式诊断观测
        startFrame();
        for (const piece of splitter.feed(t)) {
          if (piece.kind === "thought") {
            thoughtBuf += piece.text;
            thoughtChunks.push(piece.text);
            this.dispatch?.emit("agent/assistant-stream", {
              frame: { type: "chunk", attemptId, ...frameMeta(), time: Date.now(), chunk: { type: "reasoning-delta", index: 1, text: piece.text } },
            });
          } else if (piece.text) {
            textBuf += piece.text;
            textChunks.push(piece.text);
            textDt.push(Date.now() - time0);
            this.dispatch?.emit("agent/assistant-stream", {
              frame: { type: "chunk", attemptId, ...frameMeta(), time: Date.now(), chunk: { type: "text-delta", index: 0, text: piece.text } },
            });
          }
        }
      },
      onThought: (t: string) => {
        // 去重（2026-10-07 老大实测"每段显示两次"）：直播翻译（pin 的 <think> 附加流经
        // splitter 拆出）已经喂过思考区时，grok 回合末再发的完整推理摘要是重复内容——丢弃。
        if (thoughtChunks.length > 0) {
          console.log(`[grokcli] thought 去重：丢弃回合末重复摘要 ${t.length}B（直播已展示）`);
          return;
        }
        startFrame();
        thoughtBuf += t;
        thoughtChunks.push(t);
        this.dispatch?.emit("agent/assistant-stream", {
          frame: { type: "chunk", attemptId, ...frameMeta(), time: Date.now(), chunk: { type: "reasoning-delta", index: 1, text: t } },
        });
      },
      onToolCall: (callId: string, title: string, rawInput: string) => {
        const name = title || "tool";
        const args = typeof rawInput === "string" ? rawInput : JSON.stringify(rawInput ?? {});
        // ask_user_question 纯兜底位（不弹卡！）：_x.ai/ask_user_question 协议请求才是唯一
        // 弹卡点（protocolAskUser）。此前这里抢先 svc.ask 弹卡，协议请求到达又弹第二张——
        // 用户答第二张，第一张的 waterfall pending 永不 settle → 「提问·等待回答」状态条
        // 残留、composer 被锁 h=0（2026-10-06 实锄件）。本分支只布防看门狗：协议请求
        // 迟迟不来（协议路径失效）或面板长期无人答时，cancel 收口走 restoreBinding 文本流。
        if (name === "ask_user_question" && !this.askTimer && !this.protocolInteractive) {
          this.askRawInput = args;
          const keepSid = this.acpSessionId;
          const cancelTurn = () => {
            if (keepSid) this.restoreBinding = { grokSessionId: keepSid }; // 回答发回同一会话
            if (this.acpSessionId) void this.driver.cancel(this.acpSessionId).catch(() => {});
          };
          this.askTimer = setTimeout(() => {
            this.askTimer = null;
            if (this.activeTurnContext && !this.cancelRequested && this.askAnswer === null && !this.askStuck && !this.protocolInteractive) {
              console.log("[grokcli] ask_user_question fallback (协议请求 45s 未到) -> cancel 收口");
              this.askStuck = true;
              cancelTurn();
            }
          }, 45_000);
          this.askTimer.unref?.();
        }
        // 计划审批纯兜底位（不弹面板！）：_x.ai/exit_plan_mode 协议请求才是唯一弹面板点
        // （protocolExitPlan）。旧实现抢先弹面板 + 协议路径再弹一张 → 残留面板要点两次才
        // 消失（同 ask 的双 pending 根因）。本分支只布防 15 分钟看门狗兜底。
        if (name === "exit_plan_mode" && !this.planExitTimer && !this.protocolInteractive) {
          this.planExitTimer = setTimeout(() => {
            this.planExitTimer = null;
            if (this.activeTurnContext && !this.cancelRequested && this.planDecision === null) {
              console.log("[grokcli] plan approval timeout (15min) -> auto close turn");
              this.planStuck = true;
              if (this.acpSessionId) void this.driver.cancel(this.acpSessionId).catch(() => {});
            }
          }, 15 * 60_000);
          this.planExitTimer.unref?.();
        }
        // v4 格式契约（血泪档案 2026-10-05）：tool/call 之前必须有 assistant/message 以
        // tool-call 内容块「预告」同一 callId（v3-to-v4 relationships.ts:168）——实况投影
        // 不校验看不出来，冷读取直接判损坏并毒化整个列表（实锤断货 25 个会话）。
        // 预告块与 tool/call 的 name/arguments 必须逐字节一致（校验器比对）。
        const advMessage: AssistantMessage = {
          id: `asst-${randomUUID().slice(0, 8)}` as never,
          role: "assistant",
          content: [{ type: "tool-call", id: callId as never, name, arguments: args } as ContentBlock],
          source: { kind: "model", provider: PROVIDER, model: this.boundWantModel || route.model || "grok" },
        };
        this.session.append("assistant/message", { turn, step, message: advMessage, stream: [] } as never, { surfaceOp: "append" });
        // 记下 tool/call 的 seq：tool/result 的 sourceEventSeqs 必须引用它（dsh 事件配对契约，
        // 对齐原生 agent-loop tool-calls.ts 的 appendToolResult 做法）
        const ev = this.session.append("tool/call", { turn, step, callId: callId as never, name, arguments: args }) as unknown as { seq?: number };
        pendingTools.set(callId, { name, arguments: args, callSeq: ev?.seq });
      },
      onToolUpdate: (callId: string, status?: string, title?: string, content?: unknown, locations?: unknown) => {
        const rec = pendingTools.get(callId);
        if (!rec || !status) return;
        if (status !== "completed" && status !== "failed") return; // pending/in_progress：dsh 无工具级进度通道，忽略
        pendingTools.delete(callId);
        const failed = status === "failed";
        // ACP tool_call_update.content[]（replace 语义，取终态帧）→ 单 text block：
        // dsh 通用行对多块/非文本块会 JSON 打印，单文本块既全文展示也可能吃到专用卡。
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
          turn, step, message,
          ...(failed ? { error: { name: "GrokToolError", code: "GROK_TOOL_FAILED", ...(title ? { reason: title } : {}) } } : {}),
          ...(Array.isArray(locations) && locations.length > 0 ? { meta: { locations } } : {}),
        } as never, {
          surfaceOp: "append",
          ...(rec.callSeq !== undefined ? { sourceEventSeqs: [rec.callSeq] as never } : {}),
        });
        console.log(`[grokcli] tool/result ${String(callId).slice(0, 8)} status=${status} text=${text.length}B${Array.isArray(locations) && locations.length ? ` loc=${locations.length}` : ""}`);
      },
      // 交互工具（ask_user_question/exit_plan_mode）走 _x.ai 协议应答收口，grok 不再发
      // 终态 tool_call_update → tool/call 永远 pending：UI 工具卡「运行中」不收、问答投影
      // 卡不关、composer 被锁（实测 1.0.49）。协议应答后按工具名补写配对 tool/result。
      settleToolByName: (name: string, text: string) => {
        for (const [callId, rec] of [...pendingTools]) {
          if (rec.name !== name) continue;
          pendingTools.delete(callId);
          const message: ToolResultMessage = {
            id: `tool-${randomUUID().slice(0, 8)}` as never,
            role: "tool",
            content: [{ type: "text", text } as ContentBlock],
            source: { kind: "tool", callId: callId as never },
            toolCallId: callId as never,
          };
          this.session.append("tool/result", { turn, step, message } as never, {
            surfaceOp: "append",
            ...(rec.callSeq !== undefined ? { sourceEventSeqs: [rec.callSeq] as never } : {}),
          });
          console.log(`[grokcli] interactive tool settled: ${name} ${String(callId).slice(0, 8)} text=${text.length}B`);
        }
      },
    };

    // 实时用量推送（2026-10-06）：pin 每采到一次模型请求 usage，就 append 一个带 usage 流帧
    // 的 assistant/attempt——tokenUsage 投影 fold 它（lastAssistantStreamChunk(stream,'usage')），
    // StatsPills 的 Token/缓存命中/tok·s 随每次模型请求实时刷新（此前要等回合收尾的
    // assistant/message）。不带 surfaceOp（白名单外事件携带会毒会话）。
    const unlistenUsage = onPinUsage(u => {
      try {
        this.session.append("assistant/attempt", {
          turn, step,
          stream: [{ type: "chunk", time: Date.now(), chunk: { type: "usage", usage: u } }],
        } as never);
      } catch (e) { console.log(`[grokcli] live usage append failed: ${String(e).slice(0, 80)}`); }
    });

    let stopReason = "end_turn";
    let failure: string | null = null;
    this.lastRetry = null;
    try {
      stopReason = await this.driver.prompt(this.acpSessionId, text);
    } catch (e) {
      failure = String((e as Error)?.message || e);
      // 连接重建后旧 session id 失效自愈（2026-10-06）：steer 取消的 escalation 杀连接/
      // 子进程崩溃 → driver respawn 新进程，但 agent.acpSessionId 仍指旧进程的会话 →
      // prompt 报 -32602 unknown session 连环失败。grok 会话文件在磁盘，loadSession 同 id
      // 跨进程合法（上下文不丢）；load 失败再落绑定 id / 全新会话。只自愈重试一次。
      if (this.acpSessionId && /connection lost|unknown session|-32602|Invalid params/i.test(failure)) {
        // 连接已换（cancel escalation 杀连接/子进程崩溃 → respawn）：旧 session id 在新进程
        // 上必然失效。connection lost = 预防性作废（本回合已失败收尾，下回合经 restoreBinding
        // 自动恢复上下文）；unknown session/-32602 = 本回合自愈重试（grok 会话文件在磁盘，
        // loadSession 同 id 跨进程合法），只重试一次。
        const dead = this.acpSessionId;
        if (!/unknown session|-32602|Invalid params/i.test(failure)) {
          console.log(`[grokcli] connection lost -> invalidate session ${dead.slice(0, 8)} (下回合经绑定恢复)`);
          this.acpSessionId = null;
          this.boundSpawnKey = "";
        } else {
          const bindingId = this.lookupBinding(this.id)?.grokSessionId;
          console.log(`[grokcli] session ${dead.slice(0, 8)} unknown on connection -> self-heal (reload from disk)`);
          try {
            this.acpSessionId = await this.driver.loadSession(dead)
              ?? (bindingId && bindingId !== dead ? await this.driver.loadSession(bindingId) : null)
              ?? await this.driver.newSession();
            console.log(`[grokcli] self-heal session: ${this.acpSessionId.slice(0, 8)} (was ${dead.slice(0, 8)})`);
            failure = null;
            stopReason = await this.driver.prompt(this.acpSessionId, text);
          } catch (e2) {
            failure = String((e2 as Error)?.message || e2);
            stopReason = "cancelled";
          }
        }
      }
      stopReason = stopReason === "end_turn" && failure ? "cancelled" : stopReason;
      // 非用户取消的超时/异常：同步通知 grok 停止，避免对面孤儿白跑（实测超时后还跑过 7 分钟）
      if (!this.cancelRequested && this.acpSessionId && failure) {
        void this.driver.cancel(this.acpSessionId).catch(() => {});
      }
      // 连接类致命错误（ECONNRESET/ECONNREFUSED/socket hang up——中转断连，2026-10-06 实测
      // 孤儿路径）：连接已断 cancel 通知送不到，grok 侧正在跑的生成就是孤儿——dispose 杀进程。
      if (failure && /ECONNRESET|ECONNREFUSED|socket hang up|EPIPE/i.test(failure)) {
        console.log("[grokcli] 连接类致命错误 -> dispose 杀 grok 进程（防孤儿）");
        try { this.driver.dispose(); } catch { /* 已销毁则忽略 */ }
        this.acpSessionId = null;
        this.boundSpawnKey = "";
      }
      if (/^ACP session\/prompt timeout/.test(failure)) {
        failure = "回合超时：长时间无任何模型输出，已放弃等待并自动停止 grok 侧任务"
          + "（模型正常工作时有持续输出不会触发；此情况多为中转/网络卡死，可重试或新建会话）";
      }
      if (this.cancelRequested) stopReason = "cancelled";
    } finally {
      unlistenUsage();
      this.activeTurnContext = null;
    }
    // 重试止损 → 解释性错误（否则用户只看到静默中断，不知道要改哪里）。
    // 同时丢弃当前 grok 会话：no_visible_content 的常见根因是 grok 侧会话上下文
    // 被污染/过大（中转对大会话回空），止损后下回合自动 session/new 重建。
    const abortedRetry = this.driver.retryAbortedOf(this.acpSessionId);
    if (abortedRetry) {
      failure = `模型通道异常已自动止损：${abortedRetry.reason}（重试 ${abortedRetry.attempt} 次无进展）。`
        + "多为中转/密钥/协议不匹配或会话上下文过大——请到 设置→模型→编辑 提供商 核对密钥/换协议，"
        + "或点「新建会话」重开（grok 侧会话已自动重置）。";
      stopReason = "cancelled";
      this.acpSessionId = null;
      // 连接一并销毁（杀 grok 子进程）：cancel 只是通知，grok 可能继续跑（老大实测止损后
      // 服务端仍在烧 API）；8s 强杀的触发条件是「请求仍挂着」，止损时请求已结束永远不触发
      // ——孤儿 grok 继续生成。止损本就丢弃会话，连接没有保留价值，直接 dispose 一了百了。
      try { this.driver.dispose(); } catch { /* 已销毁则忽略 */ }
      this.boundSpawnKey = "";
    }

    // 流末残余冲刷（未闭合的 think 段按推理计）
    for (const piece of splitter.flush()) {
      if (piece.kind === "thought") { thoughtBuf += piece.text; thoughtChunks.push(piece.text); }
      else if (piece.text) { textBuf += piece.text; textChunks.push(piece.text); textDt.push(Date.now() - time0); }
    }
    // 计划审批收口：批准→计划上屏+自动排队实施；拒绝/超时→计划留档收口。
    // plan-Active 的 grok 会话对后续编辑只读（文档明示），一律弃用，下条消息全新会话。
    if (this.planExitTimer) { clearTimeout(this.planExitTimer); this.planExitTimer = null; }
    if (this.askTimer) { clearTimeout(this.askTimer); this.askTimer = null; }
    if (this.askAnswer !== null || this.askStuck) {
      // ask_user_question 收口：面板点选→答案自动回传同会话续跑；回退→问题上屏等打字
      stopReason = "end_turn";
      failure = null;
      this.acpSessionId = null;
      this.boundSpawnKey = "";
      const q = extractAskQuestion(this.askRawInput);
      if (this.askAnswer !== null) {
        textBuf = `❓→✅ grok 的提问（已在面板点选作答）：\n\n---\n${q}\n---\n\n你的选择：**${this.askAnswer}**\n（已自动发回同一 grok 会话，任务继续——见下一条消息）`;
        const impl = {
          id: `user-ask-${randomUUID().slice(0, 8)}` as never,
          role: "user",
          content: [{ type: "text", text: `用户刚刚在问答面板对你提出的问题作出了选择，请据此继续任务：\n${this.askAnswer}` }] as never,
          source: { kind: "user" } as never,
        } as unknown as UserMessage;
        this.queue.push(impl);
        console.log("[grokcli] ask answered -> auto-continue queued (same grok session)");
      } else {
        textBuf = "❓ grok 在等你回答以下问题（该工具在桥接环境没有输入框，回合已自动收口）。\n\n"
          + "请**直接在输入框回复**，你的回答会带回同一个 grok 会话继续任务：\n\n---\n"
          + q + "\n---";
      }
      textChunks.length = 0;
      textDt.length = 0;
      console.log(`[grokcli] ask_user_question closed: ${this.askAnswer !== null ? "answered" : "fallback-text"}`);
    } else if (this.planDecision !== null || this.planStuck) {
      const plan = this.planText;
      stopReason = "end_turn";
      failure = null;
      this.acpSessionId = null;
      this.boundSpawnKey = "";
      if (this.planDecision === "approved") {
        textBuf = `✅ 计划已批准（原 grok 会话已收口，自动开始实施——见下一条消息）：\n\n---\n${plan ?? "（未找到 plan.md）"}\n---`;
        if (plan) {
          const impl = {
            id: `user-plan-${randomUUID().slice(0, 8)}` as never,
            role: "user",
            content: [{ type: "text", text: `计划已获用户批准，请严格按以下计划开始实施（全新会话，计划即全部上下文）：\n\n${plan}` }] as never,
            source: { kind: "user" } as never,
          } as unknown as UserMessage;
          this.queue.push(impl);
        }
      } else {
        const why = this.planStuck ? "等待审批超时（15 分钟），已自动收口" : "你拒绝了该计划";
        textBuf = `⚠️ grok 进入计划模式并等待审批，${why}。\n\n计划全文留档（该 grok 会话已弃用，下条消息从全新会话开始；要执行请把要点贴回来）：\n\n---\n${plan ?? "（未找到 plan.md）"}\n---`;
      }
      textChunks.length = 0;
      textDt.length = 0;
      console.log(`[grokcli] plan flow closed: decision=${this.planDecision ?? "timeout"} plan=${plan ? `${plan.length}B` : "missing"}${this.planDecision === "approved" ? " -> auto-implement queued" : ""}`);
    }
    console.log(`[grokcli] turn tail: stop=${stopReason} failure=${failure ? failure.slice(0, 60) : "-"} text=${textBuf.length}B think=${thoughtBuf.length}B`);

    // usage/成本（第二步③）：pin 代理在请求层采集（含 grok 的辅助请求），回合窗口求和；
    // TokenUsage 四桶口径：inputTokens=未缓存输入（prompt_tokens 已含缓存，要减）
    let usage: { inputTokens: number; outputTokens: number; cacheReadTokens: number; cacheWriteTokens: number; totalTokens: number } | null = null;
    try { usage = this.modelSource.usageSince?.(time0) ?? null; } catch { usage = null; }
    if (usage) console.log(`[grokcli] usage: in=${usage.inputTokens} cacheR=${usage.cacheReadTokens} out=${usage.outputTokens} total=${usage.totalTokens}`);

    // durable assistant/message（必须先于 end(committed) 帧）
    const content: ContentBlock[] = [];
    if (thoughtBuf) content.push({ type: "reasoning", text: thoughtBuf } as ContentBlock);
    if (textBuf || content.length === 0) content.push({ type: "text", text: textBuf || (failure ? `⚠️ ${failure}` : "(empty)") } as ContentBlock);
    const assistantMessage: AssistantMessage = {
      id: `asst-${randomUUID().slice(0, 8)}` as never,
      role: "assistant",
      content,
      source: { kind: "model", provider: PROVIDER, model: this.boundWantModel || route.model || "grok" },
    };
    const stream: unknown[] = [];
    if (thoughtChunks.length) stream.push({ type: "reasoning-chunks", time0, index: 1, dt: thoughtChunks.map((_, i) => i), texts: thoughtChunks });
    if (textChunks.length) stream.push({ type: "text-chunks", time0, index: 0, dt: textDt, texts: textChunks });
    const appended = this.session.append("assistant/message", {
      turn, step, message: assistantMessage, stream,
      ...(usage ? { usage } : {}),
      ...(stopReason === "cancelled" ? { interrupted: true } : {}),
    } as never, { surfaceOp: "append" });
    console.log(`[grokcli] turn tail: assistant/message appended seq=${appended.seq}`);

    this.dispatch?.emit("agent/assistant-stream", {
      frame: (() => { frameSeq += 1; return { type: "end", attemptId, revision: frameSeq, index: frameSeq - 2, outcome: { kind: "committed", eventType: "assistant/message", seq: appended.seq } }; })(),
    });

    // 未闭合的工具调用补失败结果（turn 结束前必须闭合）
    for (const [callId] of pendingTools) {
      const message: ToolResultMessage = {
        id: `tool-${randomUUID().slice(0, 8)}` as never,
        role: "tool",
        content: [{ type: "text", text: "turn ended before tool completed" } as ContentBlock],
        source: { kind: "tool", callId: callId as never },
        toolCallId: callId as never,
        isError: true,
      };
      this.session.append("tool/result", { turn, step, message } as never, { surfaceOp: "append" });
    }

    this.session.append("step/end", { turn, step });
    this.session.append("turn/end", { turn, reason: stopReasonToEndReason(stopReason, failure) } as never);
    console.log(`[grokcli] turn tail: turn/end appended`);
    this.setStatus("idle");
  }

  private activeTurnContext: {
    turn: number; step: number; attemptId: unknown;
    onText(t: string): void;
    onThought(t: string): void;
    onToolCall(callId: string, title: string, rawInput: string): void;
    onToolUpdate(callId: string, status?: string, title?: string, content?: unknown, locations?: unknown): void;
    settleToolByName(name: string, text: string): void;
  } | null = null;

  private failTurn(turn: number, step: number, message: string) {
    this.session.append("step/end", { turn, step });
    this.session.append("turn/end", { turn, reason: { kind: "error", error: { message, code: "grok_bridge_error" } } } as never);
    this.setStatus("idle");
  }

  private lastTurnNumber(): number {
    try {
      const projections = (this.loopCtx as unknown as { sessionProjections?: { stateOf(s: Session, key: string): { lastTurn?: number } | undefined } }).sessionProjections;
      return projections?.stateOf(this.session, "turnBoundary")?.lastTurn ?? 0;
    } catch {
      return 0;
    }
  }

  /** 未闭合回合兜底（2026-10-06 嵌套 turn 事故）：被强杀/崩溃的回合没有 turn/end——
   *  resume 的冷读不校验嵌套，新回合直接 turn/start 会写出嵌套 turn（dsh relationships
   *  校验器随后判整个会话损坏）。开新回合前若投影里还有 open turn，先补 step/end +
   *  turn/end（interrupted）闭合它。 */
  private closeOpenTurnIfAny(): void {
    try {
      const projections = (this.loopCtx as unknown as { sessionProjections?: { stateOf(s: Session, key: string): { openTurnStartSeq?: number | null; lastTurn?: number } | undefined } }).sessionProjections;
      const st = projections?.stateOf(this.session, "turnBoundary");
      if (!st || st.openTurnStartSeq == null) return;
      const turn = st.lastTurn ?? 0;
      if (turn <= 0) return;
      console.log(`[grokcli] open turn ${turn} detected (被强杀/崩溃的回合) -> 补闭合`);
      // step/end、turn/end 不是 surface 事件：append 不能带 surfaceOp（校验器拒，实锄件）
      this.session.append("step/end", { turn, step: 1 });
      this.session.append("turn/end", { turn, reason: { kind: "interrupted" } } as never);
    } catch { /* 投影不可用时跳过兜底 */ }
  }

  // ── ACP 投影 & 权限桥 ─────────────────────────────────────────────────────
  /** ACP 事件路由（2026-10-05 抓线实证：params.sessionId 干净区分父子——父会话事件带父
   *  id，spawn_subagent 派生的子会话事件带子 id；subagent_* 生命周期在父流上）。 */
  private onAcpUpdate(sid: string | undefined, u: AcpSessionUpdate): void {
    const kind = u.sessionUpdate;
    // 子会话归因 → 镜像路由（grok 原生 spawn_subagent 的子代理转写实时落 dsh 子会话）
    if (sid && this.acpSessionId && sid !== this.acpSessionId) {
      const mirror = this.subagentMirrors.get(sid);
      if (mirror) {
        this.routeToMirror(mirror, u);
      } else if (kind !== "available_commands_update" && kind !== "session_info_update" && kind !== "tool_call_delta_chunk") {
        console.log(`[grokcli] unattributed child update sid=${sid.slice(0, 8)} kind=${kind} ignored`);
      }
      return;
    }
    // 父会话生命周期事件（无 activeTurnContext 也要处理：镜像先于回合上下文建立）
    if (kind === "subagent_spawned") {
      void this.openSubagentMirror(u as unknown as SubagentSpawnFields);
      return;
    }
    if (kind === "subagent_progress") return; // 子代理活动心跳（TUI 用），内容走子事件流
    const c = this.activeTurnContext;
    if (!c) return;
    switch (kind) {
      case "agent_message_chunk": {
        const text = (u as { content?: { text?: string } }).content?.text || "";
        if (text) c.onText(text);
        return;
      }
      case "agent_thought_chunk": {
        const text = (u as { content?: { text?: string } }).content?.text || "";
        if (text) c.onThought(text);
        return;
      }
      case "tool_call": {
        const { toolCallId, title, rawInput } = u as { toolCallId: string; title?: string; rawInput?: unknown };
        c.onToolCall(toolCallId || "unknown", title || "tool", typeof rawInput === "string" ? rawInput : JSON.stringify(rawInput ?? {}));
        return;
      }
      case "tool_call_update": {
        const { toolCallId, status, title, content, locations } = u as { toolCallId: string; status?: string; title?: string; content?: unknown; locations?: unknown };
        c.onToolUpdate(toolCallId, status, title, content, locations);
        return;
      }
      default:
        return; // session_info_update / available_commands_update 等暂忽略
    }
  }

  private async openSubagentMirror(f: SubagentSpawnFields): Promise<void> {
    try {
      const spec: SubagentSpawnSpec = {
        subagentId: String(f.subagent_id ?? f.child_session_id ?? ""),
        childSessionId: String(f.child_session_id ?? f.subagent_id ?? ""),
        description: typeof f.description === "string" ? f.description : undefined,
        subagentType: typeof f.subagent_type === "string" ? f.subagent_type : undefined,
      };
      if (!spec.childSessionId || this.subagentMirrors.has(spec.childSessionId)) return;
      const cwd = (this.session.header as { cwd?: string } | undefined)?.cwd || this.config.cwd;
      const mirror = await SubagentMirror.open(spec, this.loopCtx, this.id, cwd, this.boundWantModel || "grok");
      this.subagentMirrors.set(spec.childSessionId, mirror);
    } catch (e) {
      console.log(`[grokcli] subagent mirror open failed: ${String(e).slice(0, 150)}`);
    }
  }

  private routeToMirror(m: SubagentMirror, u: AcpSessionUpdate): void {
    switch (u.sessionUpdate) {
      case "user_message_chunk": {
        const text = (u as { content?: { text?: string } }).content?.text || "";
        if (text) m.onTask(text);
        return;
      }
      case "agent_message_chunk": {
        const text = (u as { content?: { text?: string } }).content?.text || "";
        if (text) m.onChunk(text, false);
        return;
      }
      case "agent_thought_chunk": {
        const text = (u as { content?: { text?: string } }).content?.text || "";
        if (text) m.onChunk(text, true);
        return;
      }
      case "tool_call": {
        const { toolCallId, title, rawInput } = u as { toolCallId: string; title?: string; rawInput?: unknown };
        m.onToolCall(toolCallId || "unknown", title || "tool", typeof rawInput === "string" ? rawInput : JSON.stringify(rawInput ?? {}));
        return;
      }
      case "tool_call_update": {
        const { toolCallId, status, title, content, locations } = u as { toolCallId: string; status?: string; title?: string; content?: unknown; locations?: unknown };
        m.onToolUpdate(toolCallId, status, title, content, locations);
        return;
      }
      case "response_completed":
        m.onResponseCompleted((u as { usage?: unknown }).usage);
        return;
      case "turn_completed": {
        const tu = u as { stop_reason?: string; usage?: unknown };
        m.onTurnCompleted(tu.stop_reason, tu.usage);
        return;
      }
      default:
        return; // available_commands_update / session_info_update / tool_call_delta_chunk
    }
  }

  // ── grok 私有扩展的真协议应答（_x.ai/*：回合内继续，不收口不换会话）────────
  /** `_x.ai/exit_plan_mode`：planContent 在请求参数里，弹审批面板；approved→grok
   *  同回合退出计划模式继续实施；rejected→修订闭环（④：弹可选意见卡，意见随
   *  rejected 的 comments 回给 grok——线格式支持 comments?: string，实证自
   *  @1agents bridge.js:1647；grok 可带意见修订计划再交审）。 */
  private async protocolExitPlan(params: { planContent?: string }): Promise<{ outcome: string; comments?: string }> {
    this.protocolInteractive = true;
    if (this.planExitTimer) { clearTimeout(this.planExitTimer); this.planExitTimer = null; }
    if (this.askTimer) { clearTimeout(this.askTimer); this.askTimer = null; }
    this.planText = params.planContent ?? null;
    const decision = await this.requestPlanApproval();
    let comments: string | undefined;
    if (decision === "rejected") {
      comments = await this.askRevisionComments();
    }
    console.log(`[grokcli] exit_plan_mode protocol answer: ${decision}${comments ? ` comments=${comments.slice(0, 60)}` : ""}`);
    this.activeTurnContext?.settleToolByName(
      "exit_plan_mode",
      decision === "approved"
        ? "计划已批准（approved），本回合开始实施"
        : comments ? `计划已拒绝（rejected），修改意见：${comments}` : "计划已拒绝（rejected），放弃计划继续对话",
    );
    // 修订闭环（④ 续）：实测 grok 1.0.49 收 rejected 即 end_turn，comments 不进其上下文
    // ——桥把意见自动作为下一轮 prompt 发回同一 grok 会话，驱动「改计划→再交审」循环。
    if (decision === "rejected" && comments && this.revisionLoop < 3) {
      this.revisionLoop += 1;
      this.queue.push({
        id: `user-${randomUUID().slice(0, 8)}` as never,
        role: "user",
        content: [{ type: "text", text: `计划修改意见（自动转达）：${comments}\n请按上述意见修订计划，然后重新提交审批（exit_plan_mode）。` } as ContentBlock],
        source: { kind: "user" },
      } as never);
      this.wake();
      console.log(`[grokcli] revision loop #${this.revisionLoop}: 意见已自动发回 grok`);
    }
    // 线格式（实证自 @1agents bridge.js）：{outcome:"approved"|"rejected"|"abandoned", comments?}
    // ——accepted 会被 grok 当成陌生值处理成「要修改」
    return { outcome: decision === "approved" ? "approved" : "rejected", ...(comments ? { comments } : {}) };
  }

  /** view-only 拦截的降级通知（mirror 已 finalize，提示回合落不了库）：弹一张不落
   *  会话事件的说明卡引导重新打开；面板不可用则只打日志（消息本身已保证不进 grok）。 */
  private async showViewOnlyNoticeCard(text: string): Promise<void> {
    const svc = this.getUserQuestionsSvc();
    if (!svc) {
      console.log(`[grokcli] view-only notice (no panel): ${text}`);
      return;
    }
    try {
      await svc.ask({
        questions: [{ id: "view-only-notice", question: text, options: [{ id: "ok", label: "知道了" }] } as never],
        agent: this,
      });
    } catch (e) {
      console.log(`[grokcli] view-only notice panel failed: ${String(e).slice(0, 120)}`);
    }
  }

  /** 修订闭环的意见入口：拒绝计划后弹一张纯文本问答卡（无选项，custom 即意见；
   *  跳过/留空/面板不可用 = 无意见，直接 rejected）。 */
  private async askRevisionComments(): Promise<string | undefined> {
    const svc = this.getUserQuestionsSvc();
    if (!svc) return undefined;
    try {
      const ans = await svc.ask({
        questions: [{ id: "revision-comments", question: "对这份计划的修改意见？（留空或跳过 = 直接放弃计划）" } as never],
        agent: this,
      });
      const a = ans?.answers?.find(x => x.id === "revision-comments");
      const text = (a?.custom ?? "").trim();
      return text || undefined;
    } catch (e) {
      console.log(`[grokcli] revision comments panel failed: ${String(e).slice(0, 120)}`);
      return undefined;
    }
  }

  /** `_x.ai/ask_user_question`：questions 在请求参数里，弹原生问答面板；
   *  accepted+answers→grok 同回合拿到选择继续干活。 */
  private async protocolAskUser(params: { questions?: unknown }): Promise<{ outcome: string; answers?: Record<string, string | string[]> }> {
    this.protocolInteractive = true;
    if (this.askTimer) { clearTimeout(this.askTimer); this.askTimer = null; }
    if (this.planExitTimer) { clearTimeout(this.planExitTimer); this.planExitTimer = null; }
    const items = parseAskItems(JSON.stringify({ questions: params.questions ?? [] }));
    const svc = this.getUserQuestionsSvc();
    if (!svc || items.length === 0) {
      console.log("[grokcli] ask_user_question: 无面板服务或问题解析失败 -> cancelled");
      return { outcome: "cancelled" };
    }
    try {
      const ans = await svc.ask({ questions: items, agent: this });
      const answers: Record<string, string | string[]> = {};
      for (const it of items) {
        const a = ans?.answers?.find(x => x.id === it.id);
        const picked = [...(a?.selected ?? []), ...(a?.custom ? [a.custom] : [])];
        if (picked.length > 0) answers[it.question] = picked.length > 1 ? picked : picked[0];
      }
      console.log(`[grokcli] ask_user_question protocol answer: ${JSON.stringify(answers).slice(0, 120)}`);
      this.activeTurnContext?.settleToolByName("ask_user_question", `用户已作答：${JSON.stringify(answers)}`);
      return { outcome: "accepted", answers };
    } catch (e) {
      console.log(`[grokcli] ask_user_question panel failed: ${String(e).slice(0, 120)} -> cancelled`);
      this.activeTurnContext?.settleToolByName("ask_user_question", "问题面板已取消（cancelled）");
      return { outcome: "cancelled" };
    }
  }

  /** userQuestions 服务获取（Cordis 铁律：未声明 inject 的服务 get 会抛；双路 try/catch） */
  private getUserQuestionsSvc(): { ask(req: unknown): Promise<{ answers?: Array<{ id: string; selected?: string[]; custom?: string }> }> } | undefined {
    try { return (this.loopCtx as unknown as { userQuestions?: ReturnType<GrokBridgeAgent["getUserQuestionsSvc"]> }).userQuestions; } catch { /* not injected */ }
    try { return (this.loopCtx as unknown as { get(name: string): unknown }).get?.("userQuestions") as ReturnType<GrokBridgeAgent["getUserQuestionsSvc"]>; } catch { return undefined; }
  }

  /** approval 服务获取（Cordis 铁律：未 inject 的服务属性/get 都会抛——实测炸过宿主，
   *  必须整体包 try/catch；userQuestions 同款处理见 getUserQuestionsSvc） */
  private getApprovalSvc(): { request(r: unknown): Promise<string> } | undefined {
    try { return (this.loopCtx as unknown as { approval?: { request(r: unknown): Promise<string> } }).approval; } catch { /* not injected */ }
    try { return (this.loopCtx as unknown as { get(name: string): unknown }).get?.("approval") as { request(r: unknown): Promise<string> } | undefined; } catch { return undefined; }
  }

  /** 弹 dsh 原生**计划审阅卡**审计划（2026-10-06 问题1修复：审批面板 reason 只能塞
   *  260 字预览、长计划被截成省略号看不完——换 userQuestions 的 plan-review intent：
   *  PlanReviewPanel 渲染 detail=计划全文 markdown，提交的文档可在侧边栏打开；
   *  intent.approve 指名批准选项，其余选项=拒绝。UI 不认识 intent 时回退普通选项卡，
   *  应答编码相同（intent 只改展示不改协议）。 */
  private async requestPlanApproval(): Promise<"approved" | "rejected"> {
    const svc = this.getUserQuestionsSvc();
    if (!svc) return "rejected"; // 无面板服务：别挂着，按拒绝收口
    try {
      const plan = this.planText ?? "（计划内容未随请求带上）";
      const APPROVE = "批准，按计划实施";
      const ans = await svc.ask({
        questions: [{
          id: "plan-review",
          question: "grok 已制定计划，请审阅",
          detail: plan,
          options: [{ label: APPROVE }, { label: "拒绝" }],
          intent: { kind: "plan-review", approve: APPROVE },
        } as never],
        agent: this,
      });
      const a = ans?.answers?.find(x => x.id === "plan-review");
      const ok = !!a?.selected?.includes(APPROVE);
      console.log(`[grokcli] plan review card: ${ok ? "approved" : "rejected"} (plan ${plan.length}B 全文)`);
      return ok ? "approved" : "rejected";
    } catch (e) {
      console.log(`[grokcli] plan review card failed: ${String(e).slice(0, 120)} -> rejected`);
      return "rejected";
    }
  }

  private async onAcpPermission(req: AcpPermissionRequest): Promise<AcpPermissionDecision> {
    const approval = this.getApprovalSvc();
    if (!approval) {
      // 无审批服务：fail-closed，选 reject 选项
      const reject = req.options.find(o => /^reject/i.test(o.kind) || /^reject/i.test(o.optionId));
      return reject ? { outcome: "selected", optionId: reject.optionId } : { outcome: "rejected" };
    }
    try {
      const outcome = await approval.request({
        agent: this,
        toolName: req.title || "grok tool",
        callId: req.toolCallId,
        reason: typeof req.rawInput === "string" ? req.rawInput.slice(0, 200) : JSON.stringify(req.rawInput ?? {}).slice(0, 200),
      });
      if (outcome === "allowed-once") {
        const allow = req.options.find(o => o.kind === "allow_once" || o.kind === "allow_always");
        if (allow) return { outcome: "selected", optionId: allow.optionId };
      }
      const reject = req.options.find(o => /^reject/i.test(o.kind) || /^reject/i.test(o.optionId));
      if (reject) return { outcome: "selected", optionId: reject.optionId };
      return { outcome: "rejected" };
    } catch (e) {
      this.loopCtx.logger.warn(`[grokcli] approval failed: ${String(e)}`);
      return { outcome: "rejected" };
    }
  }

  /** 接收 model/selection 会话事件（UI 会话内切模型/档位） */
  onSessionEvent(event: SessionEvent): void {
    if (event.type === "model/selection") {
      const data = event.data as { model?: string; reasoningEffort?: string };
      if (looksLikeGrokModel(data.model) || this.modelSource.isKnownModel(data.model ?? "")) this.nextModel = data.model!;
      if (data.reasoningEffort) this.nextEffort = data.reasoningEffort;
      else if (data.model && data.model !== this.nextModel) this.nextEffort = null; // 换模型不带档位=回默认
    }
  }

  async dispose(): Promise<void> {
    this.disposed = true;
    this.cancel({ kind: "disposed" });
    await this.whenIdle().catch(() => {});
    this.driver.dispose();
    for (const m of this.subagentMirrors.values()) void m.finalize();
    this.subagentMirrors.clear();
    await this.scopeDispose?.().catch(() => {});
  }
}

// ─── Factory ────────────────────────────────────────────────────────────────
/** grok 会话绑定边车（dsh↔grok 会话映射；dsh 日志不接受未知事件类型，只能落自己文件） */
// 会话绑定边车与错误日志统一放 ~/.grokdesk/（跨安装形态稳定：源码树/profile 安装副本都写同一处）
const GROKDESK_HOME = join(homedir(), ".grokdesk");
const BINDING_MAP = join(GROKDESK_HOME, "grok-session-map.json");
function readBindings(): Record<string, { grokSessionId: string; spawnKey?: string; model?: string }> {
  try {
    return JSON.parse(readFileSync(BINDING_MAP, "utf-8"));
  } catch {
    return {};
  }
}
function writeBindings(map: Record<string, unknown>): void {
  try {
    writeFileSync(BINDING_MAP, JSON.stringify(map, null, 2), "utf-8");
  } catch (e) {
    console.log(`[grokcli] binding map write failed: ${String(e).slice(0, 120)}`);
  }
}

export class GrokBridgeFactory implements AgentFactory {
  private settingsProfiles: GrokProfileEntry[] = [];
  private refreshing: Promise<void> | null = null;
  private pinProxy: ModelPinningProxy | null = null;

  constructor(private ctx: Context, private config: BridgeConfig) {}

  /** 重读「设置 → 模型 → 自定义模型 API」（llm-pi-ai 段）并解析密钥；settings/document-updated 触发 */
  async refreshProfiles(): Promise<void> {
    this.refreshing ??= (async () => {
      try {
        const list = await readPiAiProfiles(this.ctx);
        if (list.length) {
          this.settingsProfiles = list;
          console.log(`[grokcli] settings profiles: ${list.map(e => `${e.id}(${e.models.length}m${e.apiKey ? "" : ",NO-KEY"})`).join(", ")}`);
          // effort 装饰（内部差分，无变化不写；延迟出队避免与触发的 HMR 事务嵌套）
          setTimeout(() => {
            void decorateEfforts(this.ctx, list).catch(e => console.log(`[grokcli] decorateEfforts error: ${String(e).slice(0, 120)}`));
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

  private async currentProfiles(): Promise<GrokProfileEntry[]> {
    if (!this.settingsProfiles.length) await this.refreshProfiles();
    return this.settingsProfiles.length ? this.settingsProfiles : (this.config.profiles ?? []);
  }

  private modelSource(): ModelSource {
    return {
      profiles: () => this.currentProfiles(),
      isKnownModel: (m: string) => this.settingsProfiles.some(e => e.models.includes(m)),
      pinTarget: async (model, upstreamBase, effort) => {
        if (this.config.pinModel === false) return null;
        if (!this.pinProxy) this.pinProxy = await ModelPinningProxy.start();
        this.pinProxy.setTarget({ upstreamBase, model, ...(effort ? { effort } : {}) });
        return this.pinProxy.baseUrl;
      },
      saveBinding: (dshSessionId, grokSessionId, spawnKey, model) => {
        const map = readBindings();
        map[dshSessionId] = { grokSessionId, ...(spawnKey ? { spawnKey } : {}), ...(model ? { model } : {}) };
        writeBindings(map);
      },
      usageSince: ts => (this.pinProxy ? usageSince(ts) : null),
    };
  }

  /** 边车查绑定（resume 用） */
  lookupBinding(dshSessionId: string): { grokSessionId: string } | null {
    return readBindings()[dshSessionId] ?? null;
  }

  async createAgent(ownerCtx: Context, options: Parameters<AgentFactory["createAgent"]>[1]): Promise<AgentHandle> {
    // ── subagent 分流（2026-10-05，dsh 源码侦察定案）───────────────────────────
    // in-process 子代理（spawn/fork/continuable）经 AgentRegistry.create 直落本 factory
    // （单槽）。接了会把 delegation 语义（审批策略钉死 'never'/深度预算/工具限制/persona）
    // 整体丢掉，变成"又一个 grok 会话"——语义不对，故一律拒接。
    // 拒接方式 = 同步 throw（在 ANY grok 资源分配之前）：dsh 侧错误文本会作为 isError
    // 工具结果直达父代理模型，且未发布前回滚、无半成品会话残留（返回假值只会推迟爆炸点）。
    // out-of-process 的 dsh-subagent-acp provider 不走 factory，不受影响（配置层分流主通道）。
    const origin = (options.meta as { origin?: string } | undefined)?.origin;
    if (origin === "subagent" || options.parentAgent !== undefined) {
      const msg = "[grokcli-bridge] 本桥不承载 dsh 子代理会话"
        + `（origin=${origin ?? "none"}${options.parentAgent !== undefined ? "，带父代理" : ""}）。`
        + "子代理请走已插入的 dsh-subagent-acp（out-of-process，不经过本桥）；"
        + "如需 in-process 子代理引擎，请恢复 agent-loop 类引擎为本 factory。";
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

  private async createAgentInner(ownerCtx: Context, options: Parameters<AgentFactory["createAgent"]>[1]): Promise<AgentHandle> {
    // 服务访问必须走本插件的 ctx（inject 声明在这里）；ownerCtx 是 registry（dsh-agent）
    // 的 ctx，其 inject 不含 sessions，直接访问会抛 "cannot get property without inject"
    console.log(`[grokcli] createAgent session=${options.sessionId}`);
    const session = this.ctx.sessions.prepare(options.sessionId, {
      ...(options.seed !== undefined ? { seed: options.seed } : {}),
      ...(options.meta !== undefined ? { meta: options.meta } : {}),
      ...(options.inheritedEventCount !== undefined ? { inheritedEventCount: options.inheritedEventCount } : {}),
    });
    return await this.publish(ownerCtx, session, options.agentOptions ?? {}, options.setup, options.signal, "startup", options.parentAgent);
  }

  async resume(ownerCtx: Context, options: Parameters<AgentFactory["resume"]>[1]): Promise<AgentHandle> {
    // 持久化冷读：重建事件序列化进 seed，让投影（turnBoundary 等）与轮次计数恢复。
    // 关键：write handle 必须保持打开（storage 按 session.id 把 live 事件路由进 active writer），
    // 读完就关 = 后续所有事件无处落盘（实测踩坑：成功回合的会话重启后凭空消失）。
    let seed: SessionEvent[] = [];
    let meta: Record<string, unknown> | undefined;
    let keepHandle: Awaited<ReturnType<PersistenceLike["open"]>> | undefined;
    /** 镜像仍在运行（子代理还在这份会话里写）：降级只读浏览——不 open(write)（mirror 的
     *  writer 持有中，open 会 SessionAlreadyOwned），Session 直接复用 mirror 的活实例
     *  （2026-10-06 修复：此前 prepare 新实例 + sessions.enter 会撞 store 里 mirror 的注册
     *  抛 "already exists" → resume 整体失败 → agent 没建成 → 用户发消息被 dsh 吞掉
     *  （gateway/internal）；即便建成，第二本 seq 账也会撞 mirror 的 active writer）。
     *  view-only agent 零 append，提示回合经 mirror.appendOperatorNotice 落（单本账）。 */
    let viewOnlyMirror = false;
    let mirrorSession: Session | undefined;
    console.log(`[grokcli] resume session=${options.resumeSessionId}`);
    const mirrorLive = SubagentMirror.live(String(options.resumeSessionId));
    try {
      const persistence = this.persistence();
      if (persistence && !mirrorLive) {
      // 镜像运行中：不 open(write)（mirror 的 writer 持有中，open 会 SessionAlreadyOwned
      // 抛掉→空启动 agent→发消息会以 turn 1 嵌套写入=损坏复现）。历史展示交给页面 follow
      // 的 opening snapshot（服务端持久化读，实测 712→3698 实时增长即含历史）。
      const handle = await persistence.open(options.resumeSessionId, "write");
      const cold = await handle.read(0, undefined);
      seed = [...cold.events];
      meta = { ...(handle.header as object) };
      keepHandle = handle;
      console.log(`[grokcli] resume cold-read ok events=${seed.length}`);
      // continuable 子代理的冷恢复不带 meta（dsh 源码事实：resume 分支绕过 createAgent
      // 的分流检查），持久化头落地后自查 origin——resume 路径的防线。
      if ((meta as { origin?: string } | undefined)?.origin === "subagent") {
        console.log(`[grokcli] reject subagent resume: session=${options.resumeSessionId}`);
        await handle.close().catch(() => {}); // 释放写句柄，否则会毒化会话（写锁互踩坑）
        keepHandle = undefined;
        throw new Error("[grokcli-bridge] 拒绝恢复子代理会话（header.origin=subagent）：子代理请走 subagent-acp 通道。");
      }
      } else if (!persistence) {
        console.log(`[grokcli] resume: no sessionPersistence service`);
      }
      if (mirrorLive) {
        if (!mirrorLive.session) throw new Error("[grokcli-bridge] mirror-live 会话缺 session 实例，拒绝只读浏览。");
        console.log(`[grokcli] resume mirror-live session -> view-only browse (复用镜像 Session 单本账，不占写句柄)`);
        viewOnlyMirror = true;
        mirrorSession = mirrorLive.session;
      }
    } catch (e) {
      keepHandle = undefined;
      this.ctx.logger.warn(`[grokcli] resume cold-read failed, starting empty: ${String(e)}`);
      dumpError("resume-cold-read", e);
    }
    // 持久化绑定：边车 map 按 dsh 会话 id 查 grok 会话（跨重启恢复 grok 上下文）
    const restoreBinding = viewOnlyMirror ? null : this.lookupBinding(options.resumeSessionId);
    console.log(`[grokcli] resume binding: ${restoreBinding ? restoreBinding.grokSessionId.slice(0, 8) : "(none)"}`);
    // view-only：绝不 prepare 新实例（enter 会撞 mirror 在 store 的注册）——直接用 mirror 的
    const session = mirrorSession ?? this.ctx.sessions.prepare(options.resumeSessionId, {
      seed,
      ...(meta !== undefined ? { meta } : {}),
    });
    return await this.publish(ownerCtx, session, options.agentOptions ?? {}, options.setup, options.signal, "resume", options.parentAgent, keepHandle, seed.length, restoreBinding, viewOnlyMirror);
  }

  private persistence(): PersistenceLike | undefined {
    return (this.ctx as unknown as { get(name: string): unknown }).get("sessionPersistence") as PersistenceLike | undefined;
  }

  private async publish(
    ownerCtx: Context,
    session: Session,
    agentOptions: AgentOptions,
    setup: Parameters<AgentFactory["createAgent"]>[1]["setup"],
    signal: AbortSignal | undefined,
    source: "startup" | "resume",
    parentAgent: Agent | undefined,
    resumedHandle?: PersistenceHandle,
    seedCount = 0,
    restoreBinding: { grokSessionId: string } | null = null,
    viewOnlyMirror = false,
  ): Promise<AgentHandle> {
    const agent = new GrokBridgeAgent(this.ctx, session.id, agentOptions, session, this.config, this.modelSource(), restoreBinding);
    agent.viewOnlyMirror = viewOnlyMirror;
    await agent.bindRuntimeHelpers();

    // view-only：跳过 preset setup（setup 窗口事件会 append 进 mirror 的账，污染子代理转写）
    if (setup && !viewOnlyMirror) {
      const commit = await setup(agent.ctx, agent as unknown as Agent);
      if (commit && typeof (commit as { commit?: () => void }).commit === "function") (commit as { commit(): void }).commit();
    }

    // 持久化登记：create 走 persistence.create 建 writer；resume 复用冷读时打开的 write handle。
    // storage 的 session/event 监听按 session.id 把 live 事件路由进 active writer —— handle
    // 必须保持打开直到 dispose。view-only 无 handle（mirror 的 writer 是唯一 writer）。
    let storedHandle: PersistenceHandle | undefined = resumedHandle;
    if (source === "startup" && !storedHandle) {
      try {
        const persistence = this.persistence();
        storedHandle = await persistence?.create((session as unknown as { header: unknown }).header);
      } catch (e) {
        storedHandle = undefined;
        this.ctx.logger.warn(`[grokcli] persistence create failed: ${String(e)}`);
        dumpError("persistence-create", e);
      }
    }

    // 关键（照抄 agent-loop appendUnstoredSuffix）：seed 与 setup-window 事件（enter 之前
    // 追加、不发 session/event）必须显式刷进 handle，把 cursor 对齐到 session 日志当前长度。
    // 不做的话后续 live 路由第一批就 contiguity 失配 → drainPaused → 全部事件静默滞留内存。
    if (storedHandle) {
      const storedCount = source === "resume" ? seedCount : 0;
      const suffix = (session as unknown as { snapshotEvents(from: number): readonly SessionEvent[] }).snapshotEvents(storedCount);
      if (suffix.length > 0) await storedHandle.append(suffix);
      console.log(`[grokcli] unstored suffix flushed: ${suffix.length} events`);
    }

    // model/selection 监听（全局监听 + 按 session.id 过滤，untagged listener 收全量）
    const unfollowModel = this.ctx.on("session/event", (s: Session, event: SessionEvent) => {
      if (s.id === agent.id) agent.onSessionEvent(event);
    });

    // view-only：session 复用 mirror 的活实例（已在 store、已 announce）——enter/announce
    // 会撞（"already exists"/"already announced"）。agents 注册照常：dsh 的 liveAgent 命中
    // 后续 prompt 直连本 agent（不会再重复 resume）。
    let detachSession: (() => void) | undefined;
    if (!viewOnlyMirror) {
      detachSession = agent.ctx.sessions.enter(session);
      agent.ctx.sessions.announce(session);
    }
    const detachAgent = this.ctx.agents.enter(agent as unknown as Agent, parentAgent);
    await this.ctx.agents.announce(agent as unknown as Agent, source, signal);

    return {
      agent: agent as unknown as Agent,
      dispose: async () => {
        unfollowModel?.();
        await agent.dispose();
        detachAgent();
        detachSession?.();
        await storedHandle?.close?.().catch?.(() => {});
      },
    };
  }
}

// ─── 持久化句柄（jsonl 后端的 open/create 返回形态） ──────────────────────────
interface PersistenceHandle {
  read(a?: number, b?: number, o?: unknown): Promise<{ events: SessionEvent[] }>;
  append(events: readonly SessionEvent[], o?: unknown): Promise<void>;
  header: unknown;
  close(): Promise<void>;
}
interface PersistenceLike {
  open(id: string, mode: string, o?: unknown): Promise<PersistenceHandle>;
  create(header: unknown, o?: unknown): Promise<PersistenceHandle>;
}

// ─── <think> 流式拆分器 ─────────────────────────────────────────────────────
// 部分中转把推理以 <think>...</think> 混在 content 里。跨 chunk 状态机：
// 未定态开头若是 '<think>' 进 think 态；think 态遇 '</think>' 回正文态；
// 尾部可能是被切断的半个标签，保留至下个 chunk 再判。
export interface ThinkPiece { kind: "text" | "thought"; text: string }
const OPEN = "<think>";
const CLOSE = "</think>";
const HOLD = 8; // 可能构成标签前缀的尾部保留长度（max(len(OPEN),len(CLOSE))-1）

export function makeThinkSplitter() {
  let pending = "";
  let inThink = false;
  let sawAnyTag = false;
  return {
    feed(chunk: string): ThinkPiece[] {
      pending += chunk;
      const out: ThinkPiece[] = [];
      for (;;) {
        if (!inThink) {
          if (!sawAnyTag && pending.length <= OPEN.length && OPEN.startsWith(pending)) {
            // 还无法判定是否标签开头：暂留（整条消息以 '<think>' 起头是常态）
            if (!pending.startsWith("<")) { out.push({ kind: "text", text: pending }); pending = ""; }
            break;
          }
          const openAt = pending.indexOf(OPEN);
          if (openAt === 0) { inThink = true; sawAnyTag = true; pending = pending.slice(OPEN.length); continue; }
          if (openAt > 0) {
            out.push({ kind: "text", text: pending.slice(0, openAt) });
            pending = pending.slice(openAt);
            continue;
          }
          // 无标签：尾部可能是半个 '<think' 前缀，保留
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
      return out.filter(p => p.text);
    },
    /** 流结束后冲刷残余（未闭合的 think 整段按 thought 计） */
    flush(): ThinkPiece[] {
      const rest = pending;
      pending = "";
      if (!rest) return [];
      return [{ kind: inThink ? "thought" : "text", text: rest }];
    },
    get inThink() { return inThink; },
  };
}

/** s 的尾部有多长是 tag 的（严格）前缀（且不是完整标签） */
function findPartialSuffix(s: string, tag: string): number {
  for (let k = Math.min(HOLD, s.length); k > 0; k--) {
    const tail = s.slice(s.length - k);
    if (tag.startsWith(tail) && tail.length < tag.length) return k;
  }
  return 0;
}

// ─── 映射工具 ───────────────────────────────────────────────────────────────
const ERR_LOG = join(GROKDESK_HOME, "bridge-errors.log");
function dumpError(where: string, e: unknown): void {
  try {
    appendFileSync(ERR_LOG, `\n[${new Date().toISOString()}] ${where}\n${e instanceof Error ? e.stack : String(e)}\n`);
  } catch {}
}

/** 捞 grok 会话的计划文件（计划模式停滞时的回合产物）：扫 ~/.grok/sessions/*/<sid>/plan.md */
function readGrokPlan(grokSessionId: string): string | null {
  if (!grokSessionId) return null;
  try {
    const root = join(homedir(), ".grok", "sessions");
    for (const ws of readdirSync(root)) {
      const p = join(root, ws, grokSessionId, "plan.md");
      if (existsSync(p)) return readFileSync(p, "utf8").slice(0, 20_000);
    }
  } catch { /* 扫不到按无计划处理 */ }
  return null;
}

/** ask_user_question 的 rawInput → dsh 原生问答面板条目（{"questions":[{question,
 *  options:[{label,description}]}]} 实测形状；options 字段两侧同构直接映射） */
function parseAskItems(raw: string): Array<{ id: string; question: string; options?: Array<{ label: string; description?: string }> }> {
  try {
    const j = JSON.parse(raw) as { questions?: unknown[]; question?: unknown; options?: unknown };
    const src = Array.isArray(j.questions) ? j.questions
      : (j.question !== undefined ? [{ question: j.question, options: j.options }] : []);
    return src.map((qq, i) => {
      const q = qq as { question?: unknown; prompt?: unknown; options?: unknown };
      const options = Array.isArray(q.options)
        ? q.options.map(o => {
            if (typeof o === "string") return { label: o };
            const oo = o as { label?: unknown; text?: unknown; description?: unknown };
            const label = String(oo.label ?? oo.text ?? "");
            return label ? { label, ...(oo.description ? { description: String(oo.description) } : {}) } : null;
          }).filter((o): o is { label: string; description?: string } => o !== null)
        : undefined;
      return { id: `q${i + 1}`, question: String(q.question ?? q.prompt ?? "").slice(0, 500), ...(options && options.length ? { options } : {}) };
    }).filter(q => q.question);
  } catch { return []; }
}

/** 面板答案 → 人话文本（同时用于展示与回传 grok 的消息体） */
function formatAskAnswer(items: Array<{ id: string; question: string }>, ans: { answers?: Array<{ id: string; selected?: string[]; custom?: string }> } | undefined): string {
  const rows: string[] = [];
  for (const it of items) {
    const a = ans?.answers?.find(x => x.id === it.id);
    const choice = a ? [...(a.selected ?? []), ...(a.custom ? [`（补充：${a.custom}）`] : [])].join("、") : "（未作答）";
    rows.push(`「${it.question}」→ ${choice}`);
  }
  return rows.join("\n");
}

/** 从 ask_user_question 的 rawInput 提取人话问题文本（实测形状：{"questions":[{question,
 *  options:[{label,description}]}]}；兼容扁平 {question,options}；兜底原样） */
function extractAskQuestion(raw: string | null): string {
  if (!raw) return "（未捕获到问题内容）";
  const renderOptions = (options: unknown): string => {
    if (!Array.isArray(options) || options.length === 0) return "";
    const lines = options.map((o, i) => {
      if (typeof o === "string") return `${String.fromCharCode(65 + i)}. ${o}`;
      const oo = o as { label?: unknown; text?: unknown; description?: unknown };
      const label = String(oo.label ?? oo.text ?? JSON.stringify(o));
      return oo.description ? `${String.fromCharCode(65 + i)}. ${label} —— ${String(oo.description)}` : `${String.fromCharCode(65 + i)}. ${label}`;
    });
    return `\n选项：\n${lines.join("\n")}`;
  };
  try {
    const j = JSON.parse(raw) as Record<string, unknown>;
    if (Array.isArray(j.questions)) {
      const qs = (j.questions as Array<Record<string, unknown>>).map((q, i) =>
        `**问题${(j.questions as unknown[]).length > 1 ? i + 1 : ""}**：${String(q.question ?? "")}${renderOptions(q.options)}`);
      if (qs.length) return qs.join("\n\n");
    }
    if (typeof j.question === "string" || typeof j.prompt === "string") {
      return `${String(j.question ?? j.prompt)}${renderOptions(j.options)}`;
    }
  } catch { /* 非 JSON 原样 */ }
  return raw.slice(0, 2000);
}

function blocksToText(content: readonly ContentBlock[] | string | undefined): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content.filter(b => (b as { type?: string }).type === "text").map(b => (b as { text?: string }).text || "").join("");
}

// ─── ACP tool_call_update.content[] → 单 text block ─────────────────────────
// dsh 契约（2026-10-05 侦察）：ContentBlockMap 无 resource/locations 概念，通用行对
// 非文本块直接 JSON.stringify 打印；专用卡门槛是"单 text block"。故文本化全部条目并
// 合并为一个 text block（resource_link 的文本化格式照抄 dsh acp/content.ts 惯例）。
const MAX_TOOL_RESULT_CHARS = 64_000;
export function acpToolContentToText(content: unknown): string {
  if (!Array.isArray(content)) return "";
  const parts: string[] = [];
  for (const item of content as Array<Record<string, unknown>>) {
    if (!item || typeof item !== "object") continue;
    const kind = String(item.type ?? "");
    try {
      if (kind === "content") {
        const c = item.content as Record<string, unknown> | undefined;
        const ctype = String(c?.type ?? "");
        if (ctype === "text" && typeof c?.text === "string") parts.push(c.text);
        else if (ctype === "image") parts.push(`[image ${String(c?.mimeType ?? "")}]`);
        else if (ctype === "audio") parts.push("[audio]");
        else if (ctype === "resource_link") parts.push(`\n[resource_link name=${String(c?.name ?? "")} uri=${String(c?.uri ?? "")}]\n`);
        else if (ctype === "resource") parts.push(`\n[resource uri=${String((c as { uri?: unknown })?.uri ?? "")}]\n`);
        else parts.push(JSON.stringify(c ?? item));
      } else if (kind === "diff") {
        const path = String(item.path ?? "");
        const oldText = typeof item.oldText === "string" ? item.oldText : "";
        const newText = typeof item.newText === "string" ? item.newText : "";
        parts.push(`diff ${path}\n${oldText.split("\n").map(l => `-${l}`).join("\n")}\n${newText.split("\n").map(l => `+${l}`).join("\n")}`);
      } else if (kind === "terminal") {
        parts.push(JSON.stringify(item));
      } else {
        parts.push(JSON.stringify(item));
      }
    } catch { /* 单项解析失败不拖垮整包 */ }
  }
  const text = parts.join("\n").trim();
  if (text.length <= MAX_TOOL_RESULT_CHARS) return text;
  return `${text.slice(0, MAX_TOOL_RESULT_CHARS)}\n…（截断：原文 ${text.length} 字符）`;
}

function stopReasonToEndReason(stop: string, failure: string | null): unknown {
  if (failure && !/cancel/i.test(failure)) {
    return { kind: "error", error: { message: failure, code: "grok_bridge_error" } };
  }
  switch (stop) {
    case "end_turn": return { kind: "completed" };
    case "max_tokens": return { kind: "max-tokens" };
    case "cancelled": return { kind: "interrupted" };
    case "refusal": return { kind: "completed" };
    default: return { kind: "completed" };
  }
}
