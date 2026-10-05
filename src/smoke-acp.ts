/**
 * AcpDriver 独立冒烟测试（不经 dsh）：
 * node src/smoke-acp.ts "你好"   —— 走真实 ~/.grok 认证与中转档案
 */
import { homedir } from "node:os";
import { join } from "node:path";
import { AcpDriver } from "./acp-driver.ts";
import { decideRoute, syncGrokProfiles, type GrokProfileEntry } from "./profile-router.ts";

const GROK_BIN = process.env.GROKDESK_GROK_BIN || (process.platform === "win32" ? join(homedir(), ".grok", "bin", "grok.exe") : join(homedir(), ".grok", "bin", "grok"));
const REAL_HOME = process.env.GROKDESK_REAL_HOME || homedir();

// 档案来自哪里？插件化后来自 dsh 设置；冒烟测试先用最小内建档案（apikey-fun 是用户手写的，
// 我们不碰——冒烟直接走默认档案通道，或用 GROKDESK_SMOKE_PROFILE 指定档案 id）。
const profileId = process.env.GROKDESK_SMOKE_PROFILE || "";
const model = process.argv[2] || "你好，请用一句话自我介绍。";

const t0 = Date.now();
const driver = new AcpDriver(
  {
    onUpdate(sid, u) {
      const text = (u as any).content?.text ?? "";
      const head = `${u.sessionUpdate}${text ? ": " + String(text).slice(0, 60).replace(/\n/g, " ") : ""}`;
      console.log(`[update +${Date.now() - t0}ms] ${head}`);
    },
    async onPermission(req) {
      console.log(`[permission] ${req.title} options=${req.options.map(o => o.optionId).join(",")}`);
      return { outcome: "rejected" }; // 冒烟默认拒绝
    },
    onRetryState(sid, r) {
      console.log(`[retry] attempt=${r.attempt} reason=${r.reason}`);
    },
    log(msg, extra) {
      console.log(`[driver] ${msg}`, extra ?? "");
    },
  },
  {
    grokBin: GROK_BIN,
    realHome: REAL_HOME,
    cwd: process.env.GROKDESK_CWD || process.cwd(),
    modelProfile: profileId,
    reasoningEffort: process.env.GROKDESK_SMOKE_EFFORT || undefined,
  },
);

try {
  const acpSessionId = await driver.newSession();
  console.log(`session=${acpSessionId}`);
  const stopReason = await driver.prompt(acpSessionId, model);
  console.log(`\nstopReason=${stopReason}  total=${Date.now() - t0}ms`);
} finally {
  driver.dispose();
}
