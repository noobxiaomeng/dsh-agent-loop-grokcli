# GrokDesk Bridge

![GrokDesk](assets/brand-mark-128.png)

**GrokDesk by noobxiaomeng** —— 把 [GrokCLI](https://x.ai/)（xAI grok CLI）注册为 [dsh](https://github.com/deepseek-ai)（DeepSeek Harness）主引擎的桥插件。装上这个插件，dsh 桌面/Web 的所有对话、工具执行、子代理全部由你本机的 grok 驱动。

## 特性

### 引擎与回合体验

- **引擎顶替**：dsh 的 agent-loop 槽位由本插件接管，spawn `grok agent stdio`（ACP 协议）作为唯一引擎，per-(模型, 档位) 连接池复用
- **ZCode 式回合节奏**：思考一段（实时滚动）→ 工具卡弹出（实时）→ 输出一段（流式）循环呈现——桥按「单次模型请求」切段（`response_completed` 边界），每段独立 step/attempt/settle，工具事件自成一组，顺序与粒度都对齐 dsh 原生语义
- **思维链直播**：grok CLI 不转发 responses 的推理增量，桥的本地转发器把 `reasoning_summary_text.delta` 翻译成带 `<think>` 标签的正文流，再由拆分状态机路由到 UI 思考区实时增长（回合末重复摘要自动去重）
- **实时用量条**：每次模型请求的 token 用量（未缓存输入/输出/缓存命中）随请求实时刷新，不必等回合收尾
- **steer 插话**：回合运行中直接发新指令，当前回合自动收口、转向指令接续处理

### 交互与审批

- **原生交互面板**：grok 的提问（`ask_user_question`）与计划审批（`exit_plan_mode`）走 `_x.ai/*` 服务端协议直连 dsh 原生卡片——问答点选即答、计划全文审阅，应答后 grok 同回合继续，面板自动关闭
- **计划修订闭环**：拒绝计划可附修改意见，意见自动回传 grok 修订再交审（上限 3 次防死循环）
- **权限桥**：grok 的工具审批请求映射到 dsh 原生审批流（allow once / reject），无人应答超时 fail-closed

### 子代理

- **子代理会话镜像**：grok 原生 `spawn_subagent` 的子会话实时镜像成 dsh 会话（侧栏独立分组、可回放）；镜像运行中对本会话只读浏览，结束后自动恢复可写

### 中转健壮性（OpenAI 兼容中转实测打磨）

- **坏流全路径防御**：SSE data 行 JSON 校验、事件 shape 白名单（`type` 必须 string）、残缺 error 事件自动补全、非流式大块与流尾残块同规则处理——中转夹带脏数据不再炸 grok 反序列化
- **响应全量 dump**：`~/.grokdesk/relay-last-responses.log` 滚动记录响应原文，再出坏数据直接看字节真身
- **重试止损**：中转拒请求的指数退避可见化 + 次数/时长双保险止损，止损后附解释性错误（不再静默空转），并杀掉 grok 侧孤儿进程（Windows 树杀兜底）
- **超时看门狗**：prompt 空闲窗口续期（长回合有流量不误杀）+ 绝对上限护栏；ECONNRESET 类断连自动 dispose 防白跑
- **连接自愈**：会话 id 在新进程上失效（`-32602`）时自动从磁盘重载恢复，上下文不丢

### 模型路由与设置

- **设置桥**：dsh 设置 → 模型 → 自定义模型 API 填的 baseURL/key/模型自动翻译成 grok 档案，换 key 热生效（自动重建连接）
- **单模型钉死**：grok 的辅助请求（起题/摘要）经本地转发器统一改写为所选模型出网，兼容注入 reasoning effort；responses 端点型 baseURL 自动路由
- **档位语义**：显式选档下发、Default 尊重模型原生默认，会话内切换即时生效

### 会话运维

- **跨重启恢复**：会话绑定落边车，重启 dsh 后 grok 上下文无缝恢复
- **删除链路**：前端「永久删除」直达，归档/删除不再复活、不污染工作区账目
- **引擎释放**：会话菜单「释放引擎（空闲时）」一键杀掉空闲 grok 进程，绑定保留磁盘、下条消息透明重连

## 前置要求

- **dsh**：0.2.1-alpha.1（桥依赖的运行期接口在此版本实测；其他版本未验证）
- **GrokCLI**：1.0.49-alpha 实测（`~/.grok/bin/`）；需已登录或已配好 OpenAI 兼容中转
- 平台：Windows 10/11 实测；macOS/Linux 理论可用（路径自动探测按平台区分）

## 安装

### 方式一：桌面 UI（推荐）

1. 打开 dsh 桌面版，侧栏点「插件」
2. 点「添加插件」，填本仓库地址：
   ```
   github:noobxiaomeng/dsh-agent-loop-grokcli
   ```
   （或直接贴 git URL `https://github.com/noobxiaomeng/dsh-agent-loop-grokcli`）
3. 点安装，重启 dsh 即生效。

### 方式二：CLI（web/tui profile）

```bash
dsh plugin --profile web add github:noobxiaomeng/dsh-agent-loop-grokcli
dsh web
```

> 桌面 profile 由 Electron 应用独占管理，CLI 会拒绝——请用方式一。

## 配置

**默认零配置**：不写任何路径时，插件自动探测——

| 配置项 | 默认值 |
|---|---|
| `grokBin` | `~/.grok/bin/grok.exe`（Windows）/ `~/.grok/bin/grok`（其他） |
| `realHome` | 当前用户主目录 |
| `cwd` | dsh 进程工作目录 |
| `defaultModel` | 无（跟随 grok 档案；建议显式设为你的主模型） |

**自定义路径**：在你 profile 的 `cordis.patch.yml`（`~/.dsh/profiles/<name>/`）加 override：

```yaml
- id: grokcli-bridge
  config:
    grokBin: 'D:/tools/grok/grok.exe'
    cwd: '/your/workspace'
    defaultModel: 'grok-4.7'
```

**API 密钥/中转**：不需要改文件——dsh 设置 → 模型 → 添加模型提供商 → 自定义模型 API，填 baseURL/key/模型 ID，插件自动同步成 grok 档案。

## 数据目录

运行时数据统一放 `~/.grokdesk/`：

- `grok-session-map.json` —— dsh↔grok 会话绑定边车（跨重启恢复）
- `relay-last-responses.log` —— 中转响应原文 dump（坏数据排查第一证据源）
- `bridge-errors.log` —— 桥错误日志
- `subagents/` —— 子代理镜像会话的工作区目录

## 故障排查速查

| 症状 | 先看什么 |
|---|---|
| `serialization error: ...` | `relay-last-responses.log` 尾部 = 坏数据原文；防御日志（丢弃/补全）若已打仍炸，拿报错字段名对照 dump 定位形状缺口 |
| 回合静默中断 + 「模型通道异常已自动止损」 | 中转/密钥/会话上下文过大——核对密钥或新建会话（grok 侧会话已自动重置） |
| 换了 key 没生效 | 无需处理：档案变化自动重建连接；若手动改过 config.toml，重启 dsh |
| 某模型连不通、另一模型正常 | 多半是该渠道上游故障（通道独立），换模型或去中转后台查渠道 |
| 用量/缓存命中不显示 | 确认走的是 settings 里配的档案（pin 转发采集层）；直连 xAI 原生时采集面不同 |

## 开发

```bash
# 构建（client.js 为手写的浏览器 half，按需编辑）
npx esbuild src/index.ts --bundle --format=esm --platform=node \
  --outfile=lib/index.js --external:@deepseek-ai/* --external:node:*
```

安装形态为 symlink 直连源目录：**编译即生效，重启 dsh 桌面加载**（完全退出再启动）。

回归脚本（桥加载 / 完整回合 / ask 链 / plan 链 / 停止键 / 会话恢复，前置：桌面已启动且新 lib 已加载）：

```bash
node F:/GrokDesk/scripts/regression.mjs
```

## 更新日志（要点）

- **2026-10-07**：混合回合流式与顺序根治——按单次模型请求切段（每段独立 step/attempt/settle、工具自成 step）、usage 事件改段边界冲刷（消除客户端 fold 毒化引发的 rebaseline 断流）、pin 首文本 delta 双发修复；引擎释放按钮；思维链直播（`<think>` 翻译链 + 回合末去重）；用量条实时
- **2026-10-06**：中转坏流全路径防御定版（行校验/shape 白名单/error 补全/全量 dump）；重试止损 + 孤儿进程根治；ECONNRESET 断连处置；换 key 热生效；会话删除/归档链路修复；子代理镜像全链实测；协议应答面板自愈；GrokCLI 1.0.49-alpha 适配；回归自动化
- **2026-10-05**：1.0.41→1.0.46 协议面迁移（`_x.ai/*` 服务端请求、`session_notification` 通道分工去重）；桌面 0.2.1-alpha.1 适配；计划审批/问答面板协议化；品牌位交付；开源交付验收

## 已知限制

- dsh 版本钉 0.2.1-alpha.1，跨版本升级需回归验证（协议面依赖实测清单见源码注释）
- 图像生成等媒体工具走 xAI 原生 API，经中转时不可用（中转只开 chat 通道）
- 推理直播的量级取决于中转是否下发 reasoning 摘要增量——简单问题直播量少属正常

## License

MIT © noobxiaomeng
