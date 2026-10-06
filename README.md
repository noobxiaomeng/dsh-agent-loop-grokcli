# GrokDesk Bridge

![GrokDesk](assets/brand-mark-128.png)

**GrokDesk by noobxiaomeng** —— 把 [GrokCLI](https://x.ai/)（xAI grok CLI）注册为 [dsh](https://github.com/deepseek-ai)（DeepSeek Harness）主引擎的桥插件。装上这个插件，dsh 桌面/Web 的所有对话、工具执行、子代理全部由你本机的 grok 驱动。

## 特性

- **引擎顶替**：dsh 的 agent-loop 槽位由本插件接管，spawn `grok agent stdio`（ACP 协议）作为唯一引擎
- **完整回合链**：流式文本/思考（含 `<think>` 拆分）、工具调用直播、usage 用量统计
- **原生交互面板**：grok 的提问（ask_user_question）与计划审批（exit_plan_mode）直连 dsh 原生卡片，点选/批准后 grok 同回合继续
- **子代理会话镜像**：grok 原生 `spawn_subagent` 的子会话实时镜像成 dsh 会话（侧栏独立分组、可回放、运行中可只读浏览）
- **设置桥**：dsh 设置 → 模型 → 自定义模型 API 填的 baseURL/key 自动翻译成 grok 档案
- **跨重启恢复**：会话绑定落边车，重启 dsh 后 grok 上下文无缝恢复

## 前置要求

- **dsh**：0.2.1-alpha.1（桥依赖的运行期接口在此版本实测；其他版本未验证）
- **GrokCLI**：已安装并登录（或已配好兼容 OpenAI Chat Completions 的中转），默认安装位 `~/.grok/bin/`
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

**自定义路径**：在你 profile 的 `cordis.patch.yml`（`~/.dsh/profiles/<name>/`）加 override：

```yaml
- id: grokcli-bridge
  config:
    grokBin: 'D:/tools/grok/grok.exe'
    cwd: '/your/workspace'
    defaultModel: 'grok-4.7'
```

**API 密钥/中转**：不需要改文件——dsh 设置 → 模型 → 添加模型提供商 → 自定义模型 API，填 baseURL/key/模型 ID（协议选 OpenAI Chat Completions），插件自动同步成 grok 档案。

## 数据目录

运行时数据统一放 `~/.grokdesk/`：

- `grok-session-map.json` —— dsh↔grok 会话绑定边车（跨重启恢复）
- `bridge-errors.log` —— 桥错误日志
- `subagents/` —— 子代理镜像会话的工作区目录

## 开发

```bash
# 构建（Node half + 浏览器 half，client.js 为手写产物按需编辑）
npx esbuild src/index.ts --bundle --format=esm --platform=node \
  --outfile=lib/index.js --external:@deepseek-ai/* --external:node:*

# 源码直跑（dsh 源码树）
corepack pnpm dsh web --patch <你的 overlay 文件>
```

改代码后同步进 profile 安装副本：先完全退出 dsh 桌面 → 拷贝 `lib/`、`dsh-patch.yml`、`package.json` 到 `~/.dsh/profiles/<name>/node_modules/dsh-agent-loop-grokcli/` → 重启。

## 已知限制

- dsh 版本钉 0.2.1-alpha.1，跨版本升级需回归验证（协议面依赖实测清单见源码注释）
- 图像生成等媒体工具走 xAI 原生 API，经中转时不可用（中转只开 chat 通道）
- 逐字打字机效果不支持（子代理浏览为块级直播粒度）

## License

MIT © noobxiaomeng
