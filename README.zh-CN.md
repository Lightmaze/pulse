# Pulse

[英文版本](README.md)

> 为长期运行的 Agent 团队提供持久连续性。

Pulse 是以持久 Pi Harness 会话组成 Agent 团队的单主机运行时。一个
`PulseWorld` 负责持久调度、因果恢复、跨 Engram 内容流、任务与生活中心、
换代和本机浏览器工作台；Pi 负责每个 Engram 的模型—工具循环与完整会话。

这是工程 Research Alpha。当前版本提供可安装代码、确定性合同与可复现发布流程，
不声称意识、自主生活、真实 provider 验证或由长期经历导致的行为变化。

## 当前包含

- 单主机 `PulseWorld`、SQLite 状态、所有者租约与 fencing epoch；
- 每个 Engram 一个持久 Pi Harness 会话，并限制常驻进程数量、隔离局部故障；
- 持久因果事件、turn、generation，以及对不可安全重放外部结果的 `uncertain` 恢复；
- `TaskFront`、`TaskRelationship`、`TaskOffer`、活动中心、RoleLease、
  LivingConcern、LivingOrientation、生活组合与 Purpose 边界；
- 相互分离的内容流、频谱流与隧道流；
- 可回滚的 factory 与 field 权重层；
- 本机回环 REST API、能力 Profile 与本地化浏览器工作台；
- 默认无 provider 凭据测试、固定工具链、冷安装与公开发布门禁。

当前工程表面见[能力说明](docs/CAPABILITIES.zh-CN.md)。

## 无凭据快速开始

锁定工具链使用 Python 3.12.13 与 uv 0.11.1。

```bash
uv sync --locked --extra observatory
uv run python demo.py
uv run pulse --mock
```

`--mock` 是显式离线测试 Harness，不验证 Pi RPC 或真实模型 provider。生产路径在
Pi 无法启动时直接失败，不会静默切换到 mock 后端。

## 生产 Harness

真实运行还需要 Pi Coding Agent 可执行文件：

```bash
npm install -g --ignore-scripts @earendil-works/pi-coding-agent
pi --version
uv sync --locked --extra observatory
uv run pulse
```

provider、模型、可执行文件、Profile、存储与 worker 上限都必须显式配置：

```bash
uv run pulse --pi-provider deepseek --pi-model deepseek-v4-flash
uv run pulse --pi-executable /absolute/path/to/pi
uv run pulse --profile workspace
uv run pulse --db .pulse/run.db --port 8100 \
  --with-claustrum --with-router \
  --pulse-workers 4 --pi-resident-sessions 8
```

Windows PowerShell：

```powershell
uv run pulse --pi-executable C:\path\to\pi.cmd
```

默认 `safe` Profile 拒绝 HTTP 状态修改。`workspace` 与 `lab` 仍要求启动时生成的
Bearer 令牌、精确 Origin，以及能力自己的允许清单、审批和 RoleLease。Profile
是应用层边界，不是操作系统沙箱。参见[安全说明](SECURITY.zh-CN.md)。

## 工作台

Web 工具链固定为 Node.js 24.15.0 与 npm 11.12.1。

```bash
cd web
npm ci
npm test
npm run build
cd ..
uv run pulse
```

工作台由同一运行时托管。英文与简体中文使用独立 locale 资源，界面一次只呈现一种
语言。浏览器、UI、本机 TCP、SQLite 与 mock 回归仍只是工程合同，不构成真实
provider 或外部服务证据。

## 运行模型

```text
单台主机
└── PulseWorld
    ├── 运行时租约、调度器与因果账本
    ├── TaskFront 0..N
    ├── Life Center 0..N
    └── Engram 1..N
        └── 一个持久 PiSession
```

Pi 负责单个 Engram 的模型—工具循环与 transcript；`PulseWorld` 负责跨 Engram
的时间、调度、传播、委派、关系、持久恢复与换代。SQLite 消息索引不能替代 Pi
JSONL transcript。

参见[架构](ARCHITECTURE.zh-CN.md)、[机制不变量](ESSENCE.zh-CN.md)与
[统一术语](docs/architecture/TERMS.zh-CN.md)。

## 开发与验证

```bash
uv sync --locked
uv run pytest -q
uv run python demo.py
uv build
bash scripts/release_check.sh
```

默认测试不读取 provider 凭据，也不发起付费请求。需要外部系统的检查必须保持独立、
显式，并由操作者授权。

## 主张边界

- E0：实现路径存在；
- E1：确定性、无 provider 合同通过；
- E2：实际观察到经授权的真实 provider 或指定外部系统；
- E3：干净检出可以安装、构建与复现；
- E4：长期反事实证据表明行为发生持久变化。

本次公开 Alpha 只主张 E0/E1 工程覆盖与 E3 发布流程，不发布 E2 证据档案，也不作
E4 主张。参见[能力证据](docs/evidence/README.zh-CN.md)与
[公开发布边界](PUBLIC_RELEASE.zh-CN.md)。

## 已知限制

- 单主机、单操作者、1.0 前版本；
- 不提供完整文件系统、网络、内核或凭据沙箱；
- 不公开声称真实 provider、外部 MCP、交互 PTY 或长时容量已经验证；
- 不公开声称意识、自主生活、稳定兴趣形成或长期因果性；
- 外部 Pi/provider 制品与 provider 成本不属于本仓库的可复现供应链。

## 文档

- [架构](ARCHITECTURE.zh-CN.md)
- [机制不变量](ESSENCE.zh-CN.md)
- [能力说明](docs/CAPABILITIES.zh-CN.md)
- [能力证据](docs/evidence/README.zh-CN.md)
- [公开发布边界](PUBLIC_RELEASE.zh-CN.md)
- [安全说明](SECURITY.zh-CN.md)
- [参与贡献](CONTRIBUTING.zh-CN.md)
- [变更记录](CHANGELOG.zh-CN.md)

## 许可证

采用 Apache License 2.0。参见 [LICENSE](LICENSE) 与 [NOTICE](NOTICE)。
