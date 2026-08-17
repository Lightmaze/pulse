# Pulse 0.2 Alpha 公开发布边界

[英文版本](PUBLIC_RELEASE.md)

本仓库发布面向产品的工程快照，包含可安装单主机运行时、工作台、当前文档、
确定性合同与可独立运行的验收工具。

本仓库不发布私有开发档案或研究记录。仅从最新文件树删除文件并不足够，因为 Git
仍可到达父提交；因此首个公开版本使用单一干净根提交。

## 主张边界

- E0 — Exists：实现路径存在；
- E1 — Contract：确定性、无 provider 合同通过；
- E2 — Live：实际观察到经授权的真实 provider 或操作者指定外部系统；
- E3 — Release：干净检出可以安装、构建与复现；
- E4 — Longitudinal causality：长期反事实观察表明经历持续改变后续行为。

公开 0.2 Alpha 只主张 E0/E1 工程覆盖与 E3 发布流程，不发布 E2 证据档案，也不作
E4 主张。浏览器、UI、本机 TCP/SQLite、模拟提供者与操作系统回归仍属于 E1。

## 包含

- `src/`：Python 运行时、Harness 边界、存储、API 与权重工具；
- `web/`：浏览器工作台；
- `tests/`：验证已发布代码所需的无 provider 合同；
- `.github/workflows/ci.yml`：无密钥 Windows 与 Ubuntu 持续集成；
- 当前 README、架构、能力/证据、安全、贡献与许可证文档；
- 验证干净检出所需的验收脚本与语言边界检查；
- 固定的 Python、uv、Node 与 npm 元数据。

## 明确排除

- 试验报告、预注册、示例读数与研究笔记；
- 私有规划图、执行计划、状态日志、完成报告、日期化外部证据与评审档案；
- 与私有提交绑定的历史试验代码或配置；
- 原始会话记录、provider 输出、数据库、生成制品、本机路径、交接卡与私有发布草稿；
- 凭据、provider 配置与未获许可资产；
- 经操作者授权的外部执行记录。

权威源仓库可以为治理与追溯私下保留这些材料，但不会把它们复制到公开文件树、
源代码发行包、标签或可达的公开 Git 历史。

## 来源与干净历史

`PUBLIC_SOURCE.json` 记录公开快照的摘要与文件数，不发布私有 Git 对象标识；同时记录
`public_history_mode=clean_root` 与 `source_history_included=false`。

公开历史门禁要求只有一个根提交、`v0.2.0-alpha.1` 标签、项目 noreply 身份和既定
远端地址；它会拒绝父提交与更早标签。

## 验证干净检出

依次运行 `uv sync --locked`、`uv run pytest -q`、`uv build` 与
`bash scripts/release_check.sh`。验收脚本默认检查当前仓库；缺少 Git 工作树会直接失败。
