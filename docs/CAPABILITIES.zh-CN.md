# Pulse 0.2 Alpha 能力

[英文版本](CAPABILITIES.md)

本文描述当前公开工程表面，不是开发日记，也不记录实现过程。

## 已交付

- 一个带 fencing 的单主机 `PulseWorld` 与持久 SQLite 状态；
- 每个 Engram 一个持久 Pi Harness 会话，并限制常驻进程、隔离局部故障；
- 持久因果事件、turn 与 generation，并以 `uncertain` 恢复代替自动重放；
- TaskFront、ActivityCenter、TaskOffer、TaskRelationship 与 RoleLease 边界；
- LivingConcern、LivingOrientation、生活组合与受 settlement 约束的 Purpose 修订；
- 内容、频谱与隧道流的独立机器合同；
- factory/field 权重层，以及导出、恢复点、重置与回滚；
- 本机 REST API、safe/workspace/lab Profile 与浏览器工作台；
- 固定构建版本、无密钥 CI，以及 wheel/sdist 冷安装验证。

## 合同证据

默认测试不需要 provider 凭据。代表性验证路径包括：

- `tests/test_pi_harness.py` 与 `tests/test_runtime_harness_lifecycle.py`；
- `tests/test_causal_ledger.py` 与 `tests/test_generation_recovery.py`；
- `tests/test_task_relationship_api.py` 与 `tests/test_harness_role_leases.py`；
- `tests/test_process_containment.py` 与 `tests/test_api_security.py`；
- `tests/test_storage_migrations.py` 与 `tests/test_release_contract.py`；
- Web 单元测试与生产构建。

机器可读真源是
[`evidence/capability-evidence.v1.json`](evidence/capability-evidence.v1.json)。

## 不作主张

- 意识、sentience 或完整自主生活；
- 由长期经历形成的兴趣或 E4 因果性；
- 真实 provider、外部 MCP 或完整 provider-to-UI 链的公开 E2 证明；
- 多主机协调、分布式共识或不可信多租户；
- 完整操作系统沙箱、交互 PTY 或无限制个人文件/网络/账号访问；
- 外部 Pi/provider 发布制品或成本的可复现性。

无提供者、模拟测试、浏览器/UI、本机 TCP/SQLite 与操作系统回归仍属于 E1 合同。
