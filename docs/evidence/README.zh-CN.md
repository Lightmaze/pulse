# 能力证据

[英文版本](README.md)

状态：当前机器可读主张边界。

[`capability-evidence.v1.json`](capability-evidence.v1.json) 是公开能力表的权威来源。

## 等级

- E0 — Exists：实现路径或数据结构存在；
- E1 — Contract：确定性、无 provider 合同通过；
- E2 — Live：实际观察到经授权的真实 provider 或由操作者选择的外部系统/服务；
- E3 — Release：干净检出、固定构建、安装、升级与回滚流程可以复现；
- E4 — Longitudinal causality：长期反事实观察表明机制持续改变后续行为。

无提供者测试、浏览器与 UI 检查、本机 TCP、SQLite、模拟提供者与操作系统
回归仍只属于 E1。把这些检查组合或重复，不会使其变成 E2。

## 状态值

- `VERIFIED`
- `FAILED`
- `BLOCKED_BY_ENVIRONMENT`
- `NOT_CLAIMED`
- `NOT_APPLICABLE`

首个公开版本不分发 provider 会话或研究档案，因此所有公开 E2 字段保持
`NOT_CLAIMED`，也不作 E4 主张。
