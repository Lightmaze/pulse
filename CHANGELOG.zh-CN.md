# 变更记录

[英文版本](CHANGELOG.md)

所有公开变化都记录在这里。

## [0.2.0-alpha.1] - 2026-08-14

Pulse 首个公开 Research Alpha。

### 新增

- 可安装的单主机 `PulseWorld`，包含每个 Engram 的持久 Pi Harness 会话、持久调度、
  因果恢复与换代；
- 任务/生活中心、TaskOffer、TaskRelationship、LivingConcern、LivingOrientation、
  生活组合与受 settlement 约束的 Purpose 修订；
- 浏览器工作台、本机能力 Profile 与有界观察 API；
- factory/field 权重分离，以及导出、恢复点、重置与回滚；
- 精确 Python/uv/Node/npm 版本、无提供者密钥的 Windows/Ubuntu CI，以及
  wheel/sdist 冷安装验证；
- manifest 驱动的干净 root 公开 exporter 与机器可读来源记录。

### 发布边界

- 公开仓库从一个 root commit 开始，不继承私有开发图或试验 commit；
- 不分发研究日志、试验历史、私有规划/状态/完成记录、日期化证据、provider session
  或生成制品；
- E0/E1 工程覆盖与 E3 发布流程在范围内；不发布 E2 证据档案，也不作 E4 长期主张。
