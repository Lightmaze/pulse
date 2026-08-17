# 参与贡献

[英文版本](CONTRIBUTING.md)

欢迎提交议题与聚焦的 Pull Request。Pulse 是采用 Apache-2.0 的 1.0 前
Research Alpha。

## 环境

```bash
uv sync --locked
uv run pytest -q
uv run python demo.py
uv build
```

需要 Python 3.12 或更新版本。默认测试与 demo 必须在无 API key、无网络访问、
无付费请求的条件下运行。Web 修改还需要：

```bash
cd web
npm ci
npm test
npm run build
```

调用真实 provider 的测试必须使用 `real` marker。默认 pytest 与 CI 会排除它们，
也不会注入 provider 凭据。

## 修改纪律

- 行为修改需要 regression test；
- 修改 STDP 或数值侧路时，应明确其学习信号；
- 新增可学习状态属于 specification change，不是普通 feature；
- 保持 factory 与 field 权重分离；
- 把不确定性如实报告为不确定性；无法观察不等于负面结果；
- 不向公开 tree 加入试验历史、provider transcript、私有计划/状态/完成记录、
  生成制品或本机路径；
- pull request 应保持聚焦，并列出验证命令。

当前工程与发布边界见 [ESSENCE.zh-CN.md](ESSENCE.zh-CN.md)、
[ARCHITECTURE.zh-CN.md](ARCHITECTURE.zh-CN.md) 与
[PUBLIC_RELEASE.zh-CN.md](PUBLIC_RELEASE.zh-CN.md)。
