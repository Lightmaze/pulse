# 脉冲工作台

[英文版本](README.md)

Pulse Workbench 是 Pulse 随附的浏览器界面。它是运行时 API 的客户端，
不是独立的世界真源或因果真源。

## 工具链

- Node.js 24.15.0
- npm 11.12.1

```bash
npm ci
npm test
npm run build
```

本地开发：

```bash
npm run dev
```

开发服务器监听 `http://localhost:5173`。生产构建写入 `web/dist/`，并由 Pulse
运行时托管。

## 模式

- 回放：打开兼容的 MetricsRecorder JSONL 文件；
- 实时：从运行中的本机运行时读取有界事件流；
- 运行时工作区：查看会话、任务与生活中心、因果状态、调度、Harness 活动与能力
  Profile。

回放与实时视图共享同一解析和渲染路径。浏览器不会写入第二份指标或因果数据库。

## 本地化

英文与简体中文使用独立资源模块：

```text
src/locales/en.ts
src/locales/zh-CN.ts
src/workbench/locales/en.ts
src/workbench/locales/zh-CN.ts
```

当前 locale 决定文档语义、页面标题、选择器与本地化文案。语言选择器会用当前语言
显示两个选项，而不是同时混排两种语言的自称。

## 安全

工作台只把启动 bearer token 保存在当前标签页的 `sessionStorage`，不把 token 写入
URL、cookie、SQLite、指标流或 API 响应。浏览器界面不会把应用层 Profile 变成
操作系统沙箱。

## 证据边界

Web 单元测试与生产构建属于 E1 工程合同。回放、浏览器、本机 HTTP 与视觉检查都
不能证明真实 provider、外部服务或长期行为。
