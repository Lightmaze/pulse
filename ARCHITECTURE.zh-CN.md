# 脉冲计算基质 0.2 Alpha 架构

[英文版本](ARCHITECTURE.md)

状态：`0.2.0-alpha.1` 当前公开架构。

本文只描述已发布运行时，不是开发日记、试验报告或路线图。

## 1. 部署边界

公开部署模型是一台主机、一个 `PulseWorld`、多个 Engram，并为每个 Engram
generation 保留一个持久 `PiSession`。

```text
主机
└── PulseWorld / RuntimeService
    ├── 所有者租约与 fencing epoch
    ├── 调度器与 PulseEngine
    ├── 因果账本与 generation 状态
    ├── TaskFront 0..N
    ├── Life Center 0..N
    ├── Engram 网络与数值控制路径
    └── PiHarnessRuntime
        ├── Engram A ── PiSession A
        ├── Engram B ── PiSession B
        └── Engram N ── PiSession N
```

`RuntimeService` 是当前 `PulseWorld` 的实现。浏览器连接、用户任务或
`TaskFront` 都只是观察与介入入口，不会复制第二个世界。

## 2. 职责层次

| 层次 | 职责 | 所有者 |
|---|---|---|
| 模型—工具往返 | 模型调用、工具调用、结果与停止条件 | Pi |
| Engram 轮次 | 让单个 Engram 持续运行，直到模型—工具循环稳定 | Pi 会话与 Pulse 边界检查 |
| 世界活动 | 时间、准入、调度、传播、委派、恢复与换代 | PulseWorld |

`Pulse` 是世界层资源分配，可以准入一个完整 Pi turn。一个 turn 可以包含多次
模型—工具往返，不能切在工具调用与工具结果之间。调度器时标本身不是 Engram
已经思考的证据。

## 3. 持久会话生命周期

1. 运行时打开 SQLite，并在恢复或启动进程前取得所有者租约；
2. 恢复已经持久化的因果、generation、reservation 与 Harness binding；
3. Engram 首次需要模型计算时才懒启动 Pi 进程；
4. 只把该持久会话尚未读取的自然语言输入提交给它；
5. terminal 与持久化检查完成后，结果才允许投影或传播；
6. 同一 Engram 的 turn 串行执行，不同 Engram 共用有界 worker 池；
7. 空闲进程可以离开常驻池，但不会删除持久 session binding；
8. 关闭时先停止准入、汇合 worker、关闭 transport、释放匹配租约，再关闭 SQLite。

因此，进程寿命与会话连续性彼此独立。重启可以恢复已经物化的 Pi 会话；换代会创建
新的 generation 与会话，同时保留 lineage 引用。

## 4. 信息流边界

| 信息流 | 携带 | 不携带 |
|---|---|---|
| 内容流 | Engram 之间的最终自然语言 | 工具信封或完整 transcript |
| 频谱流 | 对时机与传播的数值调制 | prompt、自然语言或权限 |
| 隧道流 | 定向自然语言工作请求与结果 | 临时身份或绕过 Harness 的路径 |

机器合同会在持久因果写入前验证这些边界。结构化工具轨迹留在原 Engram 会话内。

## 5. 持久真源与投影

| 数据 | 权威来源 |
|---|---|
| 完整模型—工具会话 | Pi JSONL |
| 世界、关系、租约、因果状态与调度 | SQLite |
| 浏览器视图 | 运行时的只读或显式授权投影 |
| factory 数值基线 | 版本化包数据与代码默认值 |
| field 覆盖 | 操作者管理的导出与 checkpoint 状态 |

浏览器不是第二真源，也不会把当前窗口偶然可见的事件累积成一条假想的完整因果链。

## 6. 恢复与外部不确定性

持久操作使用明确终态。外部动作可能已经发生、但结果无法确认时，状态是
`uncertain`。重启不会自动重放该动作；操作者必须对账，或以新的操作身份选择新动作。

所有者租约与 fencing epoch 防止旧运行时在所有权变化后继续提交新状态。RoleLease、
TaskRelationship、能力 Profile 与审批保持彼此独立的授权边界。

## 7. 任务与生活表面

- `TaskOffer` 在主体接受任务前记录拟议条款；
- `TaskRelationship` 记录由主体掌握的参与状态；
- `TaskFront` 是一项任务的持久用户入口；
- Life Center 保存任务之外、也可以保持安静的活动；
- `LivingConcern`、`LivingOrientation`、生活组合与 Purpose 修订可以持久存在，
  不把全部生活转换成任务。

这些对象可以引用同一个 Engram，但不会复制其身份或 Pi 会话。

## 8. 学习状态

当前版本包含三个相互隔离的数值学习路径：

| 路径 | 信号 | 范围 |
|---|---|---|
| 连接时序 | 局部时间共现 | Engram 连接权重 |
| 委派路由 | 相对结果反馈 | 委派路由器 |
| 屏状体调制 | 有界全局激活反馈 | 时机与传播控制 |

factory 权重是随版本提供的基线。field 权重是本地覆盖，可以导出、checkpoint、回滚或
清零。这些路径不修改 provider 模型参数或 Pi transcript。

## 9. API、Profile 与工作台

默认服务绑定回环地址。HTTP 状态修改需要非默认 Profile 与启动 token；非回环绑定
和浏览器 Origin 都必须显式配置。

工作台读取运行时状态，并提供有界介入表面。英文与简体中文词库彼此独立，当前
locale 决定全部本地化标签。UI 回归测试仍是工程合同，不是外部系统证据。

## 10. 公开证据边界

公开仓库包含当前实现、确定性合同与发布流程，不包含私有规划记录、经操作者授权的
外部会话、原始研究数据或生成的运行制品。公开证据表不作 E2 或 E4 主张。

参见[机制不变量](ESSENCE.zh-CN.md)、
[统一术语](docs/architecture/TERMS.zh-CN.md)、
[能力说明](docs/CAPABILITIES.zh-CN.md)与
[公开发布边界](PUBLIC_RELEASE.zh-CN.md)。
