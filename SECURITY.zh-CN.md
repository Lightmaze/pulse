# 安全策略

[英文版本](SECURITY.md)

Pulse 是面向可信主机上单一操作者的 1.0 前 Research Alpha。它具有真实
进程、文件、HTTP 与可选外部工具能力。“默认关闭”不等于“代码不存在”。

## 报告漏洞

不要在公开 issue 中发布可用 exploit、凭据、私有会话或敏感路径。请通过仓库托管方
显示的安全联系方式私下联系维护者，并提供：

- 受影响版本或 commit；
- 部署 Profile 与操作系统；
- 被跨越的能力和边界；
- 已移除秘密与个人路径的最小复现；
- 是否已经向其他人披露。

这是单维护者 Alpha，不承诺响应时限。

## 支持表面

- 公开 `0.2.0-alpha.1` 与之后明确支持的候选；
- Windows-first、单操作者、本机 loopback 部署；
- 只支持公开代码与已发布制品，私有开发历史不属于支持表面。

## 能力 Profile

| Profile | HTTP 状态修改 | 启动 token | 命令与进程能力 |
|---|---|---|---|
| `safe` | 拒绝 | 不开放修改 | 拒绝外部命令与后台进程配置 |
| `workspace` | 可到达既有写路由 | 必须 | 显式配置的有界工作区能力 |
| `lab` | 可到达既有写路由 | 必须 | 显式配置的有界进程能力 |

Profile 是能力上限，不是能力授予。选择 `lab` 不会自动启用工具。Purpose 文本、模型
输出与界面状态都不能改变 Profile、RoleLease、文件边界、网络边界或审批决定。

## HTTP 边界

- 默认绑定 `127.0.0.1`；
- 非 loopback 绑定同时要求 `--allow-network-bind` 与至少一个显式、精确 Origin；
- CORS 拒绝通配符、null、包含凭据、路径、查询或片段的 Origin；
- `workspace` 或 `lab` 下的状态修改路由必须携带本次进程启动生成的 bearer token；
- token 使用固定时间比较；
- 工作台只把 token 保存在当前标签页的 `sessionStorage`；
- health 与 Profile 端点不返回本机绝对路径或 token。

GET 路由默认无认证。精确 CORS 可以限制普通浏览器访问，但不能阻止另一同机进程
直接发出请求。

## 进程与工作区边界

- 工作区写入要求显式根目录与 checkpoint 策略；
- 保护路径、命令允许清单、工具允许清单、审批与 RoleLease 分别执行；
- 进程 containment 用于在关闭时收敛子进程；
- 进程 containment 不是完整文件系统、网络、内核或凭据沙箱；
- 直接组装运行时的库调用者必须施加等价或更严格的外部策略。

## 秘密与敏感数据

不得提交或公开：

- 提供者密钥、启动令牌、Cookie、凭据或私有端点；
- 原始 Pi session、prompt、工具结果或数据库；
- 本机绝对路径、个人工作区名称或生成的运行制品；
- 经操作者授权但未获公开许可的外部证据。

请使用最小权限操作系统账号、独立工作区，以及带消费上限的项目专用 provider 凭据。

## 在范围内

- 绕过 Profile、token、精确 Origin、非 loopback 双重选择、工作区根、保护路径、
  checkpoint、允许清单、审批、RoleLease、TaskRelationship、所有者租约或
  fencing epoch；
- 凭据、会话、工具结果、本机路径或私有载荷进入不应出现的日志、数据库、响应、
  发布制品或错误；
- 运行时关闭后仍可继续产生外部副作用的子进程；
- 从已发布代码可到达的依赖漏洞。

## 研究风险与非保证

- 这些边界不能解决模型错误、偏见或幻觉；
- 只改变文本、但没有跨越工具或授权边界的 prompt injection 仍是研究风险；
- 经操作者批准的动作可以在声明权限内修改文件或产生费用；
- mock 输出是虚构结果，不能当作真实 provider 证据；
- `sessionStorage` 不能抵御同源恶意脚本；
- 同一进程可接触重要个人凭据时，不应让 Alpha 软件处理不可信内容。

参见[架构](ARCHITECTURE.zh-CN.md)、
[能力说明](docs/CAPABILITIES.zh-CN.md)与
[公开发布边界](PUBLIC_RELEASE.zh-CN.md)。
