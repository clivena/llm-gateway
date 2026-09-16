# llm-gateway 实现文档

按步骤记录了这个网关从工程底座到真机验证的完整过程。
每篇固定结构：**目标 / 改动 / 关键代码 / 验证方式 / 结果 / 踩了什么坑 / 下一步**。

| 步骤 | 内容 | 文档 |
|---|---|---|
| 01 | 工程底座：WebClient / 配置绑定 / Redis / Kafka | [step-01](./step-01-gateway-bootstrap.md) |
| 02 | 上游适配：OpenAI 非流式 + SSE 流式 + embeddings | [step-02](./step-02-upstream-adapter.md) |
| 03 | 编排管线：配额→缓存→路由→上游→级联→落账 | [step-03](./step-03-chat-pipeline.md) |
| 04 | 语义缓存：向量检索 + 防误命中 | [step-04](./step-04-semantic-cache.md) |
| 05 | 分布式配额：Redis + Lua 原子扣减 | [step-05](./step-05-quota-lua.md) |
| 06 | 成本账本与指标：Kafka 事件 + traceId | [step-06](./step-06-cost-ledger.md) |
| 10 | Maven 安装与 Java 侧编译验证 | [step-10](./step-10-java-build.md) |
| 11 | 接入真实模型：密钥落位与配置接线（DeepSeek） | [step-11](./step-11-provider-config.md) |
| 12 | **真机联调**：跑通全链路并修掉 6 个问题 | [step-12](./step-12-live-integration.md) |
| 13 | **配额实测 + 账本落库**：Lua 无超发、traceId 贯通 | [step-13](./step-13-redis-quota-ledger.md) |

---

## 关于这些文档

它们是**开发过程的原始记录**，不是事后重写的教程，因此：

- 保留了当时的排查路径，包括走过的弯路和一度错误的结论；
- 后面步骤可能会推翻前面步骤的判断——按顺序读，能看出问题是怎么被一步步定位的；
- 编号有跳号（07~09 属于配套的 `llm-eval` 仓库，14 属于 `llm-dashboard` 仓库），因为三个仓库原本是同一个工程的两个阶段。

如果只看一篇，推荐 [step-12 真机联调](./step-12-live-integration.md)：
那一篇里六个问题的定位过程，比最终代码更能说明工程能力。
