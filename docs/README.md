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
| 15 | 全量复查：修掉 5 个"静默失效"缺陷 | [step-15](./step-15-review-fixes.md) |
| 16 | 响应式化 + 多 provider + 路由配置化 | [step-16](./step-16-reactive-multi-provider.md) |
| 17 | **估算从"猜"变"算"** + 缓存拆两级 + MDC 贯通 | [step-17](./step-17-token-estimate-and-cache-layers.md) |
| 18 | 推理型模型的失败语义 + 配额 reserve-then-settle（F0） | [step-18](./step-18-reasoning-model-failure-semantics.md) |

> 16 与 17 均已成篇。14 号步骤当时未单独成篇，内容并入 [step-15](./step-15-review-fixes.md)。
> 当前状态与已知边界见 [README 的已知边界](../README.md#已知边界诚实清单)。

---

## 已被后续改动推翻的结论

下面几条在当时的步骤文档里是**正确**的，但现在已不成立。保留原文是为了不篡改开发记录，
读到时请以本条和 [README](../README.md) 为准：

| 位置 | 当时的结论 | 现状 |
|---|---|---|
| [step-11](./step-11-provider-config.md) | `name` 必须叫 `openai`，改了会启动失败 | **不成立**：`name` 现在是供应商标识（只用于日志与归因），适配器按 provider 列表构建，不再按名字查找 |
| [step-11](./step-11-provider-config.md) | `List<ProviderAdapter>` 里永远只有 1 个元素 | **不成立**：见 `ProviderRegistry`，每家 provider 一个适配器 |
| [step-12](./step-12-live-integration.md) | "多供应商 ❌ 未实现" | **已实现** |
| [step-11](./step-11-provider-config.md) / [step-12](./step-12-live-integration.md) | `adapterFor(model)` 做模型校验 | 方法已不存在，职责移到 `ProviderRegistry.forModel`（找不到抛 `IllegalArgumentException` → HTTP 400） |
| [step-04](./step-04-semantic-cache.md) | 缓存就是"语义缓存"，依赖向量检索 | **已变更**：拆成两级 —— L1 精确（哈希，**不依赖 embedding**）+ L2 语义（向量，可选）。原因是原实现下"缓存开着但真机一次都没命中"是常态（见 [step-17](./step-17-token-estimate-and-cache-layers.md)） |
| [step-03](./step-03-chat-pipeline.md) / [step-05](./step-05-quota-lua.md) | 配额是"一次原子预扣"，扣的就是本次消耗 | **已变更**：改成 reserve-then-settle 两段式。预扣只按 prompt 估算，响应后按真实 usage 补扣；缓存命中全额退还（见 [step-18](./step-18-reasoning-model-failure-semantics.md)） |
| [step-02](./step-02-upstream-adapter.md) | 非流式只取 `message.content` | **已增强**：同时透传 `finish_reason`；流式补上收尾帧。硬编码 `"stop"` 会把"被截断"报成"正常结束"（见 [step-18](./step-18-reasoning-model-failure-semantics.md)） |
| [step-03](./step-03-chat-pipeline.md) | 管线里用 `Flux.defer(...)` 包住同步调用 + `subscribeOn` 挪出事件循环 | **不成立**：适配器接口本身已返回 `Mono`/`Flux`，全链路 `.block()` 残留为 0 |

---

## 关于这些文档

它们是**开发过程的原始记录**，不是事后重写的教程，因此：

- 保留了当时的排查路径，包括走过的弯路和一度错误的结论；
- 后面步骤可能会推翻前面步骤的判断——按顺序读，能看出问题是怎么被一步步定位的；
- 编号有跳号（07~09 属于配套的 `llm-eval` 仓库，14 属于 `llm-dashboard` 仓库），因为三个仓库原本是同一个工程的两个阶段。

如果只看一篇，推荐 [step-12 真机联调](./step-12-live-integration.md)：
那一篇里六个问题的定位过程，比最终代码更能说明工程能力。
