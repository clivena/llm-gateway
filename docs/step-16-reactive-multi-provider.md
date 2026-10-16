# Step 16 · 响应式化 + 多 provider + 路由配置化

这一批（P1 → P2 → P3 → F3 → F4 → F5）的目标是**让 README 里的话变成可验证的事实**。

改动前，"全链路非阻塞"是一句口号；"支持多供应商"写在特性表里但配置加第二家
完全不生效；`model=auto` 是主推用法但必然 502。六个问题都有同一个特征：
**不报错**。配置写错不报错，参数被丢弃不报错，账本记了个估算值也不报错。

---

## 汇总

| # | 问题 | 性质 | 状态 |
|---|---|---|---|
| 1 | 全链路不是真的非阻塞（4 处 `.block()`） | **其他健壮性问题的总闸** | 已修 |
| 2 | 配置里加第二家 provider 完全不生效且不报错 | 静默失效 | 已修（`ProviderRegistry`） |
| 3 | `model=auto` 必然 502 | 静默失效 | 已修（候选表配置化 + 启动期校验） |
| 4 | 流式分片的 `model` 在级联升级后是错的 | 归属错误 | 已修（`StreamChunk`） |
| 5 | 流式账本只能按字符数估算 | 计量失真 | 已修（`include_usage`） |
| 6 | 请求参数被静默丢弃（`max_tokens` 等） | 静默失效 | 已修（补 `@JsonProperty`） |
| 7 | 调用方错误被报成 502 | 错误映射 | 已修（→ 400） |

测试 **20 → 40** passed。

---

## 问题 1 · 全链路不是真的非阻塞

### 现象

`ProviderAdapter` / `SemanticCacheService` 的出网方法签名是**同步**的，
调用方再用 `Mono.fromCallable(...).subscribeOn(Schedulers.boundedElastic())` 兜住。

看起来是响应式，实际代价有三：

1. **并发上限被线程池锁死。** 弹性线程池默认 10 × CPU 核数。
   而"少量线程扛高并发"恰恰是引入 WebFlux 的**全部理由**——
   用弹性线程池兜同步调用，等于把 Netty 事件循环退化成了线程池模型。
2. **取消传播断裂。** 客户端断开连接后，被兜住的上游请求仍在跑，
   **费用照产生**。网关的第一职责是管住钱，这条直接违背。
3. **Retry / timeout / circuitBreaker 全都无处可挂。** Reactor 的这几个
   都是**算子**，只能挂在 `Mono`/`Flux` 上。接口是同步的，就没有挂载点。

第 3 条意味着：**响应式化是重试与熔断的硬前提**，不是一个独立的美化项。

### 修法

出网方法全部改为返回 `Mono`/`Flux`，四处 `.block()` 删除。

**`WebClient` 本来就返回 `Mono`**——`.block()` 是后来主动加上去的。
删掉之后代码更短，不是更长。

### 刻意保留的 1 处 `subscribeOn`

`ChatPipeline` 里的 `recordAsync` 保留 `subscribeOn(boundedElastic())`，
但理由**不同**：`KafkaTemplate.send()` 在 broker 不可达时会阻塞到
`max.block.ms`。这是"已知会阻塞的边缘操作"，把它隔离出去是对的；
当初的错误是拿这招去兜**整条链路**。

> 原则：`subscribeOn(boundedElastic())` 适合包一个**已知会阻塞的叶子操作**，
> 不适合包一个**调用链**。

---

## 问题 2 · 第二家 provider 完全不生效

### 根因

`OpenAiAdapter` 是单例 `@Component`，获取时按 `name` 做 `findFirst()`——
即"永远只取第一个"。于是 `gateway.providers` 里写十家，运行时只有一家在工作，
且**没有任何日志或异常提示**。

### 修法

新增 `ProviderRegistry`：

- 按配置为**每家** provider 各建一个适配器实例（`OpenAiAdapter` 改为普通类）
- 建立 `model → adapter` 索引；同名模型**先声明者胜**（顺序即配置文件顺序，
  不依赖 bean 加载顺序——依赖加载顺序是更隐蔽的坑）
- 启动时逐条打印注册结果：`provider 注册：[deepseek] https://... → [models]`
  —— 配置写错在日志里就能看见

### 顺带修掉一个静默陷阱

`WebClient.Builder.baseUrl()` 是 **mutating** 的。多 provider 循环里复用
同一个 builder 实例，会让**所有**适配器指向同一个 base-url——最后一家覆盖前面全部。
必须 `clone()`。

---

## 问题 3 · `model=auto` 必然 502

### 根因

路由候选表硬编码为 `gpt-4o-mini` / `gpt-4o`，而 provider 只配了 deepseek。
`model=auto` 挑出的模型名**没有任何适配器支持**。

### 修法

- 候选表改为配置项 `gateway.router.candidates`
- **启动时校验**"每个候选都被某个 provider 支持"，不一致直接启动失败
  ——把运行时的 502 前移成启动期报错
- `shouldEscalate` 不再按 `"CHEAP"` 字符串判断，改问"还有没有 `qualityScore`
  更高的候选"——这样三档候选表也能自然工作，不必改代码
- 显式指定模型时 `qualityScore = 1.0`，**永远不会被自作主张升级**
  （否则会静默产生贵模型账单）

---

## 问题 4 & 5 · 流式契约的两个信息缺失

新增 `StreamChunk(model, delta, usage)`，把"**谁在服务**"和"**真实花了多少**"
随分片一起带出来。

### 分片里的 `model` 原先只能填请求的模型名

级联升级之后这个值是**错的**——客户端与账本同时看到错误的归属。
现在取上游返回的**真实服务方**。

### 流式账本原先只能按字符数估算

而流式恰恰是主战场（大多数对话场景走流式）。
现在优先采用上游末帧的真实 `usage`（`stream_options.include_usage`），
缺失才回退估算，且**回退值不冒充真实值**。

### 第三个坑：末帧 `usage` 出现在 `choices` 为空的分片里

旧实现把"没有 content 的分片"一律跳过——真实用量就是被这一步丢掉的。
用量帧必须**单独带出来**。

---

## 问题 6 · 参数被静默丢弃

`ChatRequest` 缺少 `@JsonProperty` 时，Spring Boot 的 Jackson 不做
snake_case 转换，于是 `max_tokens` / `top_p` / `stop` / `seed` /
`stream_options` **被静默丢弃**：客户端传了、网关收了、没转发给上游。

修法：补齐字段与 `@JsonProperty` 透传。

---

## 问题 7 · 调用方错误被报成 502

`messages` 为空、`model` 不在服务范围，都是**调用方**错误。
旧实现报 502，客户端会误判为"网关故障"去退避重试——
而**重试一万次结果一样**。

修法：`IllegalArgumentException` → **400**（`invalid_request_error`）；
`ProviderRegistry.forModel` 相应改抛 `IllegalArgumentException`。

另新增 `chatCompletionUsageChunk`：`include_usage` 那一帧的形状是
`choices` 为**空数组**，不能复用 delta 分片构造器——多一个空 `delta`
会让部分客户端当成"又收到一段空回复"。

---

## 验证

### 静态与装配层

| 项 | 结果 |
|---|---|
| 响应式化 | 全仓 `.block()` 残留 **0**（命中全在注释里）；`subscribeOn` 仅剩 1 处且有独立理由 |
| 单元与装配测试 | **40 passed**（原 20 → 新增 20） |
| 启动期校验 | 默认配置与 `local` profile 的 Spring 上下文**均装载成功** |
| 多 provider | 适配器数量 = 配置的 provider 家数 |
| 路由一致性 | 候选与 provider 不一致时**启动即失败** |

### 真机实测（DeepSeek 真实联网调用）

| 项 | 结果 |
|---|---|
| 非流式双模型 | `deepseek-flash` **4.25s**（41→576 tok）、`deepseek-v4-pro` **4.84s**（94→281 tok），均 200 |
| 流式 | 首字 **1076ms** / 总 **1104ms**；9 个文本分片；**收到 `[DONE]` 终止帧** |
| 流式真实 usage | 账本记 `prompt=43 / completion=70`，与上游末帧回传值**逐位一致** |
| 参数透传 | 请求带 `max_tokens: 8` 时，账本 `completion_tokens` **8/8 命中** |
| 账本 traceId 贯通 | 3 行全部与响应头 `X-Trace-Id` 一一对应 |
| 配额原子性 | 20 线程冲（上限 50 / 预扣 7）→ **7 放行 / 13 拒绝**，计数器停在 **49** |
| 账本完整性 | 配额放行的 7 次 → 账本 **7 行**，无静默丢失；`ledger_failure = 0` |

### 顺带排掉的一个环境坑（值得单独记）

Postgres 宿主端口从 `55432` 改到 `15432`。原因：

```
55432 落在 Windows 动态端口排除区间 55359–55458 之内
```

docker 绑定时报 `bind: An attempt was made to access a socket in a way
forbidden by its access permissions`。查法：

```bash
netsh interface ipv4 show excludedportrange protocol=tcp
```

**端口"空闲"不等于"可绑定"。** `netstat` 看不出被排除的区间，
所以这个坑会一直伪装成别的问题。

---

## 从这批抽象出的 2 条原则

1. **静默失效要从"不报错"改造成"启动即报错"。** 这批里有两处都是配置写了没用
   却不报错（多家 provider、候选模型无 provider）。共同修法是同一个动作：
   把校验从运行期前移到**启动期**，并逐条打印注册结果。
   代价是"改配置"从此能让应用起不来——这个代价值得付，因为配置恰恰是
   最容易被单测漏掉的一层（单测都自己造配置对象，测不到 `application.yml` 里那份）。

2. **兜底线程池要包叶子，不要包链路。** `subscribeOn(boundedElastic())`
   包一个已知阻塞的叶子操作（Kafka send）是正确用法；包整条调用链，
   就把 WebFlux 的全部收益换成了它的全部复杂度。

---

## 下一步

见 [step-17](./step-17-token-estimate-and-cache-layers.md)（配额估算重做 + 缓存分层
+ 基础可观测）。重试/熔断尚未接上——响应式化只是它们的**前置条件**。
