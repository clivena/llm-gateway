# Step 18 · 推理型模型的失败语义 + 配额 reserve-then-settle（F0）

## 为什么先做这一项

原计划 B 批是 F1 重试熔断 → F2 跨 provider 故障转移。做之前发现一个前置问题：

**熔断器与故障转移都需要一个"这次失败了"的判据。** 而在 F0 之前，
"上游返回了响应"就等于"这次成功了"—— 哪怕响应里一个字的答案都没有。
于是熔断永远不会认为上游在出问题，故障转移也永远不会触发。

先把失败判据做出来，F1/F2 才有东西可测。这一批做三件事：

1. `finish_reason` 透传（它此前被**硬编码**成 `"stop"`）
2. 空正文 / 截断的可用性判定与可观测
3. 配额从"只预扣"改成 reserve-then-settle

---

## 汇总

| # | 问题 | 性质 | 状态 |
|---|---|---|---|
| 1 | `finish_reason` 被硬编码成 `"stop"` | 主动误导客户端 | 已修 |
| 2 | 空正文被当成成功 | **F1/F2 的前置缺失** | 已修（`UpstreamOutcome`） |
| 3 | 配额只扣 prompt，completion 从不入账 | 计量只覆盖百分之几 | 已修（结算补扣） |
| 4 | 流式末帧同时带 usage 与 finish_reason 时整条流 502 | **靠巧合运行的代码** | 已修（`flatMapIterable`） |

测试 **67 → 104** passed。

---

## 问题 1 · `finish_reason` 曾经是硬编码的

### 现象

上游因预算耗尽返回 `finish_reason=length` + 空正文时，客户端收到的是
`finish_reason: "stop"` —— 网关**主动告诉客户端"正常结束了"**。

### 为什么这比"不返回该字段"更坏

字段缺失时客户端至少知道自己不知道；填一个假值则让客户端
**彻底失去判断能力**：它无法区分"模型说完了，答案是空的"与"被截断了，应该加大预算重试"。
这是本次修的东西里唯一一个"谎言型"缺陷，其余都是"沉默型"。

### 修法

- `ChatResponse` 增加 `finishReason` 分量，`OpenAiAdapter` 从上游读出并透传
- `OpenAiProtocol.chatCompletionBody` 加参数，**只在 null 时**兜底为 `stop`
  （上游给了就原样出去，包括 `length` 这种"不圆满"的值）
- 流式侧新增**收尾帧**：`delta: {}` + 非 null 的 `finish_reason`。
  此前流式分片的 `finish_reason` 恒为 null，流的结束只由 `[DONE]` 表达，
  于是流式同样分不出"正常结束"与"被截断"——用户只看到一个突然停住的答案。

---

## 问题 2 · 空正文被当成成功

### 判定谓词的设计

`UpstreamOutcome` 覆盖四种组合，**每一组都必须单独对待**：

| 正文 | finish_reason | empty | truncated | 含义 |
|---|---|---|---|---|
| 有 | stop | ✗ | ✗ | 正常 |
| 空 | length | ✓ | ✓ | 推理型模型预算耗尽 ← **F0 存在的理由** |
| 有 | length | ✗ | ✓ | 客户端 max_tokens 太小 |
| 空 | stop | ✓ | ✗ | 模型主动说"我说完了，内容是空的" |

三条设计要点：

**① 失败谓词用 `empty` 而不是 `truncated`。**
第四行（空正文 + stop）同样是不可用的答案，但它不 truncated。
谓词写成 `truncated` 就会漏掉它 —— 而且漏得很安静。

**② `empty` 与 `truncated` 必须分成两个计数器。**
单独出现时含义**相反**：`empty` 单独涨 = 上游异常，该告警；
`truncated` 单独涨 = 客户端把 `max_tokens` 设小了，该去联系调用方。
合在一个计数器里，这两种情况的处置动作永远分不开。

**③ `empty` 值得重试，`budgetTooSmall` 不值得。**
推理 token 数实测高度不确定（flash 在同一问题上是 512/1024/1935/1740 之间跳），
换一次就可能正常出正文；而"预算不变"的截断重试多少次都一样。
这个区分就是 F1 的重试谓词。

---

## 问题 3 · 配额只扣 prompt

### 证据

`QuotaService.tryConsume` 是全项目**唯一**写入配额键的地方，
`currentUsage` 只读 —— 也就是说**没有任何结算/补扣步骤**。
配额记的永远是"prompt 估算之和"。而实测：

| 请求 | prompt | completion |
|---|---|---|
| 什么是语义缓存 | 33 | **2172** |
| 什么是语义缓存（标点不同） | 34 | **3516** |
| 什么是成本账本 | 34 | **2464** |

completion 是 prompt 的 **20~100 倍**（推理型模型把 reasoning token
也计进 `completion_tokens`）。所以 `quota.default-limit-tokens` 实际约束的
东西，与它名字宣称的不是一回事。

### 修法：reserve-then-settle

| 阶段 | 时机 | 依据 | 作用 |
|---|---|---|---|
| 预扣 | 请求**之前** | prompt 估算 | 在花钱之前拦住超额请求 |
| 结算 | 响应**之后** | 真实 usage | 把差额补上（可为负 = 退还） |

`quota_settle.lua` 的三个语义细节，都单独验证过：

```
预扣 34 → 结算 +1605 → 得 1639          （补扣）
结算 -1639 → 得 0                        （退还，压到 0 不为负）
键不存在 → 返回 -1                       （周期翻篇，跳过本轮结算）
已用 50，结算 -9999 → 得 0               （下限保护）
```

**周期翻篇必须跳过**，不能把上个月的差额补到本月头上 ——
那会让上月用量污染本月计数。代价是"跨周期边界上的最后几笔不结算"，
换来"每个周期的用量只含本周期"，后者才是配额要的语义。

### 顺带修掉的一条：缓存命中原先白扣配额

F0 之前，缓存命中虽然 usage 记 0、账本记 0 成本，**但配额照样按 prompt 估算扣了一笔**。
也就是说"命中就省钱"这个理由，在配额侧被削掉了一半。
现在命中路径全额退还预扣（真机验证：命中前后配额增量 = 0）。

### 已知代价：天然允许小幅超发

真实值要等上游返回才知道，那时钱已经花了。这是选这个口径时已知情并接受的代价，
不是缺陷。结算脚本里**刻意不做上限检查** —— 在下游补一道闸没有意义。
要收紧应该换成"按 max_tokens 上界预留"的口径，而不是在结算时补救。

---

## 问题 4 · 一个"靠数据形状的巧合运行"的 502

### 现象

加上收尾帧之后，**流式请求全部 502**，响应体是：

```json
data: {"error":{"message":"Cannot emit more than one data","type":"upstream_error"}}
```

### 根因

解析逻辑写在 `handle((json, sink) -> ...)` 里，而 **`SynchronousSink`
每次调用只允许发一个元素**。段里有三个各自独立的 `sink.next()`：

```java
if (hasUsage)  sink.next(usageChunk);     // ①
if (hasDelta)  sink.next(deltaChunk);     // ②
if (hasFinish) sink.next(finishChunk);    // ③ ← F0 新增
```

**① 与 ② 从未在同一次调用里同时命中** —— 因为普通增量帧没有 `usage`、
用量帧没有 `choices`。也就是说这段代码一直在**依赖上游帧形状的巧合**在运行。

F0 加入 ③ 之后巧合被打破：DeepSeek 的**末帧恰好同时带 `usage` 与
`finish_reason`**（`include_usage=true` 时），①②③ 里有两个同时命中 → 抛异常 → 502。

### 修法

改用 `flatMapIterable`，一次映射出 0~2 个分片。

更重要的动作是**把解析抽成 `OpenAiAdapter.parseChunk(json, model)` 静态方法**
并补上单测（`OpenAiAdapterChunkTest`，11 条）。理由：这个坑埋在 lambda 里
就没法为它写永久的回归断言，而这个坑值得一条 —— 它靠真机跑一次是抓不住的，
必须恰好碰上"同时带 usage 与 finish_reason"的帧才行。

---

## 实现期间踩到的三个坑

### ① Mockito 在 JDK 24 上无法插桩

`ChatPipelineQuotaSettleTest` 一上来 11 条全红，报
`Could not modify all classes [class java.lang.Object, ...]`。

根因不是测试写错了：Spring Boot 3.3.4 管理的 **byte-buddy 1.14.19 不支持 JDK 24**，
而本机编译运行用的是 JDK 24（pom 里 `java.version=17` 只影响字节码目标版本，
不影响 mock 生成时的 JVM 类文件版本）。

修法：pom 覆写 `<byte-buddy.version>1.15.11</byte-buddy.version>`。
仅影响测试期。

### ② 我自己引入的缺陷：结算会把整笔预扣退掉

第一版实现里，"上游没给 usage"的回退只在**级联升级分支**做了，
普通的最终响应仍然直接用 `response.usage()`。而结算是按 `actual - reserved` 补扣的 ——
于是上游不返回 usage 时 `actual = 0`，结算结果是把预扣**全额退掉**，可这次请求明明花了钱。

**这条是单测抓出来的**（`fallsBackToEstimateWhenUpstreamUsageIsMissing`），
真机跑很容易漏掉（要恰好碰上上游不给 usage 的情况）。
修法：抽出 `usageOrEstimate()`，两条路径共用同一份回退逻辑，
并写进注释说明"不知道花了多少 ≠ 没花钱"。

### ③ 依赖容器挂了，症状长得像代码 bug

第一次真机验证时三条配额断言全红（配额恒为 0、结算计数为 0）。
我先去读代码 —— 实际原因是 **Redis 容器在跑脚本前 15 分钟停了**，
而 `QuotaService.tryConsume` 在 Redis 不可用时是 **fail-open**（放行 + 只记日志），
于是配额恒为 0，看起来就像"结算根本没实现"。

修法：脚本开头加**依赖前置检查**，Redis/Postgres 没在跑就直接中止并说明原因。
环境问题必须在开始前与代码问题分开，否则每次都要重新推一遍。
同一批次里还给脚本补了"非 200 时打印原始响应体 + 失败时打印服务日志末尾"——
前面那个流式 502（问题 4）正是靠这两项才看到 `Cannot emit more than one data`
这句真实错误信息的，否则只能看到一个没有上下文的 502。

---

## 实测验证

`python scripts/check-f0-failure-semantics.py` —— **8/8 PASS**。

| 检查 | 结果 |
|---|---|
| 非流式 `finish_reason` 透传 | `finish_reason='length'`、正文 0 字（`max_tokens=64`） |
| 流式收尾帧 | 收尾帧 1 个、`finish_reason='length'`、`delta` 是空对象、`[DONE]` 正常 |
| 配额结算覆盖 completion | **增量 2498 == 真实 usage 2498**（prompt 34 + completion 2464）<br>F0 之前这里只会记 34 |
| 缓存命中全额退还 | 命中前后配额增量 **0** |
| `llmgw.upstream.empty` | 0 → 1 |
| `llmgw.upstream.truncated` | 0 → 1 |
| `llmgw.quota.settled` | 4（无失败） |

---

## 从这一批抽象出的 3 条原则

1. **失败判据要先于失败处理存在。** 熔断、重试、故障转移都建立在
   "这次算不算失败"之上。这个判据没定义清楚就去做熔断，做出来的东西
   在真实最常见的失败上永远不会触发 —— 而且看不出来，因为它平时确实不触发。

2. **"靠巧合运行"的代码必须靠单测钉住，不能靠真机。** 那个 502 的成因是
   "两个分支从未同时命中"，而这件事的真机复现需要恰好碰上特定帧形状。
   真机验证擅长回答"我们对上游行为的假设对不对"，不擅长覆盖帧形状组合。

3. **计量口径要能被外部对账。** 本次验证用的不是"配额度变化了"
   而是"配额增量 == 账本真实 tokens，精确相等"。可对账的数字才能暴露
   "只覆盖了百分之几"这种偏差；"比之前大了"这种模糊断言不能。

---

## 下一步（B 批余项）

- **F1**：重试 + 熔断（Resilience4j）。重试谓词已由
  `UpstreamOutcome.empty()` 给出；算子可直接挂在 P1 铺好的 `Mono`/`Flux` 上。
- **F2**：跨 provider 故障转移。验证方式为同端点配两个 name，
  把第一个的 base-url 指向不可达端口，观察请求是否自动转移。
- **P6**：`TenantService` 抽出来。
- **P7 补**：重试次数、熔断状态、转移次数三项指标。
