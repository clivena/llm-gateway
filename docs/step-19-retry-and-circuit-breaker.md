# step-19 · 重试与熔断（F1）

> 批次：B 批第三项（F0 之后）。提交前测试数 104 → **124**。
> 真机验证脚本：[`scripts/check-resilience.py`](../scripts/check-resilience.py)

## 目标

让"上游这一次调用失败"从**直接把错误甩给客户端**，变成三件事：

1. **先重试** —— 处理瞬时抖动；
2. **再熔断** —— 处理持续故障，而且熔断的真正价值是**不再花钱**；
3. **失败也要结算配额** —— 这一条不在原始计划里，是前两条逼出来的（见下）。

F1 只负责**单个 provider 内部**的韧性。跨 provider 的转移是 F2。

## 一、为什么重试与熔断必须成对出现

它们回答的是两个不同的问题，而且**错的方向相反**：

| | 重试 | 熔断 |
|---|---|---|
| 问题 | 这一次失败，立刻再试一次会不会好？ | 这条链路现在还值得试吗？ |
| 针对 | 瞬时抖动 | 持续故障 |
| 与钱的关系 | 每次重试**都是一次真实计费的上游调用** | 价值恰恰在于**不再发请求** |
| 缺失的后果 | 一次抖动就报错 | 故障期自己给自己制造 N 倍负载，把恢复期拖长 |

只有重试没有熔断，等于在上游整体挂掉的时候把一个失败乘以 `maxAttempts`；
只有熔断没有重试，则是把瞬时抖动当故障处理。

## 二、算子嵌套顺序：熔断在内、重试在外

```
call.timeout(…)                                    ← 最内：超时也是失败，要进熔断窗口
     .transformDeferred(CircuitBreakerOperator.of(cb))
     .transformDeferred(RetryOperator.of(retry))     ← 重试在外
```

这个顺序有两处"反过来就错"：

1. **熔断必须能看到每一次尝试。** 重试在外层，每次重试都是重新订阅内层，
   熔断器因此每次都拿到新的 Subscriber、每次都记一笔 —— "3 次尝试 2 次失败"
   才是滑动窗口该看到的数据。若反过来，一次逻辑请求内部重试几次都只记 1 笔，
   失败率被系统性稀释，熔断器**永远到不了阈值**。
2. **超时必须在内层。** 放外层的话，一次 3 次重试的请求总耗时可以到
   `3 × timeout`，而"单次调用超时"和"整个请求超时"是两件事。

用 `transformDeferred` 而不是 `transform`：后者的算子在**装配期**就装好、
被所有订阅者共享；而重试的每一次必须是独立订阅，否则
`CircuitBreakerSubscriber` 会被复用、状态直接错乱。

### 一个反直觉的后果（已用单测钉住）

因为熔断在内层、每次尝试都记一笔，**熔断触发的速度变成 `maxAttempts` 倍**：

```
maxAttempts=3 + minimumNumberOfCalls=8  →  一个失败的请求就贡献 3 次失败
                                            ≈ 3 个请求就能打开熔断器
```

真机日志里能直接看到这一幕：

```
16:05:07.669 WARN circuit breaker state transition [provider=deepseek] CLOSED -> OPEN
16:05:07.669 WARN retrying upstream call [provider=deepseek attempt=1]: HTTP 500
16:05:07.986 WARN circuit breaker open, request not sent upstream [provider=deepseek]
```

熔断在这一刻打开，紧跟着的那次重试**立刻**被拒。这是有意的，不是 bug，
但它是那种"会被误认为 bug 的现象"——所以
`UpstreamResilienceTest.everyRetryAttemptCountsTowardTheCircuitWindow`
专门把这个耦合写成了断言，免得将来有人看到"刚起服务打一个请求就熔断"
跑去改阈值。

## 三、把"空正文"送进重试算子：一次必须成对的翻译

F0 建立的判定是：`finish_reason=length` + 空正文 = 上游失败了（推理型模型把
`max_tokens` 预算花在 reasoning 上）。但**这个失败在传输层是一次彻底的成功** ——
HTTP 200，不会有任何 onError。

而重试算子只认错误信号。所以必须来回翻译一次：

```java
Mono<ChatResponse> attempt = Mono.defer(() -> adapter.chat(model, request))
        .map(resp -> {
            if (UpstreamOutcome.of(resp.content(), resp.finishReason()).empty()) {
                throw new UpstreamEmptyResponseException(resp);   // 进重试边界
            }
            return resp;
        });
return resilience.protect(adapter.name(), attempt)
        .onErrorResume(UpstreamEmptyResponseException.class, e -> Mono.just(e.response()));  // 出重试边界
```

**末尾那一步不是可选的。** 少了它，客户端会从
`200 + finish_reason=length + 空正文` 退化成 `502` ——
那是 F0 修掉的错误的**镜像版本**：这次是把"上游明确说明了原因"伪装成"网关坏了"。

### 为什么用 `Mono.defer` 包住 `adapter.chat`

重试的语义是"重新订阅同一条 Publisher"。当前 `OpenAiAdapter` 返回的是冷流，
每次订阅真的会重新出网 —— 但**"冷"是实现的细节，不是 `ProviderAdapter`
接口的契约**。万一将来有人写出一个复用/缓存内部 Publisher 的适配器，
重试就退化成"把上一次的旧响应再吐一遍"：看起来重试成功了，
实际上一次请求都没发出去，而且不报错。`defer` 把这条契约写进代码里。

## 四、重试谓词：白名单，不是黑名单

判断标准只有一条：**再试一次的结果是否可能不同**。与错误严重不严重无关。

| 错误 | 重试？ | 理由 |
|---|---|---|
| 空正文（`UpstreamEmptyResponseException`） | ✅ | 推理 token 数高度不确定（实测同一问题在 512/1024/1935/1740 之间跳），重跑一次很可能出正文 |
| 空响应体（`UpstreamEmptyBodyException`） | ✅ | 瞬时故障，且没有任何内容已交给客户端 |
| 5xx | ✅ | 上游自己出问题了 |
| **429** | ✅ | 唯一一个"重试就是正确响应"的 4xx —— 它不是请求错了，是"现在别来" |
| 连接被拒 / DNS / TLS（`WebClientRequestException`） | ✅ | 典型瞬时故障 |
| 超时 | ✅ | |
| 400 / 401 / 404 等 4xx | ❌ | 确定性的。重试只会原样再拿一个回来，还多烧一次配额 |
| 熔断打开（`CallNotPermittedException`） | ❌ | 重试也进不去熔断器，只会空转 |
| 未知异常 | ❌ | 见下 |

**为什么是白名单**：黑名单（"除了这些以外都重试"）在遇到一个新的、
没见过的异常类型时会**默认去重试它** —— 而"看不懂的错误"恰恰最不该重试：
重试只会把它放大 N 倍，连同 N 倍的账单。白名单遇到未知异常默认不重试，
错也只是少救一次，而不是多花一次钱。

### `failAfterMaxAttempts(false)` 是显式写的

这是 `RetryConfig` 的默认值，但**必须写出来**：改成 `true` 会让重试耗尽后
抛出 `MaxRetriesExceededException` 而不是原始异常 —— 上层就再也拿不到
`UpstreamEmptyResponseException` 身上那份原始响应，上面那条翻译链被静默破坏。

## 五、流式：重试只能发生在第一个字出去之前

重试 = 重新订阅上游、从头再放一遍。非流式没问题（客户端什么都还没拿到），
但流式一旦已经推出去几个字，重放会让客户端看到**两段拼接的答案**：
前一次成功的半截 + 这一次完整的一遍。这种损坏不可撤销，
而且看起来像模型在胡说，排查时完全指不到重试头上。

实现上是把标记挂在**重试边界之外**：

```java
decorated.doOnNext(x -> upstreamProducedSomething.set(true))   // ← 在 RetryOperator 之后
```

位置关键。放前面的话，一次"吐了 3 个分片再报错"的尝试也会把标记置上 ——
而那 3 个分片可能已经被随后的重试判定为丢弃，客户端其实什么都没看到。

闸门的另一端是超时：`gateway.resilience.first-chunk-timeout-millis`
映射到 `Flux.timeout(Duration)` —— 这个方法的作用域**就是首元素**，
后续元素不再受约束。参数语义与算子语义天然对齐，不需要自己写计时器。

### 两处刻意的不对称

| 判断 | 依据 | 原因 |
|---|---|---|
| 能不能重试 | `upstreamProduced`（任何帧都算） | 保守：只要有过任何一帧流向下游，就不敢重放。usage 帧在流末尾，几乎不会撞上 |
| 能不能切换模型（级联升级） | `emitted`（只有正文帧算） | 只有正文帧才会让客户端看到"两段拼接的答案"，usage 帧不会 |

## 六、失败路径的配额结算（不变量 D）—— 本批顺带修掉的真实漏洞

这一条**不在原始计划里**。是熔断引入的新失败模式逼出来的。

### 问题

`settle` 只在成功路径被调用。于是一次都没调通上游的请求，预扣那笔就
**永远留在账上**：

```
上游 502     → 请求已送到、prompt 大概率已计费 → 扣得不冤
熔断打开     → 请求一个字节都没发出去           → 白扣
连不上       → 到不了模型                       → 白扣
```

平时流量小看不出来，但**熔断恰恰是"连续失败"触发的** ——
上游整体挂掉的那段时间，每个被拒的请求都白吃一整笔配额。
F1 引入的新失败模式反而成了最大的配额泄漏点。

### 取舍规则：只在"确定没花钱"时才退还

两个方向的错不是一样重：

- 少退一次 = 用户多扣了一点额度（可事后补偿）
- 多退一次 = 故障期配额上限失效（不可逆）

所以默认取保守方向，只有能**确定**请求没到上游时才退还：

```java
private static long chargedOnFailure(Throwable e, long knownUsage, long reserved) {
    if (knownUsage > 0) return knownUsage;                  // 已有真实用量 → 用真值
    if (neverReachedUpstream(e)) return 0;                  // 确定没到上游 → 全额退还
    return reserved;                                         // 其余 → 保守保留预扣
}
```

`neverReachedUpstream` 只列两种能"确定"的：

- `UpstreamCircuitOpenException` —— 请求在网关内部就被拦下了，TCP 连接都没建；
- `WebClientRequestException` —— 连接被拒 / DNS 失败 / TLS 握手失败。

`TimeoutException` **不在此列**：请求已经发出去了，上游可能正在算。

### 流式用另一套口径

流式的真实用量只在末帧给出，流一旦断掉就永远等不到那一帧 ——
所以"已经花了多少"只能靠"有没有分片出来过"来推断：

```java
long known = upstreamUsage.get() == null ? 0 : upstreamUsage.get().total();
// upstreamProduced 为 true 时走到 chargedOnFailure 的第三行 → 保留预扣
```

## 七、验证

### 单测：104 → 124（+20）

| 新增 | 数量 | 守的是什么 |
|---|---|---|
| `UpstreamResilienceTest` | 10 | 重试谓词的每一格、重试次数、熔断状态机、**熔断后上游调用数不增长**、流式闸门两个方向、首片超时的作用域 |
| `ChatPipelineResilienceTest` | 6 | 空正文重试后**原始响应被还原**、`budgetTooSmall` 不重试、熔断退还 / 5xx 保留、升级后失败用第一次真实用量 |
| `OpenAiProtocolTest` 补 | 3 | 熔断 → 503 + `circuit_open`；空正文与熔断不能混成同一状态码；上游非 2xx → 502 |

其中两条是**反向断言**，价值高于正向：

- `truncatedButNonEmptyAnswerIsNotRetried` —— 有正文 + `length` 是客户端
  `max_tokens` 设小了，不该重试。它和"空正文该重试"只差一格，
  而那正是 F0 四分表里最容易写反的地方。
- `aStreamThatStartsInTimeIsNotAborted` —— 防止把首片超时配成"总是触发"。

### 真机 / 桩：11 项断言全 PASS

脚本要打**两段**上游，因为两类问题只有不同的上游能回答：

- **A 段（真实 DeepSeek）**：重试在真实失败上到底会不会触发 ——
  这取决于上游的实际行为（推理型模型在小 `max_tokens` 下必然返回空正文），桩模拟不来。
- **B 段（本地桩）**：重试了几次、熔断后有没有再发请求 ——
  这类**精确计数**必须由能计数的上游回答，而且故障要能按需制造。

| # | 断言 | 实测 |
|---|---|---|
| A1 | 空正文触发重试，且重试耗尽后仍是 200 + `length` | 重试增量 **2**，`finish_reason='length'`，HTTP 200 |
| A2 | 正常答案不被重试（对照） | 重试增量 **0** |
| A3 | `/admin/circuit` 可读，健康时 CLOSED | `state=CLOSED`，buffered=4 failed=3 successful=1 |
| B0 | 桩 base-url 覆盖生效（预检） | 桩收到 1 次 |
| B1 | **重试真的重新发起了请求** | 桩设定"前 2 次 500"→ 桩收到 **3** 次，拿到成功响应 |
| B2 | 重试耗尽如实报 502 | HTTP **502**，`type=upstream_error`，桩收到 3 次 |
| B3 | 熔断在连续失败后打开 | 1 轮后 `state=OPEN` |
| B4 | **熔断后不再发请求** | HTTP **503** + `circuit_open`，桩请求数 **4 → 4 未变** |
| B5 | 熔断可自动恢复 | `HALF_OPEN` → 3 次成功 → `CLOSED` |
| B6 | **流式首片前重试不产生重复内容** | 桩收到 **2** 次流式请求，客户端正文 = 期望值（无重复） |
| B7 | F1 三个指标带 provider 标签 | retry=6 rejected=2 open=0 |

### B1 与 B4 是全套里最强的两条

- **B1**：只看向应码的话，一个"把旧 Mono 缓存起来"的实现也能通过 ——
  它返回 200、正文正确、日志也打了"retrying"。只有**上游自己的计数**
  能证明那 3 次请求真的发生了。
- **B4**：熔断状态显示 OPEN、`circuit.rejected` 涨了，都不等于上游真的被放过了。
  唯一能证明的是"熔断之后桩收到的请求数没有增加"。

### 桩的隔离方式：不落任何文件

桩地址经 `SPRING_APPLICATION_JSON` 注入（它的优先级高于配置文件，
且会**整体替换** `providers` 列表）——**不修改 `config/application-local.yml`**。
理由不是洁癖：那个文件是 gitignored 的私有配置，含真实密钥；
脚本一旦中途被打断，改回不去就等于把开发者的 base-url 永久留在桩地址上，
而症状是"网关突然连不上了"，跟脚本已经没关系了。

脚本还带一个**预检**（B0）：先发一个请求、核对桩自己的计数 > 0。
没有它的话，覆盖一旦失效，B 段所有断言都会"看起来通过"（请求打到了真实上游，
一切正常），而实际上一次都没经过桩 —— 那是最坏的一种假绿。

## 八、踩到的坑

### 1. `WebClientException` 没被 `statusOf` 覆盖 → 上游任何非 2xx 都报 500

真机 B2 第一次跑出来的是：

```
HTTP=500  type='upstream_error'
{"error":{"message":"500 Internal Server Error from POST http://127.0.0.1:18765/v1/chat/completions"}}
```

`WebClientResponseException` 继承 `NestedRuntimeException`，
既不是 `IllegalStateException` 也不是 `IllegalArgumentException`，
于是穿过所有分支掉进最后的 `→ 500`。客户端看到的是"网关自己崩了"，
而问题在下游 —— 排查方向从一开始就是错的。

**这个缺陷 F1 之前就存在**，只是网关从不重试、上游一次 500 就直接穿过去，
从没人核对过状态码。重试让"重试耗尽"变成一条必然走到的路径之后，
它才被真机断言抓出来。修法是把 `WebClientException` 整类映射到 502。

> 教训：**新的失败路径会把旧代码里从没被认真走过的分支变主路径。**
> 加一条重试，等于给所有"上游错误"分支都加了一次曝光。

### 2. Mockito 的 `anyString()` 不匹配 null

`ChatPipelineQuotaSettleTest` 里 `resilience.protect(provider, ...)` 的桩没生效，
8 条断言全红，报的是一个看不懂的 NPE：

```
Cannot invoke "Mono.onErrorResume(...)" because the return value of
"UpstreamResilience.protect(String, Mono)" is null
```

根因：管线用 `adapter.name()` 做熔断器实例键，而那个 mock 没桩 `name()` →
返回 `null` → **`anyString()` 不匹配 null** → 桩未生效 → 走默认返回 `null`。

> 教训：**`anyString()` / `anyLong()` 之类都不匹配 null。**
> 一个方法返回值参与后续控制流时，mock 里漏桩它会以"另一个地方的 NPE"的形式出现。

### 3. 重试计数：桩的 `thenAnswer` 数的是"调用"不是"订阅"

`ChatPipelineResilienceTest` 里 `emptyAnswerIsRetried...` 一开始断言
"调用 3 次，实际 1 次"。因为 Mockito 的 `thenAnswer` 在 `adapter.chat(...)`
被调用时执行一次，之后重试是**重新订阅同一个 Mono**，不会再走 `thenAnswer`。

这恰好反过来印证了第七节 B1 的必要性：**代码里"重试了 3 次"和
"上游收到了 3 次请求"是两件事**，前者看日志、后者要上游计数。
生产路径上这两者一致（`OpenAiAdapter` 是冷流），但一致是**契约**，
现在是靠 `Mono.defer` 保证的。

## 九、已知边界（本批新增）

1. **重试的中间几次尝试不计入账本。** 账本只为"最终交给客户端的那一次"
   落一条（不变量 C 的口径）。一次重试 2 次的请求 = 3 次真实计费，
   账本记 1 次 —— 成本归因在故障期系统性偏低，而故障期恰恰是重试最多的时候。
   配额侧是准的（`spentTokens` 走的是最后那次响应）。**留作下一批的待办**：
   每次尝试单独落账。
2. **上游 429 不透传为 429。** 上游限流被统一归到 502。
   不透传是因为我们自己的 429 有明确含义（本租户配额用尽），
   混进上游的限流会让客户端分不清该找谁。
3. **不读 `Retry-After`。** 429 的退避走统一指数退避；
   上游若给了很长的 `Retry-After`，我们会在它说的窗口之前就重试。
4. **重试/熔断参数是全局一份，没有 per-provider 覆盖。**
   当前只部署 1 家 provider，"哪家更爱抖"在跑够数据之前说不清。
5. **上游非 2xx 一律 502，不透传原始状态码。**
   上游 400（比如模型名上游已下线）会报成 502 而不是 400。
   网关能自己识别的调用方错误（`messages` 为空、模型不在服务范围）
   在进上游之前就拦掉了，所以上游 4xx 绝大多数确实是配置/上游问题。
6. **流的"中途卡住"没有防护。** `Flux.timeout()` 只约束首片；
   首片之后如果上游停止发送但不断开，连接会一直挂着。
   要覆盖它需要按分片做空闲超时，属于另一个改动。

## 十、下一步（B 批余项）

- **F2**：跨 provider 故障转移。`UpstreamResilience.stateOf(provider)`
  已经能按 provider 查熔断状态，正是 F2 挑下一个候选需要的形状。
  验证方式：同端点配两个 name，把第一个的 base-url 指向不可达端口。
- **P6**：`TenantService` 抽出来（apiKey 同时是身份、凭证、日志字段）。
- **P7 补**：重试次数 / 熔断状态 / 转移次数——本批已经补了前两项。
- **待办（本批新增）**：重试的每次尝试单独落账。
