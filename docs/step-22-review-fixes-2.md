# step-22 · 复查修复：三处"账本与熔断算错了但不报错"的缺陷

> 前置：F0 ~ F3 全部完成（step-18 ~ step-21）。本轮不是新功能，是把
> 「已知边界」里三条标着"尚未做 / 待办"的缺陷真正修掉，并补上真机验证。
>
> 触发方式：起 Redis/Postgres 后做一次全项目体检 —— 先跑全量单测确认基线，
> 再补跑上轮因容器缺失而没跑的真机断言，最后逐条核对已知边界里哪些是**缺陷**、
> 哪些只是**功能没做**。修的是前者。

---

## 1. 背景：上一轮缺口补齐

F3 交付时 `scripts/check-task-routing.py`（8 条断言）没有实跑 —— 本机 Docker
启动被沙箱安全策略阻止。这次容器起来后补跑：

```
结果：8/8 PASS
  ✓ A1 显式 task=code 走强模型
  ✓ A2 显式 task=summarize 走便宜模型
  ✓ B1 代码围栏被推断成 code
  ✓ B2 普通提问落 default-task（零回归）
  ✓ C1 未知任务 → 400 invalid_request_error
  ✓ D1 model=pro + task=summarize 仍走 pro
  ✓ E1 流式同样走路由结果
  ✓ F1 llmgw.route.task 指标可用
```

其中 **C1 同时断言了两件事**：HTTP 400，且`上游收到=[]`。
只断言状态码不够 —— 如果实现是"发完请求才发现任务名不对"，状态码可能仍是 400，
但钱已经花掉了。

另外修正一条旧记录：`GatewayContextTest` 是 `@SpringBootTest(webEnvironment=NONE)`，
Redis 只读 classpath 里的 Lua、R2DBC/Kafka 惰性连接，所以**它不需要容器**。
此前 README 写的"跑测试前必须先起 docker"对 F2 及以前成立，对当前不成立。

---

## 2. 修复一：客户端错误不该让 provider 熔断

### 现象（可用性事故的雏形）

Resilience4j 默认把每个异常都记成一次失败。而网关的请求是**替客户端发出去的**，
上游回的 400 往往说明"调用方参数写错了"（`max_tokens` 为负、不支持的字段）。
把它记到 provider 的失败率上，链条是：

```
一个客户端连发 9 次参数错误的请求
  → 这家 provider 的滑动窗口被打满
  → 熔断打开
  → 所有租户、所有模型、所有正常请求一起 503
```

也就是说：**一个坏客户端可以凭一己之力把整条模型链路下线**，而且熔断打开后
连"你的参数错了"这个真实原因都拿不到，只剩 503。这是"用健康信号响应客户端错误"
的典型误用。

### 改法

```java
CircuitBreakerConfig.custom()
        …
        .ignoreException(UpstreamResilience::clientError)
        .build());
```

```java
static boolean clientError(Throwable e) {
    if (e instanceof WebClientResponseException w) {
        return w.getStatusCode().is4xxClientError()
                && w.getStatusCode().value() != 429;   // 429 是容量信号，仍要记账
    }
    return false;
}
```

被忽略的异常**既不算成功也不算失败** —— 这正是想要的语义：
"这条链路好不好"和"这次请求合不合法"是两件事。

### 为什么 429 必须留在窗口里

429 虽然也是 4xx，但它说的是"**我现在服务不了你**"，是 provider 侧的容量信号。
持续 429 说明这家确实扛不住，打开熔断把流量交给下一家，正是故障转移该做的事。
所以谓词写成"4xx 且不是 429"，而不是笼统的 `is4xxClientError()`。

### 一条被推翻的测试

`providerWithOpenCircuitIsSkippedWithoutCallingIt` 原来**用 400 去打开熔断器**，
并在注释里写明"顺带暴露了一个真实边界"。修完之后这条测试自然红了 ——
它编码的正是被修掉的行为。

改法不是放宽断言，而是**换一种更干净的前置构造**：直接在 `UpstreamResilience`
上把 A 打到 OPEN，绕过管线的转移逻辑。理由：这个用例要验的是"管线遇到 OPEN 候选会跳过"，
不是"熔断器怎么被打开"（后者由 `UpstreamResilienceTest` 覆盖）。用 5xx 去打开它反而更糟 ——
B 会在打开之前就先接管请求，最后分不清"跳过"和"根本没得跳"。

---

## 3. 修复二：被丢弃的尝试也要记账

### 现象（成本被系统性低估）

一次请求重试 2 次、最终成功，账本会安安静静写出 **1 行**：金额正确、traceId 对得上、
监控曲线毫无异常。唯一的问题是这次请求真实发生了 3 次计费调用。

而这不是理论风险：F0 存在的前提就是"推理型模型的空正文很常见"，
而空正文**恰好是最该被重试的那类失败**。故障期反而是账本最需要准的时候。

### 判据：为什么是"下一次尝试开始"

拿到响应的那一刻还不知道会不会重试，所以只能先 `hold` 住；
等重试算子重新订阅（`Mono.defer` 再次执行）时，才算证明"上一次没有被交付"。
如果最终没有再重试，这条响应会由 `adopt` 认领为最终答案，由正常路径落账 ——
**两条路互斥，不会重复记账**。

三个分支，一个都不能少：

| 场景 | 谁负责 | 结果 |
|---|---|---|
| 空正文 → 重试 → 成功 | `defer` 里的 `settleSuperseded` | 丢弃的那次落 1 行 + 计入配额 |
| 空正文 → 重试耗尽 → 透传给客户端 | `terminal` 里的 `adopt` | 只落 1 行（它是最终答案） |
| 正常成功（没重试） | `defer` 首次执行时 pending 为空 | 只落 1 行 |

第二条是最容易写错的：**最后一次尝试的空响应既是"失败的尝试"，又是"最终交付的响应"**。
记两次就是凭空多出一笔费用。

### 配额同步

`spentTokens` 同时累加被丢弃尝试的用量。此前配额和账本一起少算，
现在两个口径一起修正 —— 不变量 C（每次上游调用一份费用）与 E（每家候选各自结算）
在这一处才真正闭合。

### 流式仍然不补

被丢弃的流式尝试拿不到 `usage`（末帧没到就断了），没有可记的数字。
这条如实留在边界里，没有假装统一。

---

## 4. 修复三：流式缓存命中缺收尾帧

走上游的流在末帧一定带 `finish_reason`（F0 修的透传），而缓存命中只发一个 delta
就结束 —— 客户端拿到的是一条**没有收尾帧**的流。后果不是"少一个字段"：
所有靠累积 `finish_reason` 做判断的实现（答案是否被截断、结构化输出是否完整）
在命中时只能得到 `null`，与"走上游但被 length 截断"无法区分。
而缓存命中恰恰是最该被信任的那条路径。

```java
return settleQuota(apiKey, estimatedTokens, 0)
        .thenMany(Flux.just(
                StreamChunk.delta("cache", hit.answer()),
                StreamChunk.finish("cache", "stop")));
```

真机脚本对这条完全无感（正文对、状态码 200、账本正常），只能靠单测守。

---

## 5. 修复四：级联升级后的计价用错了档位

```java
// 旧：回退到调用方手上的 tier
return usage.total() / 1000.0 * tier.costPer1k();

// 新：按实际服务的模型查候选表
double per1k = candidates.stream()
        .filter(c -> model.equals(c.getModel()))
        .mapToDouble(Candidate::getCostPer1k)
        .findFirst()
        .orElse(tier.costPer1k());
```

升级之后 `model` 已经是强模型，而 `tier` 还是请求开始时选的便宜档。
用旧 tier 兜底会让"因为便宜模型答不好所以换强模型"这条路径按便宜模型记账 ——
恰好是最该算准的请求被算少了。只有在模型不在候选表里（客户端显式指定了一个
未纳入路由的模型）时，才退回调用方给的档位。

只在**没有配 `gateway.pricing`** 时才走这条路，所以影响面小，但方向是错的：
配置缺失应该降级到"同模型的粗略单价"，而不是"别的模型的单价"。

---

## 6. 验证

```
mvn -o clean test → 186 passed / 0 failures / 0 errors / BUILD SUCCESS
```

| 变化 | 数量 |
|---|---|
| 基线（F3） | 175 |
| `UpstreamResilienceTest` 新增 4xx / 429 两条 | +2 |
| `ChatPipelineCostTest`（计价口径） | +3 |
| `ChatPipelineStreamCacheTest`（收尾帧） | +3 |
| `ChatPipelineAttemptLedgerTest`（补账） | +3 |
| **合计** | **186** |

真机：

```
python scripts/check-task-routing.py   → 8/8 PASS（上轮缺口，本次补齐）
```

本轮**没有重跑** `check-failover.py` / `check-resilience.py`：
它们依赖的桩行为没有变，但账本口径变了（中间尝试会多写行），
下次动到这两块时应当连真机一起复跑。这一条写在这里，避免下次误以为已覆盖。

---

## 7. 被推翻的旧结论

| 位置 | 当时的结论 | 现状 |
|---|---|---|
| step-19 / step-20 边界 11、20 | 重试的中间尝试不落账，"每次尝试单独落账"列为待办 | **已修**：判据是"下一次尝试开始"，流式除外 |
| step-20 边界 21 | 4xx 会计入熔断窗口，应配 `ignoreException` | **已修**：4xx（429 除外）不进窗口 |
| step-19 `providerWithOpenCircuitIsSkippedWithoutCallingIt` | 用 400 打开熔断器，并注明"这是已知边界" | **改为直接构造 OPEN**：原写法依赖被修掉的行为 |

---

## 8. 本轮新增的边界

- 被丢弃尝试的账本行 `latency_ms` 记 0：单次尝试没单独计时，这一行的用途是成本。
- 流式缓存命中的 `usage` 仍是 0/0（正确），但不要用它反推上游消耗。
- 流式路径不补中间尝试的账（拿不到 usage）。

---

## 9. 下一步

- R2：`X-Route-Task` / `X-Route-Source` 响应头（exchange attribute，不碰 22 处构造点的 `ChatResponse`）
- R2：`cost_ledger` 加 `task` 列 + migration
- R2：`/admin/route`（当前映射表 + 各任务命中次数）
- 有容器时复跑 `check-failover.py` / `check-resilience.py`，确认账本行数变化符合预期
