# Step 15 · 全量复盘：复查中发现并修掉的 5 个缺陷

> 触发原因：在写面试复盘材料时，把两个项目的源码逐行过了一遍，
> 重点追问一个问题——**"这段代码在真实条件下会怎样"**。
>
> 结果在网关侧找出 5 个之前没记录在案的缺陷（其中 2 个是严重级别）。
> 本文记录**现象 → 根因 → 修法 → 回归测试**。评测侧的 6 个缺陷见 `llm-eval/docs/step-10-review-fixes.md`。

---

## 汇总

| # | 缺陷 | 严重度 | 回归测试 |
|---|---|---|---|
| 1 | 配额超额返回 HTTP 200 而非 429 | 🔴 | `ChatControllerStatusTest`（4 条） |
| 2 | 缓存命中分支的落账在响应路径上同步执行 | 🔴 | 约束写入 `ChatPipeline` 类注释 |
| 3 | 级联升级的成本只记一次（实际发生两次调用） | 🟠 | — |
| 4 | 缓存 namespace 不含模型维度 | 🟠 | `ChatPipelineCacheNamespaceTest`（3 条） |
| 5 | `schema.sql` 里的预留表无任何标注 | 🟡 | — |

**单测数量**：3 → **10**（`mvn test` 全绿，BUILD SUCCESS，编译 24 个源文件）。

---

## 缺陷 1 · 配额超额返回 HTTP 200

### 现象

配额超额的请求，客户端拿到的是 **HTTP 200 + `{"error":"quota exceeded"}`**。

### 根因

`ChatController` 先把状态码设成 200，之后在 `onErrorResume` 里只写响应体、不改状态码：

```java
response.setStatusCode(HttpStatus.OK);          // ← 先设成 200
return pipeline.execute(key, request, traceId)
        .flatMap(...)
        .onErrorResume(e -> response.writeWith(...));   // ← 只写 body
```

### 为什么要修（不是"不够规范"，是实际有害）

1. **客户端无法做退避决策**。OpenAI 兼容协议里限流就是 429，
   调用方的重试库（tenacity / resilience4j）按 429 + `Retry-After` 工作；
2. **监控看不到**。按状态码统计错误率时，限流根本不会出现在错误率里——
   等到用户投诉才会发现。

### 修法

把异常类型和状态码对应起来，并统一错误体为 OpenAI 形状：

```java
static HttpStatus statusOf(Throwable e) {
    if (e instanceof QuotaExceededException) return HttpStatus.TOO_MANY_REQUESTS; // 429
    if (e instanceof IllegalStateException)  return HttpStatus.BAD_GATEWAY;       // 502
    return HttpStatus.INTERNAL_SERVER_ERROR;                                      // 500
}
```

新增 `core/quota/QuotaExceededException`（message 保持 `quota exceeded` 不变，
因为 `scripts/verify-quota-ledger.py` 靠这个字符串识别"被配额拦下"的请求）。

**顺带修掉一个隐藏炸弹**：流式路径上，如果响应已经 flush 过分片，
`setStatusCode` 会抛 `IllegalStateException("Cannot set status code after response has been committed")`，
而这个异常发生在 `onErrorResume` 内部，会把"优雅报错"变成"流莫名其妙断掉"。

```java
private static void setStatusIfUncommitted(ServerHttpResponse response, HttpStatus status) {
    try {
        response.setStatusCode(status);
    } catch (IllegalStateException alreadyCommitted) {
        // 已提交：状态码改不动了，错误改由 SSE error 帧传递
    }
}
```

---

## 缺陷 2 · 缓存命中分支的落账在响应路径上

### 根因

同一个类里两条分支的处理方式不一致：

```java
// 未命中 → 异步（正确）
.doOnNext(response -> {
    recordAsync(traceId, apiKey, request, response, cost, latency);
})

// 命中 → 同步（错误）
.flatMap(hit -> {
    metrics.onCacheHit();
    ledger.record(traceId, apiKey, request.feature(), "cache", true, 0, 0, 0.0, latency);  // ← 同步
    ...
})
```

### 为什么特别讽刺

- 缓存命中是**整个系统最快的路径**（应该几毫秒）；
- 而 step-12 那个 62 秒的坑，正是本项目最有价值的性能修复；
- 结果那条修复**只覆盖了未命中分支**，命中路径上留着同一个坑。

### 修法

命中分支统一走 `recordAsync`，并把这条约束**提升成类注释里的不变量**：

```java
/**
 * 三条不变量（都是被线上问题换回来的，改代码时别破坏）：
 *   A. 落账只能走 recordAsync —— 任何一次同步 ledger.record 都可能把响应拖住。
 *   B. 缓存 namespace 必须带模型维度 —— 否则强模型的答案会被弱模型请求命中。
 *   C. 每一次真实发生的上游调用都要落一条账 —— 级联升级时是两次调用、两份费用。
 */
```

---

## 缺陷 3 · 级联升级的成本只记一次

### 根因

```java
if (routerService.shouldEscalate(tier, response.content(), formatValid)) {
    // 便宜模型这次调用已经发生、已经计费……但这里什么都没记
    ModelTier stronger = routerService.escalateFrom(tier);
    ChatResponse upgraded = adapterFor(stronger.model()).chat(stronger.model(), request);
    return Mono.just(new ChatResponse(upgraded.id(), stronger.model(), ...));
}
// 只有最终这一次被 doOnNext 里的 recordAsync 记下来
```

被升级请求实际发生了**两次上游调用、两份费用**，但账本只落一行。
直接违背 `DESIGN.md` 的验收指标"成本报表与厂商账单误差 < 1%"。

### 修法

升级前先把便宜模型这次调用落一条账：

```java
Usage firstUsage = adapter.estimateUsage(tier.model(), request, response.content());
recordAsync(traceId, apiKey, request,
        new ChatResponse(response.id(), tier.model(), response.content(), firstUsage, false),
        estimateCost(tier.model(), tier, firstUsage),
        System.currentTimeMillis() - start);
```

同一个 `traceId` 下两行，对账时按 traceId 汇总即可看到"这次请求实际花了两次钱"。

### 流式的情况（刻意不做）

流式升级是由上游**报错**触发的，那一次调用的 token 用量上游根本没有返回。
**无法可信统计时不伪造账本行**——这是明确记录的已知边界，不是漏掉。

---

## 缺陷 4 · 缓存 namespace 不含模型维度

### 根因

缓存 key 是 `sem:{apiKey}`，**只有租户维度**。
而 `DESIGN.md` 写的是 `sem:{model}:{hash}`——**实现和设计不一致**。

### 后果（隐蔽，值得细讲）

```
① 租户 A 用 gpt-4o（贵、质量高）问"什么是语义缓存" → 写入 sem:tenantA
② 租户 A 用 gpt-4o-mini（便宜）问"语义缓存是什么" → 命中 ① 的答案
```

- 你以为在用便宜模型省钱，实际复用了贵模型算出来的答案；
- **路由策略被缓存完全架空**；
- **没有任何指标能看出来**——命中率还涨了，看起来是好事。

### 修法

```java
/** 缓存 namespace 必须带模型维度。 */
static String cacheNamespace(String apiKey, String model) {
    return apiKey + ":" + model;
}
```

两个细节：

1. **用"实际服务的模型"而不是"请求档位"来划段**——
   否则升级后的高质量答案会被写进便宜模型的段里、被后续低价请求复用；
2. **档位要提前解析**：缓存查询发生在路由之前，所以 `resolveTier()` 提到配额之前
   （`RouterService.pick` 是纯函数、不做 IO，安全）。

---

## 缺陷 5 · `schema.sql` 里的预留表没有标注

`quota_ledger` 表**没有任何代码读写**（配额完全在 Redis）。
`model_route` 表也没被读（`RouterService` 用硬编码候选列表）。
表建了却找不到写入点，会误导后来者。

**修法**：在两处都补上明确注释——哪些表已启用、哪些是预留、预留的原因是什么。
（不删表：保留是为了让 schema 与 `DESIGN.md` 的数据模型一致，
接上后再启用。删掉反而会让人以为设计里没有这块。）

---

## 验证

```
[INFO] Compiling 24 source files with javac [debug parameters release 17] to target\classes
[INFO] Compiling 3 source files with javac [debug parameters release 17] to target\test-classes
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0 -- in com.llmgw.api.ChatControllerStatusTest
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 -- in com.llmgw.core.pipeline.ChatPipelineCacheNamespaceTest
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 -- in com.llmgw.core.router.RouterServiceTest
[INFO] Tests run: 10, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

---

## 从这批缺陷里抽象出的 3 条原则

1. **修一类问题，不要只修你看到的那一处。**
   Kafka 阻塞的修复漏掉了缓存命中分支。正确做法是发现问题后先 `grep` 所有同类调用点，
   再把约束写成**类注释里的不变量**。

2. **要防御的不只是"指标不好"，还有"指标看起来太好"。**
   缓存 key 少一个维度不会报错，只会让路由策略静默失效，而且命中率还上升了。

3. **错误状态码不是给人看的，是给程序和监控看的。**
   一个所有人都看不见的错误，等于没报。

---

## 下一步

- [ ] 网关侧补 GitHub Actions 流水线（编译 + 单测）
- [ ] `OpenAiAdapter` 改成按 provider 列表构建多个 bean（多供应商）
- [ ] `RouterService` 改为读 `model_route` 表
- [ ] 配额按上游真实 usage 补扣差额
- [ ] 日终对账任务：`cost_ledger` 汇总 vs 供应商账单
