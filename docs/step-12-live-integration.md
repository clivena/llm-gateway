# Step 12 · 真机联调：接上真实模型，一次跑通并修掉 6 个问题

> 目标：把 step-11 配好的 DeepSeek 真正跑起来，验证「客户端 → 网关 → 上游模型 → 返回」全链路。
> 结论：**跑通了，而且这一轮逼出 6 个问题——其中 3 个是只有真机联调才会暴露的代码缺陷。**

## 1. 怎么跑（一键联调脚本）

新增 `scripts/run-gateway-smoke.py`，自包含完成：起服务 → 等就绪 → 发 3 次真实请求 → 抓指标 → 关服务。

之所以写成一个脚本而不是分步执行：**服务是常驻进程**，靠跨轮次保持连接不稳定；把「起-测-停」放进一个进程，跑完即退，只留一份 `run-gateway.log`。

```bash
python projects/scripts/run-gateway-smoke.py
```

## 2. 结果：通了

| # | 请求 | 模型 | 耗时 | 结果 |
|---|---|---|---|---|
| 2 | 非流式 | `deepseek-flash` | **3,475 ms** | ✅ "语义缓存是按语义相似度匹配查询，复用等价历史结果的缓存技术。" |
| 3 | 非流式（同问题换强模型） | `deepseek-v4-pro` | **3,298 ms** | ✅ 模型可切换，返回正确 |
| 4 | 流式 SSE | `deepseek-flash` | 首字 **1,127 ms** / 总 1,128 ms，9 个分片 | ✅ 拼接结果 `1 2 3 4 5` |

顺带验证到的能力：

- **traceId 已注入响应头**：`X-Trace-Id: 5e7d4b0dc27d48d9bd77658f0285fce5`
- **Prometheus 指标可导出**：`llmgw_cache_miss_total 3.0`（3 次请求全部未命中，符合 cache 关闭的预期）
- **上游不可用时快速失败**：`Topic llm.cost.events not present in metadata after 500 ms`，只记日志、不抛异常，主链路正常返回

### 延迟的前后对比（这一轮最大的收获）

| | 修复前 | 修复后 |
|---|---|---|
| 非流式 flash | 62,684 ms | **3,475 ms** |
| 非流式 v4-pro | 64,300 ms | **3,298 ms** |
| 流式首字 | 61,052 ms | **1,127 ms** |

**18 倍差距，全在 Kafka 那一处。**

## 3. 修掉的 6 个问题

### 问题 1：端口被环境变量静默劫持（启动直接失败）

```
Web server failed to start. Port 47480 was already in use.
```

`application.yml` 里明明写的是 `server.port: 8080`。原因：宿主环境注入了 `SERVER__PORT=47480`
（WorkBuddy 自身内部服务的端口），Spring Boot 的宽松绑定把**双下划线**也映射成 `server.port`，
**优先级高于配置文件**，所以改 yml 无效。

> 排查：把环境变量按 `PORT/SERVER/SPRING` 关键字打一遍，一眼就看到 `SERVER__PORT = 47480`。
> 修复（双保险）：① 从子进程环境里摘掉它；② 追加命令行参数 `--server.port=8080`（优先级最高）。
>
> 注意：这只在宿主环境里复现；自己开一个干净的 PowerShell 跑不会有这个问题。

### 问题 2：模型名根本不是 `deepseek-chat`

配置里按老习惯写了 `deepseek-chat` / `deepseek-reasoner`，实际这个 key 能用的模型是
**`deepseek-flash`** 和 **`deepseek-v4-pro`**。

> **教训：模型名不要凭记忆写。**先探一次真实列表再填配置：
> ```bash
> curl -s https://api.deepseek.com/v1/models -H "Authorization: Bearer $KEY"
> ```
> 配置里的 `models` 列表和上游真实列表不一致时，`adapterFor()`（网关侧校验）会通过，
> 但请求打到上游才 4xx —— 属于"半路失败"，排查成本更高。

### 问题 3：Reactor 里不能在事件循环线程做阻塞调用

```
block()/blockFirst()/blockLast() are blocking, which is not supported in thread reactor-http-nio-4
```

`ChatController` 是 WebFlux，直接在 **Netty 事件循环线程**上订阅管线；而 `OpenAiAdapter.chat()`
内部用 `.block()` 同步取结果 → Reactor 直接拒绝。

**修复**：所有阻塞调用一律 `subscribeOn(Schedulers.boundedElastic())`。

```java
return Mono.fromCallable(() -> adapter.chat(tier.model(), request))
        .subscribeOn(Schedulers.boundedElastic())   // 离开事件循环线程
        .flatMap(response -> { ... });
```

同一个坑还有两处潜在受害者，一并修了：

- `cacheService.lookup(...)` —— 内部要走 embedding HTTP + Redis block。**缓存一旦开启就会立刻复现**，所以这次一起修。
- `cacheService.write(...)` —— 改成 `Mono.fromRunnable(...).subscribeOn(boundedElastic()).subscribe()` 的 fire-and-forget。

### 问题 4：Reactor 的 `map` 不允许返回 null（流式直接报错）

```
data: {"error":"The mapper [com.llmgw.core.protocol.OpenAiAdapter$$Lambda/...] returned a null value."}
```

原代码：

```java
.map(ServerSentEvent::data).filter(Objects::nonNull)   // ❌ filter 在 map 之后，救不回来
.map(this::extractDelta).filter(Objects::nonNull)      // ❌ 同理
```

SSE 的第一个分片往往只有 `delta.role` 没有 `content`，`extractDelta` 返回 null，
而 Reactor 的 `map` 遇到 null **直接抛异常**（后面的 `filter` 根本轮不到执行）。

**修复**：改用 `handle`，显式过滤掉空值。

```java
.handle((ServerSentEvent<String> sse, SynchronousSink<String> sink) -> {
    String data = sse.data();
    if (data != null) { sink.next(data); }        // ping/注释事件 data 为 null，丢弃
})
.takeUntil("[DONE]"::equals)
.filter(d -> !"[DONE]".equals(d))
.handle((String json, SynchronousSink<String> sink) -> {
    String delta = extractDelta(json);
    if (delta != null && !delta.isEmpty()) { sink.next(delta); }
});
```

顺手还把 `extractDelta` 里**每个分片 new 一个 ObjectMapper** 改成了静态常量复用。

### 问题 5：Kafka 不可达时 `send()` 阻塞 60 秒，把响应一起拖死（最严重）

现象很有辨识度：**三个请求耗时几乎一模一样（62.7s / 64.3s / 61.1s）**——固定超时的特征。

根因：`KafkaTemplate.send()` 并不是"立即返回"的。broker 不可达时它会阻塞到
**`max.block.ms`（默认 60000ms）**才抛 `TimeoutException`。而落账代码写在 `doOnNext` 里，
位于响应写回之前 → 用户白等 60 秒。

`CostLedgerService` 的注释写着"Kafka 故障不能影响主链路，所以异常只记日志、不抛"——
**这个意图是对的，但没实现**：它只防住了"抛异常"，没防住"阻塞"。

**修复（双保险）**：

```yaml
# application.yml
spring:
  kafka:
    producer:
      properties:
        max.block.ms: 500      # 不可用时快速失败，而不是死等 60s
```

```java
/** 落账必须在响应路径之外执行。 */
private void recordAsync(String apiKey, ChatRequest request, ChatResponse response, double cost, long latencyMs) {
    Mono.fromRunnable(() -> ledger.record(...))
            .subscribeOn(Schedulers.boundedElastic())   // 独立线程 fire-and-forget
            .subscribe(null, e -> log.debug("cost ledger failed: {}", e.getMessage()));
}
```

修复后日志变成 `Topic llm.cost.events not present in metadata after 500 ms.`——**500ms 内失败，主链路 3.4s 正常返回**。

> 这条是整轮联调最有价值的发现：**"异步副作用"必须同时满足"不抛异常"和"不阻塞"两个条件**。
> 只处理前者，线上会变成"功能都对，就是慢得莫名其妙"。

### 问题 6：流式分支漏计指标

指标端点显示 `llmgw_cache_miss_total 2.0`，但实际发了 3 次请求。
对比代码发现：`execute()` 里有 `metrics.onCacheMiss()`，而 `executeStream()` 的
`switchIfEmpty` 分支漏了这一行。修复后指标变 `3.0`，与请求数一致。

> 指标漏计比指标不准更危险——它会让所有"命中率/成本"结论系统性偏低。

## 4. 关键代码

见上文各问题的修复片段。完整的落账解耦与线程模型说明写在
`ChatPipeline` 的类注释里（以后接手的人第一眼就能看到这两个约束）。

## 5. 验证方式

`scripts/run-gateway-smoke.py` 一次跑完全部断言：

- 服务在 ~33s 内监听 8080（含 Maven 编译）
- 3 次真实请求全部 HTTP 200 且返回非空答案
- 流式 9 个分片、拼接结果正确
- Prometheus 指标条数与请求数一致
- 服务能被干净地关掉、端口释放

## 6. 坑（可直接当面试素材）

- **固定耗时 = 固定超时**。看到 N 次请求耗时高度一致，不要去猜网络波动，去找超时配置。
- **`max.block.ms` 是 Kafka 生产者的隐形地雷**，任何"异步发送"都要显式压小它。
- **Reactor 的两个铁律**：阻塞调用必须换线程；`map` 不允许返回 null。
- **测试脚本本身也会骗人**：这次我自己的就绪判定错了两次（把 503 当没起来、被宿主代理的 502 骗到），
  所以用**原生 socket 判端口**比用 HTTP 判健康更可靠。
- **配置里的模型名/端点不要凭记忆写**，先请求一次真实列表。

## 7. 已知边界与下一步

| 项 | 状态 |
|---|---|
| traceId 贯通到成本事件 | ❌ 账本事件里 `traceId: null`。响应头有了，但没传进落账上下文，需要 Reactor Context 传播（`contextWrite` + `deferContextual`） |
| 真实成本数字 | ❌ 显式模型走 EXPLICIT 层，`costPer1k=0` → 账本金额恒为 0，需把 `RouterService` 配置化 |
| 多供应商 | ❌ `OpenAiAdapter` 仍是单例，只读第一个 provider |
| 语义缓存 | ❌ DeepSeek 无 `/embeddings`，需换有向量能力的厂商或加第二个适配器 |
| Redis 路径 | ⚠️ 本次未启动 Redis，配额走 fail-open、账本只落日志；**Redis 的 Lua 原子扣减尚未真机验证** |

**下一步优先级**：① 起 Redis（`docker compose up -d redis`）把配额路径验证掉 →
② 把 `RouterService` 配置化拿到真实成本 → ③ traceId 打通成本事件。
