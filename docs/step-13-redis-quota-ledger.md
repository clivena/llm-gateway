# Step 13 · Redis 配额实测 + 成本账本真正落库

> 对应命令：`python projects/scripts/verify-quota-ledger.py`
> 产物：`projects/run-quota-ledger.log`

## 目标

前面 12 步留下的三个"没闭环"的点，这一步一次性收掉：

1. **Redis 路径没真机验证过** —— 配额走的是 fail-open，Lua 原子扣减从没实测；
2. **成本账本金额恒为 0** —— `RouterService` 的 EXPLICIT 层 `costPer1k=0`，落库也全是 0；
3. **traceId 没贯通到账本** —— 响应头有 `X-Trace-Id`，但落库字段是 `null`。

第 3 条不只是"不够好"，而是**落库的硬前置**：`cost_ledger.trace_id` 是 `NOT NULL`，
传 `null` 会直接 INSERT 失败。所以 traceId 贯通必须先做。

## 改动

| 文件 | 改动 |
|---|---|
| `docker-compose.yml` | Postgres 宿主端口 `5432 → 55432`（原因见"坑 2"）<br>⚠️ **后续修订（2026-10）**：`55432` 落在 Windows 动态端口排除区间 `55359-55458` 内，docker 绑不上，最终改为 `15432`。见 README「踩过的坑」第 2 条 |
| `pom.xml` | 加 `spring-boot-starter-data-r2dbc` + `r2dbc-postgresql` |
| `application.yml` | 加 `spring.r2dbc.*`；加 `gateway.cost.sink`（`postgres`/`kafka`/`both`）；加 `gateway.pricing.*` 单价表 |
| `config/application-local.yml` | `cost.sink: postgres`、配额上限注释 |
| `config/GatewayProperties.java` | 新增 `Cost`（sink 开关 + `toPostgres()/toKafka()`）与 `Price`（**输入输出分开计价**） |
| `core/cost/CostLedgerRepository.java` | **新增**，R2DBC 写 `cost_ledger` + 两个对账查询 |
| `core/cost/CostLedgerService.java` | 落库方式可切换；traceId 兜底；保留 Kafka 作为可选通道 |
| `core/pipeline/ChatPipeline.java` | 全链路传 `traceId`；成本改按单价表算；流式记录**实际服务的模型名** |
| `observability/TraceIdFilter.java` | 无论透传还是新生成，都回写响应头 |
| `observability/GatewayMetrics.java` | 加 `llmgw.ledger.write` / `llmgw.ledger.failure` 两个计数器 |
| `api/ChatController.java` | 从 exchange attribute 取出 traceId 往下传 |

## 关键代码

**① 成本按单价算，输入输出分开**（合并计价会失真，两者价差常达数倍）

```java
private double estimateCost(String model, ModelTier tier, Usage usage) {
    GatewayProperties.Price price = properties.getPricing().get(model);
    if (price != null) {
        return price.costOf(usage.promptTokens(), usage.completionTokens());
    }
    return usage.total() / 1000.0 * tier.costPer1k();   // 没配单价的模型才回退
}
```

**② traceId 从过滤器贯穿到落库**

```java
// Controller：从 exchange attribute 取出
String traceId = (String) exchange.getAttributes().get(TraceIdFilter.HEADER);
... pipeline.execute(key, request, traceId)
```
```java
// CostLedgerService：trace_id 是 NOT NULL，必须兜底，否则整条 INSERT 被拒
final String tid = (traceId == null || traceId.isBlank())
        ? "local-" + UUID.randomUUID().toString().replace("-", "").substring(0, 16)
        : traceId;
```

**③ 落库走 R2DBC（非阻塞），且与主链路解耦**

```java
if (properties.getCost().toPostgres()) {
    repository.insert(tid, apiKey, feat, model, cacheHit,
                    promptTokens, completionTokens, cost, latencyMs)
            .subscribe(rows -> metrics.onLedgerWrite(),
                       ex -> { metrics.onLedgerFailure();
                               log.warn("cost ledger postgres write failed: {}", ex.getMessage()); });
}
```

## 为什么这么写

**为什么落库方式做成可切换（postgres / kafka / both）？**
DESIGN.md 里账本的设计链路是 `Kafka → 消费者入仓`，好处是解耦、能扛住仓库抖动。
但 MVP 阶段为此起一套 Kafka + 消费者不划算，而且"账本到底有没有记对"这件事必须能立刻查。
所以做成开关：**默认直写 Postgres 先把闭环跑通拿证据**，生产切 `kafka`/`both` 即可，
代码路径都留着。这是"设计不降级、实现分阶段"。

**为什么用 R2DBC 而不是 JDBC？**
网关整条链路是 WebFlux 非阻塞的。JDBC 在连接等待时会占住线程，和"落账不能拖响应"
（step-12 里用 60 秒换来的教训）直接冲突。R2DBC 全程非阻塞。

**为什么 `ledger.write/failure` 要单独打点？**
账本不像请求，写失败用户毫无感知 —— 没有这个计数器，对账缺数据要等到月底才发现。

## 验证方式与结果

脚本分四段，**A 段绕开应用直测 Lua，B~D 段走真实应用**——分层验证，出问题能立刻定位是脚本还是应用。

### A · Lua 原子性（直连 Redis，60/20 并发）

| 场景 | 放行 / 拒绝 | 计数器最终值 | TTL | 耗时 | 结论 |
|---|---|---|---|---|---|
| 60 并发 × 1 token，上限 30 | **30 / 30** | 30（=上限） | 3600s | 70 ms | 无超发 ✅ |
| 20 并发 × 7 token，上限 50 | **7 / 13** | 49（=7×7 最大可容纳） | 3600s | 13 ms | 无超发 ✅ |

非整除场景（7 token / 上限 50）是关键：它证明判断是 `used + need > limit` 而不是简单的次数计数，
第 8 次会算出 `49+7=56 > 50` 而被正确拒绝。**DESIGN 里"配额超发 0"的硬指标拿到实证。**

### B~C · 应用侧配额路径（上限压到 300 tokens，每题 100）

```
#1 -> 放行  usage={'promptTokens': 233, 'completionTokens': 57} traceId=d42839fb... 耗时 2989 ms
#2 -> 放行  usage={'promptTokens': 233, 'completionTokens': 40} traceId=4c091ada... 耗时 1044 ms
#3 -> 放行  usage={'promptTokens': 233, 'completionTokens': 49} traceId=74300d81... 耗时  881 ms
#4 -> 被拒（quota exceeded）耗时 29 ms
#5 -> 被拒（quota exceeded）耗时 22 ms

Redis 计数器 q:quota-verify:2026-09 = 300（预期 300，上限 300）
TTL = 3023999 秒
```

两个值得说的细节：
- **被拒的请求 29ms / 22ms 就返回了** —— 说明配额检查在调用上游**之前**短路，
  超额请求不会浪费一次模型调用。这是"配额"该有的位置。
- **估算 100 tokens，上游实际报 233** —— 见"已知不精确"。

### D · 成本账本落库（Postgres）

```
=== 明细 ===
trace_id                          model           prompt completion cost      latency
74300d81be224a7583c47786377497eb  deepseek-flash  233    49         0.000190  862
4c091adadc9b47c8a7d5ed126147117f  deepseek-flash  233    40         0.000177  1012
d42839fbe6d942f4aa527b039bc23ddc  deepseek-flash  233    57         0.000202  2621

=== 汇总 ===
quota-verify | 3 | 699 | 146 | 0.000569

cost > 0 的行数        : 3 / 3
响应头 traceId 命中账本 : 3 / 3
llmgw_ledger_write_total   : 3.0
llmgw_ledger_failure_total : 0.0
```

**"金额非 0" + "响应头 traceId 与库里的 trace_id 一一对上"** —— 这两条才是这一步真正的交付：
账本不再是空跑，而是能用同一个 traceId 从响应追溯到费用明细，也就是 DESIGN 里说的"成本归因"。

## 踩到的坑

**坑 1 · 测试工具自身的 bug 差点让我误判 Lua 有问题**
第一轮 A 段两次都 FAIL，放行/拒绝全是 0，60 个错误清一色
`TypeError: object of type 'int' has no len()`。
根因：我的 RESP 编码器对参数调 `len()`，而 `cmd("EVAL", script, 1, ...)` 里的 `1` 是 **int**。
→ 编码前先 `str()`。教训：**同一轮测试里"应用侧 PASS、脚本侧 FAIL"时，先怀疑测试工具。**

**坑 2 · 5432 被宿主机上的原生 PostgreSQL 抢了（最费时间的一个）**
现象：`cost ledger postgres write failed: Failed to obtain R2DBC Connection` +
`java.net.SocketException: Connection reset`。
排查过程：
1. `docker compose ps` → 端口映射正常 `0.0.0.0:5432->5432/tcp`；
2. 从宿主直连 `127.0.0.1:5432` → **能连上**，SSLRequest 还收到了合法的 `b'N'`；
3. `netstat -ano | findstr :5432` → **两个不同 PID 同时监听 5432**；
4. `tasklist` → 一个是 `com.docker.backend.exe`，另一个是 **`postgres.exe`**；
5. `sc query` → 宿主机装了个 **PostgreSQL 10 的 Windows 服务 `postgresql-x64-10`**，常驻 5432。

所以应用连的是那个**原生 Postgres**（没有 `llmgw` 库、凭据也不同），不是容器里的。
→ 把容器映射改成 `55432:5432`，容器内仍是标准 5432，只是宿主侧避让。
> ⚠️ **后续修订（2026-10）**：`55432` 在 Windows 上属于系统保留的**动态端口排除区间**
> （`netsh interface ipv4 show excludedportrange protocol=tcp` 可见 `55359-55458`），
> docker 绑定会报 `bind: ... forbidden by its access permissions`。
> **实际端口已改为宿主 `15432`** —— 避让 5432 的结论不变，只是具体端口换了。
> 教训：端口"空闲"不等于"可绑定"。
> 这是"看起来像网络问题、实际是端口归属问题"的典型。**先用 netstat 确认端口归谁**，
> 比反复翻代码快得多。

**坑 3 · 落库前必须先贯通 traceId**
`trace_id` 是 `NOT NULL`。原来一路传 `null`，一旦开启落库就是全量插入失败 ——
而且失败发生在 fire-and-forget 的异步路径里，**用户请求完全无感知**，只会静默丢数据。

**坑 4 · 成本恒为 0，"落库"就成了空跑**
EXPLICIT 路由层 `costPer1k=0`。只把表填满 0 没有意义，所以顺带把单价做成了配置表。

## 已知不精确（诚实清单）

1. **配额用估算、账本用实际**：配额扣的是 `question.length()/4` 估算值（准入控制要快，不能等上游），
   账本记的是上游返回的真实 usage。本例中估算 100 vs 实际 233，**偏差 2.3 倍**。
   改进方向：请求结束后用真实 usage 做一次校正（补扣差额），或对超额部分走阶梯计费。
   这是个能主动讲出来的设计权衡，别等面试官问。
2. **单价是示例值**：`gateway.pricing` 里的数字需按你自己的实际账单口径替换，否则金额不准。
3. **没有对账任务**：目前只有写入和查询，缺少"日终对账"（账本汇总 vs 供应商账单）的定时任务。

## 下一步

- **语义缓存**：Redis Stack 已带 RediSearch（6 个模块已加载），向量检索的底座是现成的；
  缺的是有 `/embeddings` 的厂商。接上后就能去测"阈值-命中率-误命中率"曲线。
- **配额校正**：按上面的第 1 条，用真实 usage 补扣。
- **对账任务**：定时跑 `cost_ledger` 汇总 vs 供应商账单，差异超阈值告警。
