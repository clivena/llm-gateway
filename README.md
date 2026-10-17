# llm-gateway · LLM 智能网关

> OpenAI 兼容协议的自建 LLM 网关。把**多模型接入、语义缓存、分布式配额、成本归因**收敛到一个入口，全链路响应式、无阻塞。

一个团队自建 LLM 应用，最先撞上的三件事是：**钱花在哪**、**会不会超支**、**慢不慢**。
这个网关就是针对这三件事做的工程化收口——不是"包一层 HTTP 转发"，而是把成本、配额、缓存做成可观测、可验证的约束。

---

## 为什么需要它

让业务方各自 `curl` 上游模型接口，很快会遇到这些问题：

| 问题 | 直接调用的后果 | 网关的做法 |
|---|---|---|
| 成本黑洞 | 月底收到账单，不知道哪条业务线花的 | 每次调用落一条账本，带 `traceId`，可按租户汇总对账 |
| 预算失控 | 一个死循环脚本能把额度跑爆 | Redis + Lua 原子预扣，超额请求在**调用上游之前**就被拦掉 |
| 重复计算 | 同一个问题被反复问，每次都付费 | 语义缓存：问法不同但意思相同也能命中 |
| 模型锁死 | 换供应商要改所有业务代码 | OpenAI 兼容协议 + 模型分级路由，业务方只认 `model=auto` |
| 排查困难 | 出问题找不到是哪次调用的锅 | `X-Trace-Id` 从入口贯到账本，一次响应可反查它的全部开销 |

---

## 架构

```
                    ┌───────────────────────────────────────────────┐
  POST /v1/chat/    │              ChatPipeline（一次请求的编排）      │
  completions  ───► │                                               │
                    │  ① 配额准入 ──► ② 语义缓存 ──► ③ 路由选择      │
                    │       │              │              │         │
                    │       │ 超额即返回     │ 命中即返回    ▼         │
                    │       ▼              ▼        ④ 上游调用       │
                    │   Redis+Lua      RediSearch    (WebClient)    │
                    │   (原子预扣)      (向量检索)     │ 流式/非流式  │
                    │                                ▼              │
                    │  ⑥ 成本落账 ◄────────── ⑤ 级联升级（可选）      │
                    └───────┬───────────────────────────────────────┘
                            │ 异步、离开响应路径
                            ▼
              CostLedgerService ──► Postgres（直写，可对账）
                                └─► Kafka（流式，生产链路）

  旁路：TraceIdFilter 生成/透传 traceId · GatewayMetrics 暴露 Prometheus 指标
        AdminController 提供只读运维接口（供 dashboard 读取真实数据）

  任务路由（在 ① 之前，只决定"用哪个模型"，不决定"怎么调这个模型"）：
        model=auto → [显式 task] → [规则推断] → [default-task] → task→model 映射表
        · 只替换 resolveRoute 的输出，下游（候选链 / 重试熔断 / 级联 / 配额）一行不改
        · 显式模型最优先：用户点名了就不替换，否则是未经同意的替换与计费
        · 未知的**显式**任务 → 400；未知的**推断**任务 → 落 default（网关判错不该让请求失败）
        · 缓存段是 apiKey:model，任务换了模型就天然不串段（不变量 B 继续成立）

  韧性（F1，落在 ④ 与 ⑤ 的内侧，按 provider 各一套）：
        timeouts → CircuitBreaker → Retry → 真实上游调用
        · 熔断在内、重试在外 —— 熔断要看到每一次尝试，否则失败率被重试稀释
        · 熔断打开 → 503 + 不再发请求（这是它存在的唯一理由）

  故障转移（F2，包在上面整条链**之外**，按"同模型的候选链"逐个尝试）：
        [provider-a 的 超时→熔断→重试] → 失败且可转移 → [provider-b 的同一条链] → …
        · 候选链顺序 = 配置声明顺序 = 优先级
        · 转移谓词 ⊇ 重试谓词，多出来的是"熔断打开"（那家不问了，去问下一家）
        · 请求级总预算（默认 4 次），把 maxAttempts × 候选家数 这个乘法关住
        · 熔断打开的候选**直接跳过**（不白扣预算）；HALF_OPEN 必须放行（否则永不自愈）
        · 流式：出过任何一帧就不再换家（标记必须跨 provider 共享）
```

**模块划分**

```
src/main/java/com/llmgw/
├── api/                 # OpenAI 兼容接口 + DTO + 只读运维接口
│   └── dto/             # ChatRequest / ChatResponse / Usage / ChatMessage
├── core/
│   ├── pipeline/        # ChatPipeline：一次请求的完整编排
│   ├── protocol/        # ProviderAdapter 抽象 + OpenAiAdapter 实现
│   │                    # ProviderRegistry：model → **候选链**（F2，顺序即优先级）
│   ├── router/          # RouterService：解析 Route（显式模型 / policy / 任务路由）
│   │                    # TaskClassifier：显式 task → 规则推断 → default-task
│   │                    # RouteTrace：路由回执（管线写、HTTP 层读 → 响应头）
│   │                    # Route / TaskDecision / ModelTier / RoutingPolicy
│   ├── resilience/      # UpstreamResilience：按 provider 的重试 + 熔断（F1）
│   │                    # 转移谓词 failoverable = retryable ∪ {熔断打开}（F2）
│   ├── cache/           # SemanticCacheService + Redis 向量检索实现
│   ├── quota/           # QuotaService：Redis + Lua 原子扣减
│   └── cost/            # CostLedgerService（sink 可切）+ Repository（R2DBC）
├── config/              # 配置绑定、WebClient、dashboard 托管
└── observability/       # TraceIdFilter、GatewayMetrics
src/main/resources/
├── application.yml      # 全部策略可配（上游/缓存/配额/路由/单价表）
├── lua/quota.lua        # 配额原子脚本
└── static/dashboard/    # 可视化控制台构建产物（由 dashboard 仓产出）
```

---

## 四个核心设计

### 1. 配额：Redis + Lua 原子扣减

朴素实现是 `GET` 读当前用量、判断、再 `SET` 写回——两个请求并发时会双双通过判断，**超发**。
这里把"读-判-写"整体塞进一段 Lua 交给 Redis 单线程执行，天然原子：

```lua
-- src/main/resources/lua/quota.lua
local used = tonumber(redis.call('GET', KEYS[1]) or '0')
local need = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])

if used + need > limit then
  return 1                      -- 超额：直接拒绝，不写、不调上游
end
redis.call('INCRBY', KEYS[1], need)
redis.call('EXPIRE', KEYS[1], ttl)
return 0                        -- 放行并预扣
```

判断用的是 `used + need > limit` 的**量级比较**而不是"数次数"，所以配额能以 token 为单位精细控制。
预扣发生在调用上游之前，被拒的请求实测量级 **29ms** 返回，不产生任何模型费用。

### 2. 两级缓存：先字面，再语义

缓存分两层，**前一层命中就不再付出后一层的代价**：

| 层 | 判据 | 依赖 | 代价 |
|---|---|---|---|
| **L1 精确** | 规范化问题 → SHA-256 → 单值查 Redis | 无（不需要 embedding） | 一次 Redis GET |
| **L2 语义** | 向量检索 + 相似度阈值（默认 0.92） | 上游 `/embeddings` | 一次 embedding 调用（**要花钱**） |

同一个意图换种问法（"怎么理解语义缓存" vs "语义缓存是什么"），字面不同、向量相近 ——
那是 L2 的活。而"同一个问题问两遍"在任何真实业务里都非常常见（用户重试、页面刷新、
批量任务重跑），这类请求根本不需要向量检索就能命中：字面完全相同。

L1 只做三种**语义上绝对安全**的规范化：`trim`、连续空白折叠、ASCII 大小写归一。
刻意**不剥标点** —— "这是对的"与"这是对的？"是两个问题，模糊匹配留给 L2（它有阈值兜着）。
每放宽一步规范化都同时抬高命中率与错命中率，而缓存最坏的失败模式是**错答案**
（漏命中只是没省钱），所以 L1 的取舍一律偏保守。

> **为什么必须有 L1**：L2 依赖 embedding，而 DeepSeek 不提供 `/embeddings` ——
> 在只有 L2 的版本里，缓存是"配置里开着、运行时一次都没命中过"。
> 这类静默失效比不做缓存更糟：不做时人知道每次都花钱，开着会让人以为有保护。
> L1 不依赖任何外部能力，所以在任何部署下都能真正命中。
> 各层的开关也是独立的（`cache.exact-enabled` / `cache.semantic-enabled`）——
> 没有向量能力的供应商应该显式关掉 L2，别让它每次请求都空跑一遍取向量。

### 3. 成本归因：一次响应能反查它的开销

`TraceIdFilter` 在入口生成或透传 `X-Trace-Id`，一路传到落账那一步。
响应头里的 traceId 与 `cost_ledger.trace_id` 一一对应，于是"这次调用花了多少钱"是可归因的事实，而不是估算。

计费按 `gateway.pricing` **输入/输出分开计价**（输出通常贵 3~8 倍，混在一起算会系统性低估）。
落账是异步的，且通过独立线程离开响应路径——见下面「踩过的坑」第 2 条。

### 4. 全链路非阻塞

上游调用用 `WebClient`，账本落库用 **R2DBC**（不是 JDBC），配额走响应式 Redis 客户端。
WebFlux 的 Netty 事件循环线程数很少，任何一处同步阻塞都会连锁拖慢全部并发请求——
所以连写数据库都不允许占用事件循环线程。

全仓 `.block()` 出现次数为 **0**（可以用 `grep -rn "\.block(" src/` 自己验，命中的全在注释里）。
适配器接口的每个出网方法都返回 `Mono`/`Flux`，缓存接口也是。

**为什么这件事比"性能"更重要**：Reactor 的 `timeout()` / `retryWhen()` / `circuitBreaker()`
是**算子**，只能挂在 `Mono`/`Flux` 上。接口只要是同步的、内部靠 `.block()` 取结果，
超时、重试、熔断就全都无处可挂 —— 这是它们的硬前提，不是一个可选的优化。

而这行 `.block()` 是**主动加上去**的：`WebClient` 本来就返回 `Mono`，
去掉它代码不是变长，是变短。当前保留 `subscribeOn` 的只有一处
（`ChatPipeline.recordAsync`），理由不同：`KafkaTemplate.send()` 在 broker 不可达时会
阻塞到 `max.block.ms`，需要把这个**已知会阻塞的边缘操作**隔离出去，而不是兜住整条链路。

---

## 快速开始

**前置**：JDK 17+、Maven、Docker

```bash
# 1) 起依赖（Redis + Postgres）
docker compose up -d redis postgres

# 2) 配密钥：从模板复制后填入自己的 key
cp config/application-local.yml.example config/application-local.yml

# 3) 编译 + 单测
scripts/run-gateway-tests.cmd          # Windows
# mvn test                             # 通用

# 4) 起服务
mvn spring-boot:run -Dspring-boot.run.profiles=local
```

打一次真实请求：

```bash
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: tenant-a" \
  -d '{"model":"deepseek-flash","stream":false,
       "messages":[{"role":"user","content":"用一句话解释什么是语义缓存"}]}'
```

看它花了多少钱（响应头的 traceId 就是这把钥匙）：

```bash
curl -s "http://localhost:8080/admin/ledger/recent?limit=5"
docker compose exec -T postgres psql -U postgres -d llmgw \
  -c "select trace_id, model, prompt_tokens, completion_tokens, cost from cost_ledger order by id desc limit 5"
```

**一键真机联调**（起服务 → 打真实请求 → 抓指标 → 关服务，一个进程跑完）：

```bash
python scripts/run-gateway-smoke.py
```

---

## 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/chat/completions` | OpenAI 兼容对话接口，支持 `stream: true`（SSE 逐字返回） |
| GET | `/admin/status` | 依赖健康（Redis / Postgres）+ 当前生效策略 |
| GET | `/admin/pricing` | 计费单价表 + 路由策略——面板上"为什么是这个价"的依据 |
| GET | `/admin/ledger/recent?limit=` | 账本明细（limit 收口 1~200，避免被当全表导出用；含 `task` / `provider` 列） |
| GET | `/admin/ledger/summary` | 账本按租户汇总（对账口径） |
| GET | `/admin/ledger/by-task` | 账本按**任务**汇总：哪个任务在花钱（`NULL` 显示为 `(none)`） |
| GET | `/admin/ledger/by-provider` | 账本按**链路**汇总：钱付给了谁、备用那家接管了多少（`NULL` 显示为 `(none)`，缓存命中为 `cache`） |
| GET | `/admin/quota/{apiKey}` | 指定租户当前周期的配额用量 |
| GET | `/admin/circuit` | 各 provider 的重试 / 熔断状态与失败率（F1；F2 之后也是"为什么转移了"的第一诊断入口） |
| GET | `/admin/route` | 当前生效的任务映射表（含真实档位）+ 本进程 `task × source` 命中分布 |
| GET | `/dashboard` | 可视化控制台（同源托管） |
| GET | `/actuator/prometheus` | Prometheus 指标（含 `llmgw_cache_hit` 等自定义指标） |

两个**不属于 OpenAI 协议**的请求字段（本网关的私有扩展，普通 OpenAI 客户端忽略即可）：

| 字段 | 作用 |
|---|---|
| `feature` | 业务线标签，**只用于成本归因**（落 `cost_ledger.feature`）。例：`resume-tailor` |
| `task` | 这次请求需要什么能力，网关据此选模型。取值由任务表决定，默认 `code` / `extract-json` / `summarize` / `chat` |

`task` 的规则：

- **只在 `model=auto` 时生效**。显式写了 model 就以 model 为准 —— 用户点名了模型，
  网关不能因为"这个任务更适合别的模型"就换掉它（那是未经同意的替换与计费）。
- 传了但**没在任务表里声明** → `400 invalid_request_error`，错误体列出可用任务。
  不静默兜底：否则调用方以为 `code` 生效了、实际走了 chat，只能翻账本才发现。
- 不传 → 网关按规则推断；推断不出来落 `default-task`（推断是网关自己的判断，
  判错不该让请求失败）。

响应头里的路由诊断信息（**不进 body**，body 保持纯 OpenAI 形状）：

| 响应头 | 取值 | 说明 |
|---|---|---|
| `X-Route-Task` | `code` / `summarize` / `chat` / `explicit` / `n/a` | 本次请求被判定成什么任务 |
| `X-Route-Source` | `explicit` / `inferred` / `fallback` / `policy` | 这个任务是怎么来的：客户端声明 / 网关推断 / 兜底 / 未启用任务路由 |

两条使用约定：

- **路由没解析成功时（参数错 / 未知任务）这两个头不会出现** —— 宁可没有，
  也不写一个假的 `n/a` 让人以为"任务路由没开"。
- 非 ASCII 任务名会被降级成 `_`（HTTP 头只保证 ASCII 可移植）。
  原始值在 `/admin/route` 与 route 日志里。**它是诊断头，不是契约**，
  不要拿它做计费或路由依据 —— 那边请用 `traceId` + 账本。

`/admin/*` **全部只读**——不扣配额、不写账本、不改配置，与 `/v1` 主链路完全隔离，挂了也不影响对外服务。

**错误状态码约定**（`/v1/chat/completions`）：

| 状态码 | 场景 |
|---|---|
| 400 | 请求不合法——`messages` 为空、`model` 不在服务范围、**`task` 未在任务表里声明**（`error.type = invalid_request_error`） |
| 429 | 配额超额（`error.type = rate_limit_exceeded`） |
| 502 | 上游/适配层异常：连不上上游、上游返回非 2xx、上游报错、空响应体（`error.type = upstream_error`） |
| 503 | **熔断器打开**——请求根本没发给上游。F2 之后也包括"整条候选链都熔断"（`error.type = circuit_open`，F1） |
| 500 | 其他内部错误 |

> 早期实现把所有错误都写成 HTTP 200 + 响应体里的 `error` 字段。问题不在于"不够规范"：
> 客户端拿不到 429 就无法用状态码做退避决策，**监控的错误率曲线里也看不到限流**——
> 一个所有人都看不见的错误，等于没报。
>
> **400 与 502 的分界是"该不该重试"**：`messages` 为空、`model` 写错是**调用方**的问题，
> 重试一万次结果一样。把它归成 502 会让客户端误判成网关故障，去退避、去重试，白等一场。
>
> **502 与 503 的分界是"我们试没试"**：502 = "我替你问了，上游答得不对"，
> 客户端该立刻重试；503 = "我现在不打算替你去问"，客户端该**退避**。
> 把熔断也报成 502，就等于在网关已经开始自我保护的时候，
> 还在用状态码鼓励客户端加速重试。
>
> ⚠️ 502 这一行是 F1 补上的：`WebClientResponseException` 继承 `NestedRuntimeException`，
> 既不是 `IllegalStateException` 也不是 `IllegalArgumentException`，
> 于是**上游任何非 2xx 都曾落到 500**。这个缺陷一直在，只是网关从不重试时
> 上游一次 500 就直接穿过去，没人核对过状态码 —— 是
> `check-resilience.py` 的断言把它抓出来的。

---

## 验证过的数据

### 重构前的历史记录（保留作演进对照）

以下都是**真机跑出来的**，不是设计目标（原始记录见 `docs/step-12`、`docs/step-13`）：

| 项 | 结果 |
|---|---|
| 真实模型联调 | DeepSeek 全链路跑通：非流式 **3.4s**、流式**首字 1.1s**；双模型切换、SSE 逐字吐均正常 |
| 编译与单测 | Maven 3.9.11 + JDK 24 编译 28 个源文件，单测 **40 passed**，BUILD SUCCESS |
| 配额原子性 | 60 并发 ×1（上限 30）→ **30 放行 / 30 拒绝**，计数器精确停在 30；<br>20 并发 ×7（上限 50）→ **7 放行 / 13 拒绝**，终值 49 |
| 配额拦截延迟 | 超额请求 **29ms** 返回，未触及上游、未产生费用（HTTP **429**） |
| 成本账本 | Postgres 写入 3 行，`cost > 0`，合计 **0.000569 元**；响应头 traceId 与库中 `trace_id` **3/3 一一对应** |
| 失败计数 | `ledger_write=3 / ledger_failure=0` |
| 复盘修复 | 全量复查另修 **5 个缺陷**：超额状态码 429、缓存命中落账异步化、级联两次调用都落账、缓存按模型隔离、schema 预留表标注。见 [`docs/step-15-review-fixes.md`](./docs/step-15-review-fixes.md) |

第二组配额测试是关键：**7 除不尽 50**，它证明逻辑是量级比较而非数次数——
第 8 次算出 `49+7=56 > 50` 被正确拒绝。

> 这批数字出自**重构前**的版本，保留是为了看演进 —— 重构后已重新实测，见下一节。
> 两点值得记下来：① 重构后的非流式延迟（4.25s / 4.84s）比这里的 3.4s **更慢**，
> 因为这次请求没有限制输出长度（completion 576 tokens），**两组数字不可直接比较**；
> ② "配额原子性"这一项在重构后用**同一组参数**（上限 50 / 预扣 7 / 20 并发）
> 复现出了相同结论（7 放行 / 13 拒绝 / 终值 49），说明重构没有破坏并发安全性。

### 第一批验证（响应式化 / 多 provider / 路由配置化）

**静态与装配层**

| 项 | 结果 |
|---|---|
| 响应式化 | 全仓 `.block()` 残留 **0**（命中全在注释里）；`subscribeOn` 仅剩 1 处且有独立理由 |
| 单元与装配测试 | **40 passed**（原 20 → 新增 20 条：响应式契约、多 provider 装配、路由一致性、上下文装载） |
| 启动期校验 | 默认配置与 `local` profile 的 Spring 上下文**均装载成功**，启动日志逐条打印 `provider 注册：[name] baseUrl → models` |
| 多 provider | 适配器数量 = 配置的 provider 家数（不再是单例 + `findFirst()`） |
| 路由一致性 | `gateway.router.candidates` 与实际 provider 不一致时**启动即失败**，不再等到请求 502 |

**真机实测**（2026-10-16 · DeepSeek 真实联网调用 · 复现见下方两条脚本）

| 项 | 结果 |
|---|---|
| 非流式双模型 | `deepseek-flash` **4.25s**（41→576 tok）、`deepseek-v4-pro` **4.84s**（94→281 tok），均 HTTP 200 |
| 流式 | 首字 **1076ms** / 总 **1104ms**；9 个文本分片；**收到 `[DONE]` 终止帧** |
| 流式真实 usage | 账本记 `prompt=43 / completion=70`，与上游末帧回传值**逐位一致**<br>（旧实现在此只能记 `长度/4` 的估算值） |
| 分片 model | 分片里的 `model` 是**实际服务方**；分片 id 为 `chatcmpl-<traceId>`，客户端可用于对账 |
| 参数透传 | 请求带 `max_tokens: 8` 时，账本 `completion_tokens` **8/8 全部命中** |
| 账本 traceId 贯通 | 3 行全部与响应头 `X-Trace-Id` 一一对应 |
| 输入输出分开计价 | 三行 cost 均可手算复现：`41×0.0005 + 576×0.0015 = 0.000885` ✓ |
| 配额原子性 | 20 线程同时冲（上限 50 / 单次预扣 7）→ **7 放行 / 13 拒绝**，计数器精确停在 **49**<br>（第 8 次 `49+7=56 > 50` 被拒 —— 说明扣的是量而不是次数） |
| 配额拦截延迟 | 429 平均 **90ms** 返回，未触及上游、未产生费用 |
| 账本完整性 | 配额放行的 7 次 → 账本 **7 行**，无静默丢失；`ledger_failure = 0` |

> 复现：`python scripts/run-gateway-smoke.py`（主链路 + 流式 + 指标）、
> `python scripts/check-quota-concurrency.py`（配额并发，自带 6 条断言）、
> `python scripts/calibrate-token-estimator.py`（配额估算精度，六组探针）、
> `python scripts/check-cache-layers.py`（缓存是否真的命中，8 条断言）、
> `python scripts/check-f0-failure-semantics.py`（推理型模型失败语义 + 配额结算，8 条断言）、
> `python scripts/check-resilience.py`（重试 + 熔断，11 条断言，含本地桩）。
> 五条脚本都会自己起服务、摘除端口劫持环境变量、跑完自动关服务，
> 并**在开始前检查 Redis/Postgres 容器是否在跑**（缺依赖时失败会长得像代码 bug）。

> 排查上游行为用 `python scripts/probe-upstream-shape.py`
> ——它扫描 `max_tokens` 与响应字段，用来回答"上游到底返回了什么"
> （例如"为什么正文是空的"）。**会真实调用上游**，跑一次约 8 个请求。

### A 批验证（配额估算 / 缓存分层 / 可观测）

**静态层**：单元与装配测试 **67 passed**（首批 40 → A 批新增 27 条）。
启动日志逐条确认缓存层装配顺序：`缓存层装配：[L1-exact, L2-semantic]`。

**配额估算精度**（DeepSeek 真实联网 · `calibrate-token-estimator.py`，六组探针全部 PASS）

| 探针 | 估算/真实（旧公式 `length/4`） | 估算/真实（`TokenEstimator`） |
|---|---|---|
| 中文短 | 0.15 | **1.00** |
| 中文长 | 0.23 | **1.00** |
| 英文短 | 0.09 | **1.03** |
| 英文长 | 0.64 | **1.02** |
| 混合 | 0.23 | **1.00** |
| 多消息 | 0.04 | **1.01** |

**缓存命中**（`check-cache-layers.py`，8/8 PASS）

| 用例 | 期望 | 第一次 | 第二次 |
|---|---|---|---|
| 完全相同 | 命中 | 15752ms | **39ms** |
| 首尾空格 | 命中 | 10327ms | **14ms** |
| 连续空白 | 命中 | 19430ms | **64ms** |
| 英文大小写 | 命中 | 4738ms | **48ms** |
| 标点不同 | 不命中 | 15829ms | 22108ms（走上游） |
| 内容不同 | 不命中 | 13958ms | 13583ms（走上游） |
| 换代模型 | 不命中 | — | 22552ms（走上游） |
| 账本留痕 | `cache_hit=true` | — | **4 行** |

命中 **快 250~400 倍**，usage 全 `0/0`。命中判定用**三重交叉验证**
（响应头 `X-Cache-Hit` + 明显更快 + 未计费）——只看响应头的话，
一个"永远返回 true 的 bug"也能骗过测试。

### F0 验证（推理型模型失败语义 + 配额结算）

`check-f0-failure-semantics.py` —— **8/8 PASS**。

| 检查 | 结果 |
|---|---|
| 非流式 `finish_reason` 透传 | `finish_reason='length'`、正文 0 字（`max_tokens=64`）<br>F0 之前这里恒为 `'stop'` —— 网关主动把"被截断"报成"正常结束" |
| 流式收尾帧 | 收尾帧 1 个、`finish_reason='length'`、`delta` 为空对象、`[DONE]` 正常 |
| 配额结算覆盖 completion | **增量 2498 == 账本真实 tokens 2498**（prompt 34 + completion 2464）<br>F0 之前只会记 34 —— 计量只覆盖实际消耗的 **1.4%** |
| 缓存命中全额退还预扣 | 命中前后配额增量 **0**（此前命中省了钱却照样吃配额） |
| `llmgw.upstream.empty` / `.truncated` | 0 → 1 / 0 → 1 |
| `llmgw.quota.settled` / `.settle.failed` | 4 / **0** |

`quota_settle.lua` 的四条语义另用 `redis-cli --eval` 单独验过：
补扣（34 → +1605 → 1639）、退还（-1639 → 0）、周期翻篇返回 -1、
下限保护（已用 50 退 -9999 → 0）。

> 复查这批缺陷的共同特征是**"静默失效"**：
> 缓存 key 少一个模型维度不会报错，只会让路由策略被悄悄架空；
> 落账同步执行也不会报错，只会在 Kafka 不可用时把最快的路径变成最慢的；
> `finish_reason` 填一个假的 `"stop"` 更不会报错 —— 它只会让客户端
> 在答案被截断时以为一切都好。
> 这类问题在监控上看起来一切正常。

### F1 验证（重试 + 熔断）

`check-resilience.py` —— **11/11 PASS**。分两段跑，因为两类问题只有不同的上游能回答：
**A 段打真实 DeepSeek**（重试在真实失败上到底会不会触发，取决于上游的实际行为），
**B 段打本地桩**（重试了几次、熔断后有没有再发请求 —— 精确计数只能由能计数的上游给出）。

| 检查 | 结果 |
|---|---|
| 空正文触发重试，且重试耗尽后 F0 的透传不被破坏 | 重试增量 **2**（= `maxAttempts-1`）<br>客户端拿到 **HTTP 200 + `finish_reason='length'`**，不是 502 |
| 正常答案不被重试（对照） | 重试增量 **0**，正文 760 字 |
| `/admin/circuit` 可读、上游健康时 CLOSED | `state=CLOSED`，buffered=4 failed=3 successful=1 |
| **重试真的重新发起了请求** | 桩设定"前 2 次 500"→ 桩收到 **3** 次，最终拿到成功响应 |
| 重试耗尽如实报 502 | HTTP **502** + `type=upstream_error`，桩收到 3 次 |
| 熔断在连续失败后打开 | **1 轮**后 `state=OPEN` |
| **熔断后不再发请求** | HTTP **503** + `type=circuit_open`，桩请求数 **4 → 4 未变** |
| 熔断可自动恢复 | `OPEN` → 10s → `HALF_OPEN` → 3 次成功 → `CLOSED` |
| **流式首片前重试不产生重复内容** | 桩收到 **2** 次流式请求，客户端正文 = 期望值（无重复拼接） |
| F1 指标带 provider 标签 | `upstream.retry`=6 · `circuit.rejected`=2 · `circuit.open`=0 |

其中两条是这套验证真正的价值所在，因为它们**无法用状态码证明**：

- **"桩收到 3 次"** —— 一个把旧响应缓存起来的实现同样会返回 200、正文正确、
  日志也打了"retrying"。只有**上游自己的计数**能证明那 3 次请求真的发生了。
- **"桩请求数 4 → 4 未变"** —— 熔断状态显示 OPEN、`circuit.rejected` 涨了，
  都不等于上游真的被放过了。这正是熔断存在的唯一理由，也是唯一能证明它的方式。

### F2 验证（跨 provider 故障转移）

`check-failover.py` —— **13/13 PASS**。分四段跑（四次启动服务），因为四类问题需要四种环境：
**A 段**打真实 DeepSeek **且**候选链里塞一个不可达部署 ——
只有真实上游能回答"转移到第二家之后拿到的是不是真答案"；
A1/A2 只把那个坏候选的**位置**换一下（链尾 / 链首），用来量出"顺序即优先级"的代价。
**B 段**两个本地桩（一家可控失败、一家永远正常并计数）—— 精确计数只能由能计数的上游给出。
**C 段**同样的桩 + 调小的熔断阈值 —— 熔断状态活在进程里、无法重置，
要观察"打开之后"就必须用一个专门为它配的环境。

| A 段（真实上游） | 结果 |
|---|---|
| 链尾坏候选**零代价**：[真实, 不可达] | HTTP=200 正文 45 字 **转移 0 次** |
| `/admin/circuit` 列出两家候选 | `providers=['deepseek', 'provider-dead']` |
| 链首不可达 → **透明转移**：[不可达, 真实] | HTTP=200 正文 57 字 **转移 1 次 retry+2 耗时 4782ms** |

> 同样两家候选，只换顺序：**代价从"零"变成"约 5 秒"** —— 链首要先把 3 次重试预算烧完
> 才轮到备选，而这段开销要等熔断攒够样本才被接管。A1 还顺手守住了"候选链不会被遍历/探测"：
> 若实现是"先把候选都探一遍再挑"，链尾多一家坏的也会让每个请求都变慢。

| B 段（两个桩） | 结果 |
|---|---|
| 覆盖生效 + **正常路径不转移** | 桩A=1 桩B=0 |
| **转移真的换了另一家** | 桩A=**3** 桩B=**1**，failover +1 / retry(A) +2，正文来自桩B |
| 跨 provider **总预算**生效 | HTTP 502 + `upstream_error`，上游总调用 **4/4**（无上限则为 6） |
| 确定性 4xx **不转移** | 桩A=1 桩B=**0**，HTTP 502 |
| 流式首片前转移，**正文不重复** | 正文='桩B的答案流式后半段' 桩A stream=3 桩B stream=1 |
| **首帧后不转移**（闸门） | 客户端收到'首帧已送达'，桩B stream=**0** |
| failover 指标带**方向**标签 | `from=a,to=b`=3.0 · `from=b,to=a`=0.0 |

| C 段（熔断后） | 结果 |
|---|---|
| 失败把 A 的熔断器打开 | HTTP=200（B 兜住）`state=OPEN` |
| OPEN 的候选**被跳过** | 桩A **3→3**（增量为 0）桩B 1→2 |
| 全候选不可用 → 快速失败 | HTTP **503** + `circuit_open`，耗时 **32ms**，两桩计数均不变 |

三条只有真机能给的证据：

- **"桩A=3 而不是 1"** —— 证明"重试在内、转移在外"：先在同一家试满再换地方。
  反过来（一抖动就换家）会表现为桩A只收到 1 次，而备选被无谓消耗。
- **"桩B=0"**（B5）—— 证明"是否已产出"这个标记**跨 provider 共享**。
  各建各的标记会让客户端收到"一个首帧 + 一整段答案"的拼接，
  而服务端日志只有一条漂亮的 `failing over`。为此脚本故意用
  "声明大于实际写入的 Content-Length"制造 premature close ——
  那是真机上唯一能做出"首帧已送达 + 报错"这个场景的办法。
- **"桩A 3→3"**（C2）—— 证明熔断打开的候选是被**跳过**而不是"试一次被拒再换家"。
  后者每次请求都会白扣一份预算，于是备选只剩 1 次重试机会 —— 一个静默的体验退化。

### 任务路由验证（按 task 选模型）

**单元测试**：`mvn -o clean test` → **202 passed**（F3 交付时 175 → step-22 +11 → step-23 +14 → P7 +2）。

| 层面 | 断言 |
|---|---|
| 判定 | 显式优先；大小写/空白归一化；代码围栏、符号**组**、强关键词；JSON 需格式提示；长文阈值；只看最后一条 user；确定性；不抛异常；`classifier=off` 仍认显式 task |
| 路由 | 任务命中 / 推断命中 / 推断未配置落 default / 未知**显式**任务 400 / 显式模型优先 / 开关关闭走 policy |
| 启动校验 | `default-task` 未声明、task 的 model 不在 candidates、enabled 但表为空、`classifier` 取值非法、`policy=TASK_BASED` 已被删除 |
| 编排 | 非流式与流式**都**用 `route.tier()`；缓存段跟着模型走；未知任务 → 400（且必须是**流内错误**）；指标真的写了 |
| 留证（R2） | 回执两条路径都写 / 失败时留空；响应头真的发出（含流式）；非 ASCII 降级；账本 task 透传到仓储与 Kafka；`/admin/route` 用运行时档位 |
| 归因（P7） | 账本记的是**真正答话那家**（A 500 → B 接管时必须是 B）；provider 与 model 各自独立透传；Kafka 事件带 provider |

**真机**：`scripts/check-task-routing.py` —— **15/15 PASS**（A~F 段 8 条 + G 段 7 条）。

> 交付 F3 时这一节写的是"未实跑"：容器起不来（Docker Desktop 被沙箱安全策略阻止，
> 禁 `wsl.exe` 且不可绕过）。后来容器恢复正常，补跑结果如上 —— 没有把没跑过的
> 写成跑过了，也没有在跑过之后继续留着"未跑"的旧说法。

```
✓ G1 非流式响应头带 X-Route-Task / X-Route-Source
✓ G2 推断来源可区分（inferred 而不是 explicit）
✓ G3 流式响应头同样带路由结果
✓ G4 账本按请求写入 task / provider 列（非流式）
✓ G6 流式账本同样写入 provider / task
✓ G5 /admin/route 给出映射表与命中分布
✓ G7 /admin/ledger/by-provider 能按链路归因
```

复跑：`docker compose up -d && python scripts/check-task-routing.py`

一条只有真机能给的证据：**C1 同时断言"HTTP 400"和"上游零调用"**。
只断言状态码不够 —— 如果实现是在发完请求之后才发现任务名不对，
状态码可能仍然是 400，但钱已经花掉了。

G4 同理：它用**这次响应的 `traceId`** 去库里找那一行，
同时证明"响应与账本属于同一次请求"和"task 跟着这次判定走"。
第一版用"账本 id 递增"做判据，而 `/admin/ledger/recent` 的 SELECT 里没有 `id` ——
判据恒假、断言恒失败（见 step-23 第 6 节）。

**G6 不是 G4 的重复**：非流式在拿到响应体那一刻就知道是谁答的；
流式根本拿不到"响应对象"，只能在开始拉流时记下 adapter 名。
两条路径的实现完全不同，只验一条等于把另一半留在未验证状态。

### 复查修复（step-22）

`mvn -o clean test` → **186 passed**（F3 的 175 → 186，+11）。

这一轮不是新功能，是把「已知边界」里三条标着"尚未做 / 待办"的**缺陷**修掉：

| 修复 | 原来的后果 | 现在的口径 |
|---|---|---|
| 4xx 不再计入熔断窗口 | 一个客户端连发参数错误的请求，就能把健康 provider 熔断，所有租户一起 503 | 4xx（429 除外）既不算成功也不算失败；429 是容量信号，仍然记账 |
| 被丢弃的尝试也记账 | 重试 2 次 = 3 次真实计费，账本只写 1 行，故障期成本系统性低估 | 账本行数 = 真实上游调用次数；配额同步按真实用量结算 |
| 流式缓存命中补收尾帧 | 命中时客户端拿不到 `finish_reason`，与"被截断"无法区分 | `delta + finish(stop)` 两个分片 |
| 计价回退用实际模型的档位 | 级联升级后按**便宜档**计费（未配 pricing 时） | 按 `model` 查候选表；模型不在表里才退回调用方档位 |

新增 11 条单测，其中三条专门守"补账"的边界：丢弃的尝试记一行、正常请求只记一行、
**重试耗尽时最后一次空响应不能被记两次**（它既是失败的尝试，又是最终交付的答案）。

复跑：`mvn -o clean test`；真机部分本轮只补了任务路由，F2 的两个脚本下次动到时一起跑。

---

## 已知边界（诚实清单）

1. ~~**多供应商未实现**~~ → **已修**：适配器不再单例，由 `ProviderRegistry` 按配置为每家 provider
   各建一个实例，并建立 `model → 候选链` 索引。同名模型按声明顺序组成故障转移候选链，
   链首即默认执行者（**顺序即优先级**，F2；旧实现是"先声明者胜、后者被 warn 后丢弃"，
   那样"同模型多部署"根本表达不出来）。启动时逐条打印注册结果与候选链。
2. ~~**`RouterService` 模型名硬编码**~~ → **已修**：候选表改为配置项
   `gateway.router.candidates`，并在启动时校验"每个候选都被某个 provider 支持"，
   不一致直接启动失败（把运行时的 502 前移成启动期报错）。`model=auto` 现在可用。
3. ~~**语义缓存需要 embedding 能力**~~ → **已修**（拆成两级）：L1 精确层不依赖
   embedding，所以在 DeepSeek 下也**真正命中**了 —— 此前是"配置里开着、
   运行时一次都没命中过"的静默失效。L2 语义层仍是可选增强（需要 `/embeddings`），
   没有该能力的供应商应显式关掉 `cache.semantic-enabled`，
   否则每次请求都要白跑一遍"试遍所有 adapter 取向量"的失败路径。
4. ~~**配额估算严重低估中文请求**~~ → **已修**（`TokenEstimator` + 实测校准脚本）：
   旧实现按 `length/4` 估算，且只算最后一条 user 消息。校准后估算/真实比值
   **1.00 / 1.00 / 1.03 / 1.02 / 1.00 / 1.01**（六组探针）。
   校准过程还纠出一个**更根本的建模缺口**：上游存在一次性的对话模板固定开销
   （deepseek-flash 实测 ≈26.5 token）。证据是 `"Explain caching"` ——
   15 个 ASCII 字符，上游却回传 **32** prompt_tokens，差额与内容无关。
   这一项对短请求是绝对主导（5 字提问占 80%），漏掉它会让估算在短请求上偏小数倍。
   最终模型：`26.5（一次性） + 1.16×消息数 + 0.512×汉字数 + 0.188×其他字符数`，
   六组探针逐条回验误差 ≤1 token。
   > 注：此前本文件把这个偏差记作"中文低估 6.43 倍"——那个归因不准确。
   > 主导因素是固定开销没被计入，而非换算公式本身（28 个汉字其实只值 15 token，
   > 加上 29 的固定开销才成为 45）。归因错会导致修错地方。
   系数是配置项、由 `scripts/calibrate-token-estimator.py` 实测反推 ——
   **换供应商必须重新校准**（token 数由上游分词器与模板共同决定）。
   流式路径的账本仍优先采用上游末帧的真实 usage（`stream_options.include_usage`），
   只有拿不到时才回退估算 —— 回退值不冒充真实值。
5. **单价是示例值**：`gateway.pricing` 必须按自己的账单口径替换；也缺少日终对账任务。
6. **`/admin/*` 无鉴权**：只适合本机/内网，上公网前必须加。
7. ~~**重试与熔断尚未接上**~~ → **已接上（F1）**，但**跨 provider 故障转移还没做**：
   `UpstreamResilience` 按 provider 各持一套熔断器与重试（Resilience4j 2.2.0，
   编程式而非注解 —— provider 是配置里动态来的，注解要静态名字）。
   熔断打开后返回 **503** 并**不再发请求**（已用本地桩的上游计数验证）。
   链路上现在是 `timeout → CircuitBreaker → Retry → 真实上游调用`，
   熔断在内、重试在外，顺序不可颠倒（理由见 [step-19](./docs/step-19-retry-and-circuit-breaker.md)）。
   **F2（故障转移）未做**：熔断打开目前只返回 503，不会自动转移给下一个 provider。
8. **运行期日志是中文**：Windows 控制台默认 GBK(936)，Java 也按 GBK 输出，
   在 cmd 里正常；但管道/重定向被按 UTF-8 解码时会显示为乱码。
   面向开源协作者，后续应把启动与配置类日志改成英文（代码注释可以保留中文）。
9. ~~**上游是推理型模型，而网关还没为它调整失败语义**~~ → **已修（F0）**：
   当前验证用的 DeepSeek 两个模型响应里都带 `reasoning_content`，
   且 `max_tokens` 是**"推理 + 正文"的总预算**。实测（`scripts/probe-upstream-shape.py`）：

   | 模型 | 512 | 1024 | 2048 | 4096 |
   |---|---|---|---|---|
   | `deepseek-flash` | 空（仅推理） | 空（仅推理） | 217 字 | 1220 字 |
   | `deepseek-v4-pro` | 空（仅推理） | 1514 字 | 2483 字 | 2639 字 |

   这三条都已修掉：

   - **`finish_reason` 现在原样透传**。此前 `OpenAiProtocol` 把它**硬编码成 `"stop"`**，
     于是 `finish_reason=length` + 空正文（推理模型最常见的失败形态）
     会被网关主动报成"正常结束"—— 客户端连"这次被截断了"都无从判断。
     流式侧同样补上了**收尾帧**（`delta: {}` + 非 null 的 `finish_reason`）。
   - **空正文与截断现在分开计数**（`llmgw.upstream.empty` / `.truncated`）。
     两者单独出现时含义相反：前者是上游故障信号该告警，后者是客户端
     `max_tokens` 设小了该找调用方。失败谓词（供后续重试/故障转移使用）
     由 `UpstreamOutcome` 给出，口径只有一处定义。
   - **配额改成 reserve-then-settle**（预扣 + 按真实 usage 结算补扣）。
     此前只有预扣没有结算，配额记的永远是"prompt 估算之和"，
     而实测 completion 是 prompt 的 **20~100 倍**（prompt 33~87 → completion 755~3516）——
     真机验证：一次请求的配额增量 **2498 == 账本真实 tokens 2498**（F0 之前只会记 34）。
     缓存命中路径**全额退还预扣**（此前命中省了钱却照样吃配额）。

   > 已知代价：reserve-then-settle 天然允许**小幅超发** —— 真实值要等上游返回才知道，
   > 那时钱已经花了。这是选这个口径时知情接受的，不是缺陷；
   > 要收紧应改用"按 `max_tokens` 上界预留"，而不是在结算时补一道闸。

10. **上游帧形状的兼容性只覆盖了实测到的那几种**：新增的上游收尾帧处理是在
    DeepSeek 的帧形状上验证的。不同供应商对"末帧"的表达不一致
    （有的把 `finish_reason` 与 `usage` 放同一帧，有的分两帧，有的不发 `usage`）。
    解析层已经用了 `flatMapIterable` 允许一帧映射出多个分片，
    但**换供应商仍应重新跑一遍 `scripts/check-f0-failure-semantics.py`**。

### F1 新增的边界

11. ~~**重试的中间几次尝试不计入账本**~~ → **已修（step-22）**：
    被重试 / 转移丢弃的尝试现在各自落一条账，并计入配额结算。
    判据是"**下一次尝试开始**"—— 拿到响应的那一刻还不知道会不会重试，
    等重试算子重新订阅才算证明它没被交付；而最后一次尝试若被 F0 还原成最终答案，
    则由正常路径落账，两条路互斥，不会重复记账。
    **仍有一条限制**：流式路径不补 —— 被丢弃尝试的 usage 在末帧到达之前无从得知。
    调大 `max-attempts` 前仍然要想清楚故障期的账单，理由没变。
12. **重试与熔断的参数是全局一份，没有 per-provider 覆盖**：当前只部署 1 家 provider，
    "哪家更爱抖"在跑够数据之前说不清。提前加一层只会让"为什么这家没重试"
    多一个排查维度。熔断器/重试**实例**本身是按 provider 分的，只是参数共享。
13. **上游 429 不透传为 429**：上游限流被统一归到 502。不透传是因为我们自己的
    429 有明确含义（**本租户**配额用尽），混进上游的限流会让客户端分不清该找谁。
    同理，上游任何非 2xx 都报 502，不透传原始状态码 ——
    网关能自己识别的调用方错误（`messages` 为空、模型不在服务范围）在进上游之前就拦掉了。
14. **不读 `Retry-After`**：429 的退避走统一指数退避；上游若给了很长的 `Retry-After`，
    我们会在它说的窗口之前就重试。
15. **流的"中途卡住"没有防护**：`Flux.timeout()` 只约束**首片**
    （这是刻意的 —— 给整条流设超时会拦腰砍断长回答）。首片之后若上游停止发送
    但不断开连接，请求会一直挂着。要覆盖它需要按分片做空闲超时，属另一个改动。
16. **熔断打开时无法自动恢复被拒请求**：上层不重试、不排队，客户端自己退避。
    这是 F1 的刻意范围 —— F2 已把"转移给下一个 provider"接上，
    但**只在模型配了第二家部署时**才有效（见下方 F2 边界 17）。

### F2 新增的边界

17. **只有一家部署时，F2 的配置块完全无效**。`failover.enabled` 打不打开都一样：
    候选链只有一个元素，`nextCallable` 永远返回 -1。真机里"a 熔断打开后被跳过"
    这条只有在链里确实站着第二家时才成立。
18. **流式空正文不做转移**。流式的空正文只有到流结束才能判定，而那时收尾帧已经下发，
    再换一家重放会让客户端在"一个 `finish_reason`"之后又看到新正文 ——
    两段拼接的答案。保持 F0 的 `200 + length` 透传。
    （**非流式的空正文会触发转移**：它没有被下发过，换个部署重来一次没有副作用。）
19. **一次请求只预扣一次，但故障期可能真的花掉 N 次的钱** → 配额上限会被小幅突破。
    方向与 F1 一致：少扣可追、多扣不可逆。
20. ~~**中间尝试不入账本**（沿用 F1 边界 11）~~ → **已修（step-22）**：见边界 11。
    现在"一次请求的账本行数 = 真实上游调用次数"，这条口径可以直接对账。
    流式路径仍不补（被丢弃尝试没有 usage 可记）。
21. ~~**4xx 会计入熔断窗口**~~ → **已修（step-22）**：熔断器配了 `ignoreException`，
    4xx（429 除外）既不算成功也不算失败。此前一个客户端连发参数错误的请求
    就足以把一家健康的 provider 熔断，让所有租户一起收 503 ——
    这是"用健康信号响应客户端错误"的典型误用。
    **429 仍然计入**：它说的是"我现在服务不了你"，是 provider 侧的容量信号。
22. **候选顺序是静态的**：没有按健康度动态排序、没有权重、没有轮询。
    链首恢复要靠熔断器的 HALF_OPEN 探针（自动半开已开）逐步把流量拿回来。
23. **候选目标不做预检**：不会先探一下备选再决定，直接尝试。
24. **链首不可达时每个请求仍要先烧 3 次重试**，直到熔断打开
    （需要攒够 `minimum-number-of-calls` 次调用）。真机 A2 实测约 **5 秒**。
    调小 `minimum-number-of-calls` 能更快打开，但会更容易误判 ——
    这就是为什么 F1 的熔断阈值得先跑够流量再调。
25. **熔断状态不持久化**：进程重启后全部回到 CLOSED，故障期重启会有一段"重新学一遍"的窗口。
26. **流中途卡住仍无防护**（沿用 F1 边界 15）：`Flux.timeout` 的作用域只有首元素，
    首片之后一直不吐字不会触发任何超时 —— **因此也不会触发转移**。
    F2 没有把它变好，只是让"这个坑会挡住一次本可生效的转移"这件事变得具体。
27. **同一个请求可能前半段是上游错误、后半段是熔断**：熔断器会在**一次请求中途**翻到 OPEN ——
    真机 C 段就观察到第 3 个请求里 B 的重试进行到一半就够阈值，
    第 3 次尝试被当场拒绝，于是那一次请求的失败类型是 `circuit_open` 而不是 `upstream_error`。
    这是对的（网关真的没发那次请求），但**排查时别把两者当互斥**。

### 任务路由新增的边界

28. **任务表是静态配置**，不支持热更新。要热更得让 `db/schema.sql` 里那张一直空着的
    `model_route` 表接管 —— 它当初就是为"热更新 / 按租户差异化"留的。
29. **新增任务必须改代码**：推断规则写在 `TaskClassifier` 里，配置只能给已存在的任务换模型。
    目前不支持"纯配置加一个新任务"。
30. **只看最后一条 user 消息**：多轮对话里如果第一轮定了任务、后面都在追问，
    会被判成 `default-task`。这是刻意的（成本要 O(1)），不是漏实现。
31. **任务不影响采样参数**：`task=extract-json` 不会自动加 `response_format`、也不调温度。
    它只回答"用哪个模型"。
32. **没有 per-task 预算**：任务不参与配额。成本控制仍归 quota —— 两件事揉在一起，
    排查时说不清是"选错模型"还是"配额算错"。
33. **缓存命中率会下降**：同一问题在"声明 code"和"不声明"下走不同模型、不同缓存段。
    这是**正确行为**（不同模型的答案不能混用），但要预期。
34. **`classifier: model` 未实现**，配置它会在启动时直接失败。这是刻意的：
    静默退回规则会让配的人以为模型分类已生效。
35. ~~**响应头 `X-Route-Task` / `X-Route-Source` 未做**~~ → **已做（step-23）**：
    用 `RouteTrace` 回执传递，**没有**改 `ChatResponse`（22 处构造点）。
    非 ASCII 任务名降级为 `_`；路由解析失败时不设头。
36. ~~**账本没有 `task` 列**~~ → **已做（step-23）**：`cost_ledger.task` 与 `feature` 语义正交；
    未发生任务判定时写 `NULL`（不是空串——否则 `GROUP BY` 会造出一个假桶）。
37. ~~**`/admin/route` 只读端点未做**~~ → **已做（step-23）**：映射表取自运行时档位，
    附本进程 `task × source` 命中分布。
38. **被丢弃尝试的账本行 `latency_ms` 记 0**：单次尝试的耗时没有单独计时，
    而这一行的用途是成本、不是延迟分析（延迟由 `latency` 指标单独统计，口径是整条请求）。
    真要按尝试看延迟，得把计时器下沉到 `callUpstream` 里。
39. **流式缓存命中发的是"内容 + 收尾"两个分片**：`usage` 是 0/0、`finish_reason` 是 `stop`。
    这是对的（网关交出去的是完整答案、且没走上游），但**不要**用它反推上游的 token 消耗。
40. **`X-Route-*` 可能被反向代理丢弃或改写**：它是诊断头，不是契约。
    调用方不能拿它做计费或路由依据 —— 那边请用 `traceId` + 账本。
41. **`/admin/route` 的 `hits` 是进程内计数**：重启归零。跨重启的趋势归 Prometheus，
    两者用途不同，不要把两个数合成一个"总命中"。
42. **已有数据库必须手动应用 migration**：`db/schema.sql` 挂在 initdb 下，
    **只在数据目录为空时执行**。只改 schema.sql 会让现有环境写账本时静默失败
    （异常被吞成 warn）。见 `db/migrations/001-cost-ledger-task.sql` 与 `002-cost-ledger-provider.sql`。
43. ~~**账本没有 `provider` 列**~~ → **已做（P7）**：`cost_ledger.provider` 记**实际服务这次请求的那家**，
    与 `model` 是两列 —— 级联升级改 model 不改 provider，故障转移改 provider 不改 model。
    流式与非流式取 provider 的时机不同（`.map` vs `doOnSubscribe`），分别实现、分别验证。
44. **`provider` 记的是 adapter 的配置名**，不是上游返回的任何标识。若某家 provider 背后还是网关，
    真实后端是谁本网关不知道、也不该猜。
45. **全链路失败时账本不记**（没有 `usage` 可记）。所以"某家挂了"在 `/admin/ledger/by-provider`
    里表现为"它没出现"，而不是"它有 0 次"——两者的区别排查时要心里有数。
46. **`(none)` 桶会一直存在**：那是 P7 加列之前的账，**不做 backfill**。
    用 `model` 反推回去更糟——F2 之后一个 model 可能对多家，反推值一半是错的，且错得看起来很合理。
47. **`/admin/ledger/by-provider` 没有时间窗口参数**，是全量聚合。要"最近 7 天"得直接写 SQL。

---

## 踩过的坑（都是真实调试记录）

1. **5432 端口归属**——Postgres 容器映射到 5432 时连不上，`netstat` 一查发现宿主机原生 PostgreSQL 10 服务也占着 5432，请求被路由到了那个**没有 llmgw 库**的实例。映射到宿主侧其他端口解决。看起来像网络问题，实际是端口归属问题。
2. **宿主端口不能随便挑：55432 绑不上**——避让 5432 时先选了 `55432:5432`，某次重建容器却报
   `bind: An attempt was made to access a socket in a way forbidden by its access permissions`。
   端口明明没被任何进程占用，真因是 **55432 落在 Windows 动态端口排除区间 `55359-55458` 内**，
   docker 无权绑定。一条命令可自查：`netsh interface ipv4 show excludedportrange protocol=tcp`。
   最终定为 `15432:5432`；改端口映射必须同步改 `application.yml` 的 `spring.r2dbc.url` 默认值。
   （这条的教训：**端口"空闲"不等于"可绑定"**，被系统保留的端口永远绑不上。）
3. **Kafka "异步"发送拖死响应**——`KafkaTemplate.send()` 在 broker 不可达时会阻塞到 `max.block.ms`（默认 60s）才抛异常，导致每个请求多花 60 秒。压到 500ms 并把落账放到独立线程后，响应回到 3.4s。
4. **健康端点 503 不等于服务没起来**——Redis 未就绪时 `/actuator/health` 返回 503，用它做就绪判断会把"已启动"误判成"启动失败"。改用 TCP 端口探测。
5. **`SERVER__PORT` 端口劫持**——宿主环境注入的 `SERVER__PORT` 被 Spring Boot 的宽松绑定当成 `server.port`，服务起在了错误端口上，报 `Port <注入值> was already in use`。用命令行参数强制覆盖。
6. **`SynchronousSink` 一次只允许 `next()` 一个元素（F0）**——DeepSeek 的末帧**同时带 `usage` 与 `finish_reason`**，两个分支各发一次就把整条流打成 502（`Cannot emit more than one data`）。而在此之前它一直是好的：普通增量帧没有 usage、用量帧没有 choices，两个分支从未同时命中。**旧写法是靠数据形状的巧合在运行**。修法是 `handle` → `flatMapIterable`。
7. **`WebClientException` 没被状态码映射覆盖（F1）**——上游任何非 2xx 都曾落到 `500`。`WebClientResponseException` 继承 `NestedRuntimeException`，既不是 `IllegalStateException` 也不是 `IllegalArgumentException`，穿过所有分支掉进最后的兜底。**这个缺陷一直存在**，只是网关从不重试时上游一次 500 就直接穿过去，没人核对过状态码；重试让"重试耗尽"变成必然走到的路径之后，它才被真机断言抓出来。教训：**新的失败路径会把旧代码里从没被认真走过的分支变成主路径。**
8. **Mockito 的 `anyString()` 不匹配 `null`（F1）**——`ChatPipelineQuotaSettleTest` 里 8 条断言全红，报的却是一个看不懂的 NPE：管线用 `adapter.name()` 做熔断器实例键，而那个 mock 没桩 `name()` → 返回 `null` → `anyString()` 不匹配 → 桩未生效 → 走默认返回 `null`。一个方法返回值参与后续控制流时，漏桩它会以**另一个地方的 NPE** 的形式出现。
9. **候选链根本存不下来：数据结构先于算法（F2）**——`ProviderRegistry.byModel` 原本是 `Map<String, ProviderAdapter>`，用 `putIfAbsent` + `warn 忽略`，**同模型的第二家 provider 会被丢掉**。配置写对、启动日志也打了、请求也不报错，只是第二家永远用不到。如果先写完转移逻辑再来改这本就不会发现的：**单测会全绿而真机永远不转移**。教训：加"多候选"这类能力时，**先确认承载它的数据结构能表达出来**，再写算法。
10. **`onErrorResume` 的位置决定了转移能不能生效（F2）**——F1 把"空正文还原成响应"写在 `callUpstream` **内部**（`onErrorResume(UpstreamEmptyResponseException.class, e -> Mono.just(e.response()))`）。到 F2 就出事：**还原成一个成功值之后，外层拿到的就是成功，再没有机会去问下一家** —— "换个部署就能拿到正常答案"的请求被当成"上游明确说了它答不出"直接交付。`emptyAnswerTriggersFailover` 第一次跑就是这样红的（`predicate failed on value: content=, finishReason=length`）。修法是把还原从"单次调用内部"挪到**整条候选链的末端**（`terminal`）：**先穷尽转移，再还原响应**。

11. **同步抛出的异常绕过 `onErrorResume`（任务路由）**——`resolveRoute` 在 `execute` 里是**同步**调用的（发生在返回 Mono 之前），所以"未知任务"抛出的 `IllegalArgumentException` 不是"流里的错误"，控制器的 `onErrorResume` 根本不执行：异常冒泡给 Spring，客户端收到 **500**，而设计写的是 **400**。单元测试里它的表现是 **ERROR 而不是 FAILURE**（`StepVerifier` 连 Mono 都没拿到）——这一点很关键，如果只写 `assertThrows(IllegalArgumentException.class, ...)`，测试会全绿而线上仍然是 500。教训：**"异常类型对"和"HTTP 状态码对"是两件事**，断言必须打到 `OpenAiProtocol.statusOf(e)` 那一层。

12. **配置项存在，但没有代码读它（任务路由）**——`classifier: off` 是我自己写进方案与配置里的，代码却没读这个值；`classifier: model` 更是只有名字、没有实现。这类"配置存在但无效"比"配置项不存在"更难排查：用户改了配置、重启成功、行为不变，只会怀疑自己理解错了语义。修法是两头堵——`off` 真正实现（关推断、不关显式任务），其余取值在**启动时直接拒绝**并说明原因。
13. **验证脚本"假绿"：只跑了 3 条却打印"全部通过"（step-23）**——在改源码的同时跑了 `check-failover.py`：A 段用的是改之前编译好的类，B/C 段重启服务时编译失败，`phase()` 只打一行"本段中止"就返回；而汇总的判断是 `ok = bool(results) and all(good)` —— **没跑 = 没记录 = 不影响 ok**。于是 10 条断言一条没执行，结论却是绿的，退出码还是 0。修法三件套：分段启动失败写一条 FAIL 记录、断言条数少于下限直接判失败、`raise SystemExit(main())` 让退出码反映结果。教训：**验证工具本身也需要被验证**；假绿比没有脚本更危险，因为人会把它当成"已验证"。
14. **判据里用到的字段，要先确认它真的在返回里**——G4 第一版用"账本 id 递增"判断新行，而 `/admin/ledger/recent` 的 SELECT 里根本没有 `id`，于是判据恒假、断言恒定失败。同一类写法反过来（`>= 0`）就会变成恒真，那才是真正危险的版本。改成用响应的 `traceId` 关联后，断言反而更强了：它同时证明响应与账本属于同一次请求。

---

## 文档

按步骤记录了完整的实现过程，每篇固定包含 **目标 / 改动 / 关键代码 / 验证方式 / 结果 / 坑 / 下一步**：

| 步骤 | 内容 |
|---|---|
| [01](./docs/step-01-gateway-bootstrap.md) | 工程底座：WebClient / 配置绑定 / Redis / Kafka |
| [02](./docs/step-02-upstream-adapter.md) | 上游适配：OpenAI 非流式 + SSE 流式 + embeddings |
| [03](./docs/step-03-chat-pipeline.md) | 编排管线：配额→缓存→路由→上游→级联→落账 |
| [04](./docs/step-04-semantic-cache.md) | 语义缓存：向量检索 + 防误命中 |
| [05](./docs/step-05-quota-lua.md) | 分布式配额：Redis + Lua 原子扣减 |
| [06](./docs/step-06-cost-ledger.md) | 成本账本与指标：Kafka 事件 + traceId |
| [10](./docs/step-10-java-build.md) | Maven 安装与 Java 侧编译验证 |
| [11](./docs/step-11-provider-config.md) | 接入真实模型：密钥落位与配置接线 |
| [12](./docs/step-12-live-integration.md) | **真机联调**：跑通全链路并修掉 6 个问题 |
| [13](./docs/step-13-redis-quota-ledger.md) | **配额实测 + 账本落库**：Lua 无超发、traceId 贯通 |
| [15](./docs/step-15-review-fixes.md) | 全量复查：修掉 5 个"静默失效"缺陷 |
| [16](./docs/step-16-reactive-multi-provider.md) | 响应式化 + 多 provider + 路由配置化（让"非阻塞"可 grep 验证） |
| [17](./docs/step-17-token-estimate-and-cache-layers.md) | **估算从"猜"变"算"**：`TokenEstimator` + 缓存拆两级 + MDC 贯通 |
| [18](./docs/step-18-reasoning-model-failure-semantics.md) | **推理型模型的失败语义**：`finish_reason` 透传 + 配额 reserve-then-settle |
| [19](./docs/step-19-retry-and-circuit-breaker.md) | **重试与熔断**：Resilience4j 算子嵌套 + 流式重试闸门 + 失败路径的配额结算 |
| [20](./docs/step-20-cross-provider-failover.md) | **跨 provider 故障转移**：候选链数据结构 + `failoverable` 谓词 + 请求级预算扣减 |
| [21](./docs/step-21-task-based-routing.md) | **按任务选模型**：三层判定 + 规则分类器 + 任务映射表 + 开关只能有一个 |
| [22](./docs/step-22-review-fixes-2.md) | **复查修复**：4xx 不再熔断 provider + 被丢弃的尝试也记账 + 流式收尾帧 + 计价档位 |
| [23](./docs/step-23-r2-observability.md) | **R2 留证**：路由响应头（RouteTrace）+ 账本 `task` 列 + `/admin/route`；顺带修掉脚本"假绿" |
| [24](./docs/step-24-provider-attribution.md) | **B 批 P7 链路归因**：账本 `provider` 列 + `/admin/ledger/by-provider`；流式与非流式分别取 provider |

---

## 技术栈

Java 17 · Spring Boot 3.3 · WebFlux / Reactor · Resilience4j 2.2 · Redis（Reactive + RediSearch）· PostgreSQL（R2DBC）· Kafka · Micrometer / Prometheus · Maven

---

## 相关仓库

- `llm-eval` —— LLM 评测与回归平台（三层打分器 + judge 校准 + CI 门禁），账本里的真实调用日志可作为它的语料
- `llm-dashboard` —— 可视化控制台，本仓 `src/main/resources/static/dashboard` 是它的构建产物

---

## License

MIT
