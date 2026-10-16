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

  韧性（F1，落在 ④ 与 ⑤ 的内侧，按 provider 各一套）：
        timeouts → CircuitBreaker → Retry → 真实上游调用
        · 熔断在内、重试在外 —— 熔断要看到每一次尝试，否则失败率被重试稀释
        · 熔断打开 → 503 + 不再发请求（这是它存在的唯一理由）
```

**模块划分**

```
src/main/java/com/llmgw/
├── api/                 # OpenAI 兼容接口 + DTO + 只读运维接口
│   └── dto/             # ChatRequest / ChatResponse / Usage / ChatMessage
├── core/
│   ├── pipeline/        # ChatPipeline：一次请求的完整编排
│   ├── protocol/        # ProviderAdapter 抽象 + OpenAiAdapter 实现
│   ├── router/          # RouterService / RoutingPolicy / ModelTier 分级路由
│   ├── resilience/      # UpstreamResilience：按 provider 的重试 + 熔断（F1）
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
| GET | `/admin/ledger/recent?limit=` | 账本明细（limit 收口 1~200，避免被当全表导出用） |
| GET | `/admin/ledger/summary` | 账本按租户汇总（对账口径） |
| GET | `/admin/quota/{apiKey}` | 指定租户当前周期的配额用量 |
| GET | `/admin/circuit` | 各 provider 的重试 / 熔断状态与失败率（F1） |
| GET | `/dashboard` | 可视化控制台（同源托管） |
| GET | `/actuator/prometheus` | Prometheus 指标（含 `llmgw_cache_hit` 等自定义指标） |

`/admin/*` **全部只读**——不扣配额、不写账本、不改配置，与 `/v1` 主链路完全隔离，挂了也不影响对外服务。

**错误状态码约定**（`/v1/chat/completions`）：

| 状态码 | 场景 |
|---|---|
| 400 | 请求不合法——`messages` 为空、`model` 不在服务范围（`error.type = invalid_request_error`） |
| 429 | 配额超额（`error.type = rate_limit_exceeded`） |
| 502 | 上游/适配层异常：连不上上游、上游返回非 2xx、上游报错、空响应体（`error.type = upstream_error`） |
| 503 | **熔断器打开**——请求根本没发给上游（`error.type = circuit_open`，F1） |
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

---

## 已知边界（诚实清单）

1. ~~**多供应商未实现**~~ → **已修**：适配器不再单例，由 `ProviderRegistry` 按配置为每家 provider
   各建一个实例，并建立 `model → adapter` 索引。同名模型先声明者胜（顺序即配置文件顺序）。
   启动时逐条打印注册结果，配置写错在日志里就能看见。
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

11. **重试的中间几次尝试不计入账本**：账本只为"最终交给客户端的那一次"落一条
    （不变量 C 的口径）。一次重试 2 次的请求 = 3 次真实计费，账本记 1 次 ——
    成本归因在故障期系统性偏低，而故障期恰恰是重试最多的时候。
    配额侧是准的。**列为下一批待办**：每次尝试单独落账。
    另外注意**每次重试都是一次真实计费**，调大 `max-attempts` 前先想清楚故障期的账单。
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
    这是 F1 的刻意范围（F2 才会把"转移给下一个 provider"接上）。

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
