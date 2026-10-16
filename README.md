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

### 2. 语义缓存：命中"意思"，不是"字面"

同一个意图换种问法（"怎么理解语义缓存" vs "语义缓存是什么"），字面不同、向量相近。
`RedisSemanticCacheService` 走 RediSearch 向量检索，`similarity-threshold` 默认 0.92——
阈值可以调，但误命中比低命中率更贵（会给用户一个"看起来对但不相关"的答案），所以阈值取偏保守值。

> 当前配置下缓存是**关闭**的：DeepSeek 不提供 `/embeddings` 接口，拿不到向量。
> 底座（Redis Stack + RediSearch）已就绪，换成有向量能力的供应商即可打开。

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
| GET | `/dashboard` | 可视化控制台（同源托管） |
| GET | `/actuator/prometheus` | Prometheus 指标（含 `llmgw_cache_hit` 等自定义指标） |

`/admin/*` **全部只读**——不扣配额、不写账本、不改配置，与 `/v1` 主链路完全隔离，挂了也不影响对外服务。

**错误状态码约定**（`/v1/chat/completions`）：

| 状态码 | 场景 |
|---|---|
| 400 | 请求不合法——`messages` 为空、`model` 不在服务范围（`error.type = invalid_request_error`） |
| 429 | 配额超额（`error.type = rate_limit_exceeded`） |
| 502 | 上游/适配层异常（`error.type = upstream_error`） |
| 500 | 其他内部错误 |

> 早期实现把所有错误都写成 HTTP 200 + 响应体里的 `error` 字段。问题不在于"不够规范"：
> 客户端拿不到 429 就无法用状态码做退避决策，**监控的错误率曲线里也看不到限流**——
> 一个所有人都看不见的错误，等于没报。
>
> **400 与 502 的分界是"该不该重试"**：`messages` 为空、`model` 写错是**调用方**的问题，
> 重试一万次结果一样。把它归成 502 会让客户端误判成网关故障，去退避、去重试，白等一场。

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

### 重构后的验证（本轮）

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
> `python scripts/check-quota-concurrency.py`（配额并发，自带 6 条断言）。
> 两条脚本都会自己起服务、摘除端口劫持环境变量、跑完自动关服务。

> 复查这批缺陷的共同特征是**"静默失效"**：
> 缓存 key 少一个模型维度不会报错，只会让路由策略被悄悄架空；
> 落账同步执行也不会报错，只会在 Kafka 不可用时把最快的路径变成最慢的。
> 这类问题在监控上看起来一切正常。

---

## 已知边界（诚实清单）

1. ~~**多供应商未实现**~~ → **已修**：适配器不再单例，由 `ProviderRegistry` 按配置为每家 provider
   各建一个实例，并建立 `model → adapter` 索引。同名模型先声明者胜（顺序即配置文件顺序）。
   启动时逐条打印注册结果，配置写错在日志里就能看见。
2. ~~**`RouterService` 模型名硬编码**~~ → **已修**：候选表改为配置项
   `gateway.router.candidates`，并在启动时校验"每个候选都被某个 provider 支持"，
   不一致直接启动失败（把运行时的 502 前移成启动期报错）。`model=auto` 现在可用。
3. **语义缓存需要 embedding 能力**：DeepSeek 无 `/embeddings`，当前缓存关闭。
   未命中/失败一律走空 Mono，不阻塞主链路，也不会假装命中。
4. **配额估算严重低估中文请求 —— 实测 6.43 倍**：准入时按 `length/4` 估算预扣，
   而 `4 字符 = 1 token` 是**英文**的经验值。账本记的是上游真实 usage，两者一比就露馅：
   2026-10-16 实测，28 个汉字的请求预扣算作 **7 tokens**，上游真实 `prompt_tokens = 45`，
   比值 **6.43×**。也就是说一份 20 万 token 的配额，中文场景下实际能消耗约 **130 万** token。
   偏差不是"一点点误差"，而是**随语言变化的系统性低估**（英文请求的偏差小得多）。
   修法：待办 P5 `TokenEstimator`（按字符类别加权，或引入轻量 tokenizer）
   + 一次"用真实 usage 回填校正"的对账流程。
   流式路径的账本已改为优先采用上游末帧的真实 usage（`stream_options.include_usage`），
   只有拿不到时才回退估算 —— 回退值不冒充真实值。
5. **单价是示例值**：`gateway.pricing` 必须按自己的账单口径替换；也缺少日终对账任务。
6. **`/admin/*` 无鉴权**：只适合本机/内网，上公网前必须加。
7. **重试与熔断尚未接上**：响应式化只是**前置条件**（接口不再是同步签名，算子才有地方挂），
   `timeout` / `retryWhen` / 跨 provider 故障转移还没实现。
8. **运行期日志是中文**：Windows 控制台默认 GBK(936)，Java 也按 GBK 输出，
   在 cmd 里正常；但管道/重定向被按 UTF-8 解码时会显示为乱码。
   面向开源协作者，后续应把启动与配置类日志改成英文（代码注释可以保留中文）。

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

---

## 技术栈

Java 17 · Spring Boot 3.3 · WebFlux / Reactor · Redis（Reactive + RediSearch）· PostgreSQL（R2DBC）· Kafka · Micrometer / Prometheus · Maven

---

## 相关仓库

- `llm-eval` —— LLM 评测与回归平台（三层打分器 + judge 校准 + CI 门禁），账本里的真实调用日志可作为它的语料
- `llm-dashboard` —— 可视化控制台，本仓 `src/main/resources/static/dashboard` 是它的构建产物

---

## License

MIT
