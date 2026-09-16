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

---

## 验证过的数据

以下都是**真机跑出来的**，不是设计目标（原始记录见 `docs/step-12`、`docs/step-13`）：

| 项 | 结果 |
|---|---|
| 真实模型联调 | DeepSeek 全链路跑通：非流式 **3.4s**、流式**首字 1.1s**；双模型切换、SSE 逐字吐均正常 |
| 编译与单测 | Maven 3.9.11 + JDK 24 编译 23 个源文件，单测 **3 passed**，BUILD SUCCESS |
| 配额原子性 | 60 并发 ×1（上限 30）→ **30 放行 / 30 拒绝**，计数器精确停在 30；<br>20 并发 ×7（上限 50）→ **7 放行 / 13 拒绝**，终值 49 |
| 配额拦截延迟 | 超额请求 **29ms** 返回，未触及上游、未产生费用 |
| 成本账本 | Postgres 写入 3 行，`cost > 0`，合计 **0.000569 元**；响应头 traceId 与库中 `trace_id` **3/3 一一对应** |
| 失败计数 | `ledger_write=3 / ledger_failure=0` |

第二组配额测试是关键：**7 除不尽 50**，它证明逻辑是量级比较而非数次数——
第 8 次算出 `49+7=56 > 50` 被正确拒绝。

---

## 已知边界（诚实清单）

1. **多供应商未实现**：`OpenAiAdapter` 是单例，只读第一个名为 `openai` 的 provider。配置里加第二家不生效，需改成按 provider 列表构建多个 bean。
2. **`RouterService` 模型名硬编码**：`model=auto` 会挑到 `gpt-4o-mini` 而失败；显式传 model 可绕过。
3. **语义缓存需要 embedding 能力**：DeepSeek 无 `/embeddings`，当前缓存关闭。
4. **配额估算与真实 usage 有偏差**：准入时按 `length/4` 估算预扣，账本记上游真实 usage，实测偏差 **2.3 倍**（100 vs 233）。这是"准入速度"换来的代价，需要补一次"用真实 usage 校正"的流程。
5. **单价是示例值**：`gateway.pricing` 必须按自己的账单口径替换；也缺少日终对账任务。
6. **`/admin/*` 无鉴权**：只适合本机/内网，上公网前必须加。

---

## 踩过的坑（都是真实调试记录）

1. **5432 端口归属**——Postgres 容器映射到 5432 时连不上，`netstat` 一查发现宿主机原生 PostgreSQL 10 服务也占着 5432，请求被路由到了那个**没有 llmgw 库**的实例。改成 `55432:5432` 解决。看起来像网络问题，实际是端口归属问题。
2. **Kafka "异步"发送拖死响应**——`KafkaTemplate.send()` 在 broker 不可达时会阻塞到 `max.block.ms`（默认 60s）才抛异常，导致每个请求多花 60 秒。压到 500ms 并把落账放到独立线程后，响应回到 3.4s。
3. **健康端点 503 不等于服务没起来**——Redis 未就绪时 `/actuator/health` 返回 503，用它做就绪判断会把"已启动"误判成"启动失败"。改用 TCP 端口探测。
4. **`SERVER__PORT` 端口劫持**——宿主环境注入的 `SERVER__PORT` 被 Spring Boot 的宽松绑定当成 `server.port`，服务起在了错误端口上。用命令行参数强制覆盖。

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
