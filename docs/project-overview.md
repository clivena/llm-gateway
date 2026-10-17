# 项目全景：为什么做、怎么做的、中间件怎么用

> 这不是 README 的替代品。README 回答"怎么用它"，这篇回答"**为什么长成这样**"。
>
> | 项    | 值                                                                              |
> | ---- | ------------------------------------------------------------------------------ |
> | 对应提交 | 2026-10-17（F3 任务路由 → step-22 复查修复 → R2 留证）                     |
> | 生产代码 | 44 个 Java 类 · 5868 行（`src/main/java`）                                          |
> | 测试代码 | 23 个测试类 · 200 passed / 0 failures / 0 errors                                   |
> | 真机脚本 | 10 个 Python 脚本（`scripts/`）                                                     |
> | 步骤文档 | 19 篇 step + 索引，共 3473 行（`docs/`）                                            |
> | 中间件  | Redis(+Lua/RediSearch) · PostgreSQL(R2DBC) · Kafka · Resilience4j · Micrometer |
>
> 文中所有数字都可复现。复现方式见第 7 节。



---

## 0. 一句话定位

一个 OpenAI 兼容协议的自建 LLM 网关：把**多模型接入、两级缓存、分布式配额、成本归因**收敛到一个入口，全链路响应式、无阻塞。

但这句话不是重点。重点在下一节。

---

## 1. 为什么做

动机分三层，从外到内。

### 1.1 业务层：三个绕不开的问题

让业务方各自 `curl` 上游模型，很快会遇到五件事：

| 问题   | 直接调用的后果           | 网关的做法                                   |
| ---- | ----------------- | --------------------------------------- |
| 成本黑洞 | 月底收到账单，不知道哪条业务线花的 | 每次调用落一条账本，带 `traceId`，可按租户/业务线汇总        |
| 预算失控 | 一个死循环脚本能把额度跑爆     | Redis + Lua 原子预扣，超额请求在**调用上游之前**被拦掉     |
| 重复计算 | 同一个问题被反复问，每次都付费   | 两级缓存：字面相同 / 意思相同都能命中                    |
| 模型锁死 | 换供应商要改所有业务代码      | OpenAI 兼容协议 + 模型分级路由，业务方只认 `model=auto` |
| 排查困难 | 出问题找不到是哪次调用的锅     | `X-Trace-Id` 从入口贯到账本，一次响应可反查它的全部开销      |

### 1.2 技术层：网关是"静默失效"最密集的地方

这是真正想练的东西。一次请求穿过六段，每一段失效都可以**不报错**：

| 失效形态      | 具体例子                                   | 为什么难发现                     |
| --------- | -------------------------------------- | -------------------------- |
| 缓存键维度缺失   | namespace 只用 `apiKey`，不带 `model`       | 强模型的答案被弱模型请求命中，路由策略被架空，无异常 |
| 落账同步执行    | 在响应路径里直接 `ledger.record()`             | Kafka 一挂，最快的路径变成最慢的；平时完全正常 |
| 失败语义被涂抹   | `finish_reason` 硬编码 `"stop"`           | 答案被截断了，客户端以为一切正常           |
| 估算公式错得离谱  | 配额按 `length/4` 估算中文                    | 中文低估约 6 倍，配额形同虚设           |
| 配置与代码各说各话 | 候选表写 `gpt-4o`，provider 只配了 `deepseek`  | 只打一次请求才会发现：100% 502        |
| 配置存在但无人读取 | 配置里写了 `classifier: model`，代码只实现 `rule` | 人以为能力生效了，实际静默走默认分支         |

**共同特征：监控上一切正常。** 这个项目的价值不在"又写了个转发服务"，而在于用**真机断言**把这类失效一条条逼出来。

### 1.3 工程层：四仓库闭环的入口

```
llm-gateway（入口 / 成本 / 配额）
      ↓ 被调用
llm-agent（RAG + 工具调用）
      ↓ 被评测
llm-eval（回归与上线门禁）
      ↓ 数据被消费
dashboard（可视化，构建产物同源托管在 /dashboard）
```

四者共用一套纪律：**每个数字都能自己复现**。README 里的 4.25s、0.000569 元、7 放行 13 拒绝，都不是设计目标，是脚本跑出来的。

---

## 2. 架构思路

四条，按重要性排。

### 2.1 分层：上层只依赖下层

| 层     | 包                             | 职责                                                        |
| ----- | ----------------------------- | --------------------------------------------------------- |
| 接入层   | `api`                         | OpenAI 兼容协议、只读运维接口、traceId 生成与透传                          |
| 编排层   | `core.pipeline`               | ①配额 ②缓存 ③路由 ④上游 ⑤级联 ⑥落账                                   |
| 能力层   | `core.*`                      | router · cache · quota · resilience · protocol（适配器 + 候选链） |
| 存储与观测 | `resources` / `observability` | Redis+Lua、PostgreSQL/R2DBC、Kafka、Prometheus               |

关键性质：**每一层都能单独被测**。`RouterService` 是纯函数（不碰 IO），所以任务路由的 20 条测试不需要起 Spring 容器；`TaskClassifier` 只依赖 `TokenEstimator`，17 条测试全在内存里跑。

### 2.2 一次请求的六段

| # | 阶段   | 输入                           | 输出 / 副作用                                   | 典型失败模式              |
| - | ---- | ---------------------------- | ------------------------------------------ | ------------------- |
| ① | 配额准入 | `apiKey` + 估算 prompt token   | Redis 预扣；超额直接 429                          | 并发超发（朴素 GET/SET 实现） |
| ② | 缓存查询 | 规范化后的最后一条 user 消息 + 模型       | 命中则直接返回，不调上游                               | 命中错答案（比漏命中更糟）       |
| ③ | 路由选择 | `model` / `task` / 全局 policy | 一个 `Route`（含 `ModelTier`）                  | 选到没有适配器的模型名         |
| ④ | 上游调用 | 模型 + 完整 messages             | `Mono<ChatResponse>` 或 `Flux<StreamChunk>` | 超时、5xx、空正文、连接失败     |
| ⑤ | 级联升级 | 上一次结果的质量分                    | 换更强档位再调一次                                  | 两次调用只落一笔账           |
| ⑥ | 成本落账 | 真实 usage + traceId           | 异步写 Postgres / Kafka                       | 同步写把响应拖住            |

流式与非流式**共用 ①②③**，差异只在 ④⑤⑥ 的返回形态。这条约束逼着把"判断逻辑"和"返回形态"分开——F3 的任务路由能一处改动让两条路径同时生效，就是因为这个结构。

### 2.3 新能力往外包，不往里插 `if`

想加"失败换一家"，正确做法不是：

```java
// 错误示范：在执行点插入分支
if (task == CODE) return callPro(...);
```

而是往外再包一层。当前的包裹顺序（从外到内）：

```
转移 · 候选链     这一家不行就换下一家 provider
└─ 重试           先把同一家试满（3 次）
   └─ 熔断        连续失败就不再发请求
      └─ 超时     只管首片，不拦腰砍长回答
         └─ 上游调用   只有这里产生费用
```

**顺序不可换，两个理由：**

- **熔断在内、重试在外**：熔断必须看到每一次尝试。反过来（熔断在外），重试的失败会被折叠成一次失败，失败率被稀释，熔断永远不会打开。
- **转移在重试之外**：先把同一家的重试预算烧完，再换地方。真机断言证明过这点——桩 A 收到 **3** 次而不是 1 次。

按"在执行点插 `if`"做会怎样：任务路由会和四件事各打一次架，而且是静默地打——缓存 namespace（不变量 B）、级联判据（`qualityScore`）、候选链（`candidatesFor`）、配额结算（C/D/E）。

### 2.4 五条不变量

写在 `ChatPipeline` 类注释顶部，改代码时第一眼能看到。

|       | 不变量                    | 违反后的表现                |
| ----- | ---------------------- | --------------------- |
| **A** | 落账只能走异步（`recordAsync`） | Kafka 不可用时把整条响应拖住     |
| **B** | 缓存 namespace 必须带模型维度   | 强模型答案被弱模型请求命中，路由被架空   |
| **C** | 每一次真实上游调用都要落一条账        | 级联升级时两次费用只记一笔         |
| **D** | 每次预扣都必须结算，**包括失败路径**   | 熔断期配额被白吃（连续失败正好是触发条件） |
| **E** | 一次请求预扣一次，但**每家候选各自结算** | 花了 N 次钱、只结算 1 次       |

这五条不是设计时想出来的，是线上问题换回来的。评论里写明了来源。

### 2.5 只在"决策点"改代码

F3（按任务选模型）是最典型的例子。

任务是"选哪个模型"的**前置判断**，所以它只出现在 `RouterService.resolve()` 里，最终仍然返回一个 `ModelTier`（包在 `Route` 里）。执行路径一行没动——`dispatch` / `streamDispatch` 的第一行都是 `ModelTier tier = route.tier();`。

这样做的结果：缓存段、级联判据、候选链、配额口径**根本不知道任务存在**，所以不可能被改错。

---

## 3. 制作流程

### 3.1 七个阶段

真实过程是七个阶段，每个阶段的终点都不是"功能做完"，而是"**被真机打红一次**"。

| 阶段         | 做了什么                                                 | 关键产出             | 真机 / 复查暴露的问题                                                                  |
| ---------- | ---------------------------------------------------- | ---------------- | ----------------------------------------------------------------------------- |
| **0 底座**   | WebFlux + 配置绑定 + `ProviderAdapter` 抽象 + 管线骨架         | step-01/02/03/10 | JDK 24 下 byte-buddy 版本不兼容，Mockito 直接失效                                        |
| **1 三个机制** | Lua 配额 / 语义缓存 / 账本 + traceId                         | step-04/05/06    | 单测全绿，但全是"设计意图"，没有一条能证明真的生效                                                    |
| **2 接真机**  | 密钥落位，打真实 DeepSeek                                    | step-11/12/13    | **6 个问题**：候选表写死 `gpt-4o` 而 provider 只有 deepseek（auto 必 502）、SSE 分片格式、错误码全 200 |
| **3 配额实测** | 60 并发冲配额、账本落库                                        | step-13          | 原子性成立（30 放行 / 30 拒绝）；traceId 与库里 3/3 对应                                       |
| **4 全量复查** | 逐行审代码                                                | step-15          | **5 个静默失效**：超额仍返回 200、缓存命中仍落账、级联只落一笔、缓存不按模型隔离                                 |
| **5 重构**   | 响应式化 / 多 provider / 路由配置化 / `TokenEstimator` / 缓存拆两级 | step-16/17       | 上游是**推理型模型**：`finish_reason` 恒 stop、正文可能为空、`max_tokens` 是"推理+正文"总预算           |
| **6 韧性**   | F0 失败语义 → F1 重试熔断 → F2 跨 provider 转移 → F3 任务路由       | step-18/19/20/21 | `WebClientResponseException` 落到 500；**同步异常绕过 `onErrorResume`**                |

### 3.2 背后的循环

```
写实现
  → 写断言脚本
    → 真机跑
      → 断言打红
        → 挖根因（不是改断言）
          → 改设计
            → 补文档
              → 把被推翻的旧结论登记进「推翻表」
```

第 5 步最容易偷懒。"打红了就把断言放宽"能让 CI 立刻变绿，但 bug 还在。这个项目里每次变红都改了设计，没有一次是改断言。

### 3.3 文档策略：保留错误

`docs/README.md` 里有一张 **「已被后续改动推翻的结论」表**（14 条）。例如：

> step-03 写着"`RoutingPolicy.TASK_BASED` 是按任务选模型的入口"  
> → **已删除**：实现只是 `candidates.get(0)`，从没接过任务信息，零测试。任务路由是"先判任务再查表"的前置判定，不是候选表上的一种排序方式。

**保留错误比修正错误更有信息量**——它记录了当时为什么那么想，以及后来被什么证据推翻。改历史数字会让"指标怎么变过来的"这条线索断掉。

---

## 4. 中间件

### 4.1 总表

| 组件                      | 在这里做什么               | 为什么是它                             | 关键坑                                |
| ----------------------- | -------------------- | --------------------------------- | ---------------------------------- |
| Reactor / WebFlux       | 全链路非阻塞               | 重试熔断是**算子**，只能挂 `Mono`/`Flux`     | `.block()` 残留必须为 0                 |
| Redis (redis-stack)     | 配额计数、L1 精确缓存、L2 向量检索 | 要"原子计数 + TTL + 向量检索"三合一           | 普通 Redis 无向量检索                     |
| Lua                     | 配额"读-判-写"原子化         | `WATCH/MULTI` 要客户端重试；Lua 单线程一次执行完 | 结算脚本要处理周期翻篇与下限保护                   |
| PostgreSQL + R2DBC      | 成本账本，可 SQL 对账        | 账本要能随手查；JDBC 会占住 Netty 事件循环       | 宿主端口 5432 被本机 PG10 占用              |
| Kafka                   | 账本流式链路（生产形态）         | 落账解耦，消费者再入仓                       | `send()` 阻塞到 `max.block.ms`，默认 60s |
| Resilience4j            | 按 provider 的重试与熔断    | provider 是配置动态来的，注解要静态名字          | 漏配 `minimum-number-of-calls`       |
| Micrometer + Prometheus | 缓存命中、重试、熔断、转移、路由任务   | 让"静默失效"变成能看到的东西                   | 缺 registry 时端点直接 404               |
| Docker Compose          | 拉起 Redis / Postgres  | 一条命令起依赖                           | 端口映射必须与配置文件同步                      |

### 4.2 逐个说明

#### Reactor / WebFlux

**角色**：整条链路的执行模型。适配器接口每个出网方法都返回 `Mono`/`Flux`，缓存接口也是。

**为什么这不是"性能优化"而是硬前提**：`timeout()` / `retryWhen()` / `circuitBreaker()` 都是**算子**，只能挂在响应式类型上。接口一旦是同步的、内部靠 `.block()` 取结果，超时、重试、熔断就全都无处可挂。

**失败模式**：WebFlux 的 Netty 事件循环线程数很少，任何一处同步阻塞都会连锁拖慢全部并发请求。所以连写数据库都不允许占用事件循环线程。

**代价与取舍**：全仓 `.block()` 出现次数为 **0**。唯一的 `subscribeOn` 在 `ChatPipeline.recordAsync`——它不是兜底，是把一个**已知会阻塞的边缘操作**（`KafkaTemplate.send()`）隔离出去，主链路不等待它。

> 这行 `.block()` 是主动加上去的：`WebClient` 本来就返回 `Mono`，去掉它代码不是变长，是变短。

#### Redis + Lua

**角色**：配额用量的唯一事实源。

朴素实现是 `GET` 读、判断、再 `SET` 写回——两个请求并发时会双双通过判断，**超发**。把"读-判-写"整体塞进一段 Lua 交给 Redis 单线程执行，天然原子：

```lua
local used = tonumber(redis.call('GET', KEYS[1]) or '0')
local need = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])

if used + need > limit then
  return 1                      -- 超额：拒绝，不写、不调上游
end
redis.call('INCRBY', KEYS[1], need)
redis.call('EXPIRE', KEYS[1], ttl)
return 0                        -- 放行并预扣
```

**关键点**：判断用 `used + need > limit` 的**量级比较**，不是"数次数"。所以配额能以 token 为单位控制。

验证用的是"上限 50 / 单次预扣 7 / 20 并发"—— **7 除不尽 50**，第 8 次算出 `49+7=56 > 50` 被拒。这条测试选 7 而不是 5，就是为了证明"扣的是量，不是次数"。

#### PostgreSQL + R2DBC

**角色**：成本账本，`cost_ledger` 表。每次真实上游调用一行，带 `trace_id` / `model` / `feature` / `prompt_tokens` / `completion_tokens` / `cost`。

**为什么不是 Kafka 就够了**：Kafka 是流，做不到"随手 SQL 对账"。默认 `cost.sink: postgres`，生产可切 `kafka` 或 `both`。

**为什么是 R2DBC 不是 JDBC**：JDBC 是阻塞 IO，会占住 Netty 事件循环线程。

**坑（端口）**：宿主端口最终定在 **15432**。两个原因：

1. 5432 被本机 PostgreSQL 10 的 Windows 服务占着，连过去会打到那个原生实例（没有 `llmgw` 库）→ 报 Connection reset
2. 曾用 55432，落在 Windows 动态端口排除区间 **55359-55458** 内，docker 绑不上（`bind: forbidden by its access permissions`）

端口看起来"没被占用"，但 docker 就是绑不上 —— 只能绕开。

#### Kafka

**角色**：账本的流式链路。

**最大的坑**：`KafkaTemplate.send()` 在 broker 不可达时会阻塞到 `max.block.ms`（默认 **60000ms**）才抛 `TimeoutException`。所谓"异步发送"因此会把响应一起拖住——**实测每个请求多花 60 秒**。

修法两步，缺一不可：

1. `max.block.ms: 500` —— 让它在不可用时快速失败
2. `ChatPipeline.recordAsync` 用独立线程 —— 彻底离开响应路径

只做第 1 步，响应仍会被拖 500ms；只做第 2 步，线程池会被 60 秒的调用占满。

#### Resilience4j

**角色**：按 provider 各持一套 `CircuitBreaker` + `Retry`。

**为什么是编程式而不是注解**：`pom.xml` 里刻意只引了 `circuitbreaker` / `retry` / `reactor` 三个模块，没有引 `resilience4j-spring-boot3`。三条理由：

1. 注解模式要求熔断器名字在**编译期**已知；本项目的 provider 是 `gateway.providers[]` 动态配的，名字只能在启动期算出来
2. 注解依赖 AOP 代理，而主链路返回 `Mono`/`Flux` —— 代理 + 响应式是经典坑区（自调用失效、类型被擦成 `Object`）
3. F2 需要按 provider 名查询"下一个候选的熔断器还能不能用"，编程式 `Registry` 能直接拿到状态对象

**代价**：少一个 `resilience4j` 自带的健康检查端点，已用 `/admin/circuit` 补上。

**坑**：漏配 `minimum-number-of-calls` 时，启动后头 2 个请求失败（失败率 100%）就会直接熔断 —— 而 2 个样本什么都说明不了。这是熔断器最常见的误配置。

#### Micrometer + Prometheus

**角色**：把内部状态外化。自定义指标包括 `llmgw.cache.hit`、`llmgw.upstream.retry`、`llmgw.circuit.open`、`llmgw.failover`、`llmgw.route.task`。

**为什么 `llmgw.route.task` 要带 `source` 标签**：同为 `code` 任务，"客户端明说的"与"网关猜的"含义完全不同 —— 前者是契约，后者可能判错，**这两件事的修法相反**。

**坑**：缺 `micrometer-registry-prometheus` 时 `/actuator/prometheus` 直接 404，不报错也不提示。

### 4.3 刻意没有引入的东西

没有用 Spring Cloud Gateway，没有用 Redis 分布式锁，没有用 ELK/Sentry。

理由一致：**每引入一个组件，就多一套失败语义要解释**。这个项目刻意把组件数压到"全部能讲清失败模式"的程度——这也是为什么每个中间件下面都有"坑"这一栏。

---

## 5. 关键判断清单

这几条是整个项目最值钱的部分，都能在文档里找到数据支撑。

| #  | 判断                                | 依据                                                                   |
| -- | --------------------------------- | -------------------------------------------------------------------- |
| 1  | 配额扣的是"量"不是"次数"                    | 上限 50 / 预扣 7 / 20 并发 → 7 放行 13 拒绝，终值 49                              |
| 2  | 缓存拆两级，因为"静默失效比不做更糟"               | 只有 L2 时，DeepSeek 无 `/embeddings` → 缓存开着却零命中；L1 上线后命中快 250~400 倍      |
| 3  | 502 与 503 的分界是"我们试没试"             | 502 = 替你问了但答得不对，客户端该重试；503 = 不打算替你问，客户端该退避                           |
| 4  | 400 与 502 的分界是"该不该重试"             | 参数写错重试一万次结果一样；归成 502 会让客户端误判成网关故障                                    |
| 5  | 异常类型对 ≠ HTTP 状态码对                 | `IllegalStateException` 映射成 502；同步抛出的异常绕过 `onErrorResume` → 客户端收 500 |
| 6  | 显式模型必须最优先                         | 用户点名了模型，网关不能因为"这个任务更适合另一个"就换掉——那是未经同意的替换与计费                          |
| 7  | 未知的显式任务 → 400，未知的推断任务 → 落 default | 静默兜底最坏：调用方以为 `code` 生效了，实际走了 chat，只能翻账本才发现                           |
| 8  | 开关只能有一个                           | 同时存在 `policy=TASK_BASED` 与 `task-routing.enabled` → 四种组合里必有两种语义不明    |
| 9  | 配置存在但无效，比配置不存在更难排查                | `classifier: model` 写了但没实现 → 人以为生效了，实际静默走规则                          |
| 10 | 缓存最坏的失败模式是"错答案"，不是"漏命中"           | 所以 L1 只做三种语义绝对安全的规范化，刻意不剥标点                                          |



---

## 6. 现状与边界

### 6.1 已验证

| 项 | 结果 |
|---|---|
| 单测 | **200 passed / 0 failures / 0 errors**（`mvn -o clean test`） |
| 配额原子性 | 20 并发 / 上限 50 / 预扣 7 → 7 放行 13 拒绝，终值 49 |
| 缓存命中 | 命中快 250~400 倍，`usage` 全 0/0，三重交叉验证 |
| Token 估算 | 六组探针，估算/真实 = 1.00 / 1.00 / 1.03 / 1.02 / 1.00 / 1.01 |
| 重试与熔断 | 18/18 PASS（step-23 复跑），含"熔断后上游请求数 4→4 未变" |
| 跨 provider 转移 | 13/13 PASS（step-23 复跑），含"桩A 收到 3 次"证明重试在内、转移在外 |
| 任务路由 | 13/13 PASS，含响应头、账本 `task` 列、`/admin/route` |

### 6.2 已知边界（诚实清单）

完整清单在 README「已知边界」，这里只列影响判断的：

- **单价是示例值**：`gateway.pricing` 必须按自己的账单口径替换，且缺少日终对账任务
- **`/admin/*` 无鉴权**：只适合本机/内网，上公网前必须加
- **`classifier: model` 未实现**：写了这个值会启动失败，不会静默降级
- **运行期日志是中文**：Windows 控制台 GBK 下正常，管道按 UTF-8 解码会乱码
- **`X-Route-*` 是诊断头，不是契约**：可能被反向代理丢弃或改写；计费/路由依据请用 `traceId` + 账本

> 任务路由真机脚本曾在 F3 交付时**未实跑**（Docker 被沙箱挡），当时代码与文档都标了"未实跑"，
> 没有写成"已通过"。容器恢复后补跑 **13/13**，README 与本节同步改成了实际结果。
> **没跑过的东西不写数字**，这是这个项目唯一不能妥协的规矩。

---

## 7. 复现方式

```bash
# 1) 起依赖（Redis + Postgres）
docker compose up -d redis postgres

# 2) 配密钥
cp config/application-local.yml.example config/application-local.yml
# 填入自己的 key

# 3) 全量单测
mvn -o clean test                      # 期望 200 passed

# 4) 真机联调（自己起服务、跑断言、收指标、关服务）
python scripts/run-gateway-smoke.py
```

| 脚本 | 验证什么 | 断言数 |
|---|---|---|
| `run-gateway-smoke.py` | 主链路 + 流式 + 指标 | — |
| `check-quota-concurrency.py` | 配额原子性 | 6 |
| `calibrate-token-estimator.py` | 估算精度 | 6 组探针 |
| `check-cache-layers.py` | 缓存是否真的命中 | 8 |
| `check-f0-failure-semantics.py` | 失败语义 + 配额结算 | 8 |
| `check-resilience.py` | 重试 + 熔断 | 18 |
| `check-failover.py` | 跨 provider 转移 | 13 |
| `check-task-routing.py` | 按任务选模型 + 留证 | 13 |
| `probe-upstream-shape.py` | 上游到底返回了什么 | — |

---

## 附录 A：目录结构

```
src/main/java/com/llmgw/
├── api/                 # OpenAI 兼容接口 + DTO + 只读运维接口
│   ├── OpenAiProtocol # 请求/响应/错误的协议形状与状态码映射
│   └── dto/           # ChatRequest / ChatResponse / Usage / ChatMessage
├── core/
│   ├── pipeline/       # ChatPipeline：一次请求的完整编排（865 行）
│   ├── protocol/       # ProviderAdapter 抽象 + OpenAiAdapter
│   │                   # ProviderRegistry：model → 候选链（顺序即优先级）
│   ├── router/         # RouterService / TaskClassifier / Route / TaskDecision
│   ├── resilience/     # UpstreamResilience：按 provider 的重试 + 熔断
│   ├── cache/          # 两级缓存：ExactCacheLayer + RedisSemanticCacheService
│   ├── quota/          # QuotaService：Redis + Lua 原子预扣与结算
│   ├── cost/           # CostLedgerService（sink 可切）+ R2DBC Repository
│   └── token/          # TokenEstimator：实测校准的估算模型
├── config/             # 配置绑定、WebClient、dashboard 托管
└── observability/      # TraceIdFilter / GatewayMetrics / MDC 贯通
src/main/resources/
├── application.yml     # 全部策略可配
├── lua/quota.lua       # 配额原子脚本
├── lua/quota_settle.lua# 结算脚本（补扣 / 退还 / 周期翻篇）
└── static/dashboard/   # 可视化控制台构建产物
```

## 附录 B：两个容易混的字段

| 字段 | 含义 | 用途 |
|---|---|---|
| `feature` | 业务线标签（如 `resume-tailor`） | **只用于成本归因**，落 `cost_ledger.feature` |
| `task` | 这次请求需要什么能力（如 `code` / `summarize`） | **只用于选模型**，不落账 |

两者语义正交，不能互相顶替：同一个业务线（`feature`）里可能既有写代码也有总结；同一个任务（`task`）可能来自多条业务线。
