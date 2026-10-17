# step-23 · R2：让"选了哪个模型"事后可证 —— 响应头 / 账本 task 列 / `/admin/route`

> 前置：F3（step-21）把"按任务选模型"做进了决策点，R1 的全部断言都是**在线内**看的：
> 上游收到了哪个模型、指标有没有涨。这一轮补的是**事后**：
> 请求结束之后，客户端、账本、运维面板能不能各自独立地回答"这次走的是哪个任务"。
>
> R1 与 R2 的分界是一句话：**R1 决定选谁，R2 负责留证。**

---

## 1. 为什么"留证"要单独做一轮

F3 交付时，一个任务判定错误的表现是这样的：

- 请求 HTTP 200，答案看起来正常；
- 账本里有一条记录，金额正确、traceId 对得上；
- 指标 `llmgw.route.task` 涨了，但只有拉 Prometheus 的人看得到；
- 客户端拿到的 body 是纯 OpenAI 形状，**没有任何字段说明这次走了哪个任务**。

于是"为什么这个请求走了贵模型"这个问题，只能靠翻服务端日志。而日志有两个现实问题：
它是**给人看的**（格式随时会变，也没有稳定契约），且**不跟着请求走**
（客户端拿着自己的 traceId 来问，得先能从响应里拿到 traceId）。

三处证据各有不可替代的读者：

| 证据 | 读者 | 回答的问题 |
|---|---|---|
| 响应头 `X-Route-Task` / `X-Route-Source` | 调用方 | "我这次请求被当成什么任务了？是我声明的还是网关猜的？" |
| `cost_ledger.task` | 财务 / 成本归因 | "这个月 code 任务花了多少钱？" |
| `/admin/route` | 排障的人 | "现在这张映射表配成什么样？实际在往哪边倒？" |

少了任何一处，另外两处都补不上：账本回答不了"这一次"，
响应头回答不了"总共"，面板回答不了"这一条 trace"。

---

## 2. 一个核心判断：诊断信息不进 body

### 2.1 不碰 `ChatResponse`

最直觉的做法是给 `ChatResponse` 加两个字段，让响应体里带上 `task` / `source`。
没有这么做，两个理由：

1. **它是 record，全仓 22 处构造点**（大半在测试里）。为了两个**只用于诊断**的字段
   去改一个对外协议对象，等于让"运维需求"绑架"协议形状"。
2. **它不属于 OpenAI 协议**。`X-Cache-Hit` 当初就是这么处理的（协议类里的规则 A）：
   网关私有信息一律走响应头，body 保持纯 OpenAI 形状 ——
   否则"任何 OpenAI 客户端可直连"这个前提就破了。

### 2.2 不用 exchange attribute

`step-21` 的"下一步"里写的是"用 exchange attribute 做"。实际落地时改了主意：

`ChatPipeline` 在 `core` 层，`ServerWebExchange` 在 `web` 层。让核心管线认识 HTTP，
意味着以后换接入协议（gRPC / 消息队列）要重写判断逻辑 —— 而"判断逻辑与接入协议无关"
正是 `dispatch` 这条结构存在的理由。**依赖方向不能为了省一个参数就反过来。**

### 2.3 所以引入 `RouteTrace`：一个极小的回执

```java
public final class RouteTrace {
    private volatile Route route;
    public void record(Route route) { this.route = route; }
    public Route get() { return route; }
    public static RouteTrace ignored() { return new RouteTrace(); }
}
```

三个设计点：

- **可变对象，而不是返回值**：路由要在管线内部才算得出来（依赖任务判定与配置表），
  而响应头必须在写 body **之前**设好。一个可写的回执是这两件事之间最短的桥。
- **解析完成前是 `null`**：控制器据此**不设头**。设一个 `n/a` 会让人以为"任务路由没开"，
  而真实原因是这次请求根本没走到路由（参数错 / 未知任务）。
- **`ignored()`**：让"只关心管线跑通"的调用点不必为一个新参数改二十多处。
  三参数重载因此保留 —— 但有一条测试专门钉住它**不能退化成空实现**（`threeArgOverloadStillRunsThePipeline`）。

### 2.4 为什么**不**设 `X-Route-Model`

模型会在级联升级时变化。一个在请求开始就写下的头，会在升级后与"真正服务的模型"
不符 —— 一个看起来合理、实际错误的诊断信息，比没有更坏。
真实模型本来就在响应体 `model` 与流式分片的 `model` 里（F5 已保证按真实模型填）。

### 2.5 头值必须做 ASCII 清洗

HTTP 头是字节串，规范只保证 ASCII 可见字符可移植。配置里写
`tasks: {代码: ...}` 完全可能，而把一个非 ASCII 字符塞进头的后果，
轻则客户端解成乱码，重则 Netty 抛 `IllegalArgumentException` ——
一次**本来成功**的请求变成 500。

```java
sb.append(c >= 0x20 && c < 0x7F ? c : '_');
```

**诊断信息绝不能有让请求失败的权力。** 信息并没有丢：原始任务名在
`/admin/route` 与 route 日志里都是原文。约定上任务名应当是 ASCII 标识符。

---

## 3. 账本：`cost_ledger.task`

### 3.1 `feature` 与 `task` 是正交的两列

| 列 | 回答 | 谁写 |
|---|---|---|
| `feature` | 这条流量属于哪条业务线（`resume-tailor`） | 调用方 |
| `task` | 这次请求需要什么能力（`code` / `summarize`） | 网关判定 |

一个业务线里会有多种任务，所以不能用 `feature` 顶替 `task`。
而只记 `model` 也不够：多个任务可能映射到同一个模型，
那时"从 model 反推任务"直接失效 —— 而这正是想知道"该砍哪个任务的预算"的时刻。

### 3.2 没发生任务判定时写 `NULL`，不写空串

- 任务路由关闭 → `task IS NULL`（"没有发生判定"）
- 判定成 chat → `task = 'chat'`

两者在 SQL 里都能跑，但 `GROUP BY task` 时，空串会变成一个
**"名字叫空字符串的任务"**的桶，看到报表的人会得出不同结论。
`feature` 保持着历史上的空串行为没动 —— 改它等于改既有账本语义，不在本轮范围。

### 3.3 只改 `schema.sql` 会**静默失效**

`schema.sql` 挂在容器的 `/docker-entrypoint-initdb.d` 下，**只在数据目录为空时执行**。
已经跑起来的库永远不会再读它一遍。于是"只改 schema.sql"的结果是：

- 新克隆的人能跑；
- 现有环境每次写账本都在 SQL 层失败；
- 而 `CostLedgerService` 的铁律是"账本失败不能影响用户请求"——
  异常被吞成一条 `warn`。

**表现就是"账本悄悄少了一半数据"，没有任何红色信号。**
所以本轮加了 `db/migrations/001-cost-ledger-task.sql`（幂等：`ADD COLUMN IF NOT EXISTS`），
并且必须手动应用一次：

```bash
docker compose exec -T postgres psql -U postgres -d llmgw \
  < db/migrations/001-cost-ledger-task.sql
```

> 这个项目没有迁移框架，靠的是**每条迁移语句自己幂等**。
> 比"用版本表记录执行过没有"更土，但它不依赖任何额外组件，
> 而引入 Flyway/Liquibase 会多一套要解释的失败语义。

### 3.4 `task` 用 `bindNull` 绑定

```java
spec = task == null ? spec.bindNull("task", String.class) : spec.bind("task", task);
```

不能写成 `task == null ? "" : task` —— 那样 3.2 的 NULL 语义就白设计了。
这一处也是单测里最容易"看不出来"的地方：mock 只记到"调用了一个方法"。

---

## 4. `/admin/route`

### 4.1 映射表来自运行时，不是配置回显

端点读的是 `RouterService.taskTable()` —— 启动时**从候选表反查出来的 `ModelTier`**，
带 `tier` 与 `qualityScore`。如果直接回显配置里的 `{code: deepseek-pro}`，
它就无法回答"这个任务的档位质量分是多少、级联会不会生效"，
而排障时恰恰要确认"配置生效成什么样"，不是"配置文件写了什么"。

### 4.2 命中分布必须带 source，且要写清口径

```json
{
  "enabled": true,
  "defaultTask": "chat",
  "classifier": "rule",
  "policy": "COST_FIRST",
  "tasks": { "code": {"model": "stub-pro", "tier": "STRONG", "costPer1k": 0.008, "qualityScore": 0.9} },
  "hits": { "code": {"explicit": 4, "inferred": 2} },
  "hitsScope": "since-process-start"
}
```

- 带 `source`：同为 `code`，"客户端明说的"与"网关猜的"含义完全不同 ——
  前者的修法是改调用方，后者是改判定规则。只看 task 总数，分不出是配置生效了
  还是规则太激进。
- `hitsScope` 写在**响应里**而不是只写在文档里：拿到这份数据的人未必读过 README，
  而"累计还是当前"直接决定他会不会拿它去做趋势判断。
  跨重启的趋势归 Prometheus —— 两者的用途不同，不要合成一个数。
- `TreeMap` 排序：面板上的任务顺序稳定，不然每次刷新都在跳。

---

## 5. 验证

### 5.1 单元测试

`mvn -o clean test` → **200 passed / 0 failures / 0 errors / BUILD SUCCESS**（186 → 200，+14）。

| 新增测试类 | 数量 | 钉住的东西 |
|---|---|---|
| `ChatPipelineRouteTraceTest` | 5 | 流式 / 非流式都写回执；未知任务**留空**；三参数重载不是空实现 |
| `ChatControllerRouteHeaderTest` | 4 | 两个头真的被写出去；失败时不设头；非 ASCII 降级为 `_` 且请求仍 200 |
| `CostLedgerServiceTest` | 3 | task 到仓储、到 Kafka 事件；缺省必须是 `null` 不是 `""` |
| `AdminControllerRouteTest` | 2 | 映射表带真实档位；hits 按 source 分组；口径字段；关闭时仍可读 |

### 5.2 真机：`check-task-routing.py` → **13/13 PASS**

在原有 8 条（A~F 段）之上新增 G 段 5 条：

```
✓ G1 非流式响应头带 X-Route-Task / X-Route-Source —— task=summarize source=explicit
✓ G2 推断来源可区分（inferred 而不是 explicit） —— task=code source=inferred
✓ G3 流式响应头同样带路由结果 —— task=code
✓ G4 账本按请求写入 task 列 —— trace=670c0482... 行={'task': 'code', 'model': 'stub-pro'}
✓ G5 /admin/route 给出映射表与命中分布 —— code→stub-pro chat→stub-flash hits.code={'explicit': 4, 'inferred': 2}
```

两条只有真机能给的证据：

- **G4 用这次响应的 `traceId` 去库里找那一行**，而不是"看最新一条"。
  它同时证明了两件事：响应与账本属于同一次请求；`task` 是跟着这次判定走的，
  不是碰巧被上一条请求写进去的。
- **G3 流式的头**：流式在 `writeAndFlushWith` 之后就冻结头部，没有第二次机会。
  单测能证明"调用了 set"，只有真机能证明"真的发出去了"。

### 5.3 另两条真机脚本的复跑

本轮改了 `UpstreamResilience`（step-22）与账本行数口径，两条历史脚本一并复跑：

| 脚本 | 结果 | 备注 |
|---|---|---|
| `check-failover.py` | **13/13 PASS**（exit 0） | A 段打真实上游（约 3s），B/C 段用桩 |
| `check-resilience.py` | **18/18 PASS**（exit 0） | A 段打真实上游，验"重试在真实空正文上触发" |

---

## 6. 一个工具链缺陷：脚本"全绿但其实什么都没跑"

这是本轮最值得记的一条，因为它差点让我把假结论写进文档。

### 6.1 现场

第一次串行跑 F2/F1 时，我同时在改源码。`check-failover.py` 的输出是：

```
[PASS] A1 链尾坏候选零代价
[PASS] A1' 面板列出两家候选
[PASS] A2 链首不可达透明转移

=== 结果: F2 验证全部通过 ✅ ===
F2_EXIT=0
```

**只跑了 3 条，另外 10 条（B/C 两段）一条都没执行。** 原因：A 段用的是改代码之前
编译好的类，B/C 段重启服务时编译失败，`phase()` 在 `wait_ready` 失败后
只打了一行 `[!] 服务未就绪，本段中止` 就 `return None`——

而汇总的判断是 `ok = bool(results) and all(good)`。
**没跑 = 没记录 = 不影响 ok。** 于是"什么都没跑"被读成了"全部通过"。

### 6.2 三个修法

1. **分段启动失败要写一条 FAIL 记录**，而不是静默返回：
   ```python
   results.append((name + " · 服务未启动", False, "编译或启动失败：本段断言一条都没执行"))
   ```
2. **断言条数下限**：全量跑应记录 13 条（跳过真实上游时 10 条），
   少于这个数直接判不通过。
   ```python
   expected = 10 if (SKIP_REAL or not real) else 13
   if len(results) < expected:
       log("  [!] 只记录了 %d/%d 条断言 —— 有分段没执行，不能算通过")
       ok = False
   ```
3. **退出码要反映结果**：两个脚本此前都是 `main()` 没有返回值、直接调用，
   即使全红也返回 0 —— 放进 CI 就是"永远成功"。现在改成 `raise SystemExit(main())`，
   依赖缺失/端口占用返回 2，断言失败返回 1。

> 教训：**验证工具本身也需要被验证。**
> 一个会假绿的脚本比没有脚本更危险 —— 没有脚本时人知道自己没验；
> 假绿会让人把事情标成"已验证"。

### 6.3 顺带发现的第二个假绿来源

G4 的第一版用"账本 id 递增"作判据，而 `/admin/ledger/recent` 的 SELECT 里**没有 id**。
于是 `max_id` 恒为 0、过滤条件恒假、断言恒失败（第一次 G4 FAIL）——
但同样的写法如果反向写（`>= 0`），会变成恒真。

**判据里用到的字段，必须先在返回里确认它存在。**
最终改用 traceId 关联，既修掉了这个坑，也让断言更强。

---

## 7. 已知边界（本轮新增）

1. **`X-Route-*` 可能被反向代理丢弃或改写**。它是诊断头，不是契约；
   调用方不能把它当作计费或路由的依据（真正的依据是 `traceId` + 账本）。
2. **非 ASCII 任务名在响应头里会变成 `_`**。原始值只在 `/admin/route` 与日志里。
3. **`hits` 是进程内计数，重启归零**。历史趋势看 Prometheus。
4. **`/admin/ledger/by-task` 的 `(none)` 是展示用的占位**，
   不是数据库里的真实值（真实值是 `NULL`）；用它做过滤时要写 `task IS NULL`。
5. **已有环境必须手动应用 migration**。没有迁移框架，也没有"启动时自动迁移"——
   这是刻意的取舍（见 3.3），代价是升级步骤里多一条命令。
6. **`task` 列没有进 `summaryByApiKey`**。按租户的汇总口径没变；
   要按任务看请用 `/admin/ledger/by-task`。
7. **`/admin/*` 仍然没有鉴权**（沿用 step-21 边界）。上公网前必须加。

---

## 8. 下一步

R2 完成。剩下的按价值排序：

- **R3（可选）**：`classifier: model` —— 异步分类、置信度阈值、低置信升级给 llm-eval 的 judge。
- **B 批余项**：P6 抽 `TenantService`（apiKey 同时是身份/凭证/日志字段）、
  P7 补"最终由哪家 provider 服务了这次请求"。
- **上公网前必做**：`/admin/*` 鉴权。
