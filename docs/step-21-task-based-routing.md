# step-21 · 按任务选模型（任务路由）

> 目标：`model=auto` 不再只有"挑最便宜的"或"挑最好的"两种全局策略，
> 而是先判断这条请求要做什么，再按任务选模型。
>
> 关键约束：**只在"选哪个模型"这一步插入，不动"怎么调这个模型"**。

---

## 1. 目标与范围

三条边界（本次确认）：

| 决策 | 取值 | 理由 |
|---|---|---|
| 任务来源 | 显式声明 + 规则推断 + 兜底 | 能声明就声明（最准），不声明就推断，都判不出走兜底 |
| 声明载体 | 请求体的 `task` 字段 | 与现有 `feature` 同级，都是本网关的私有扩展，风格一致 |
| 映射表承载 | YAML 配置 | 静态策略用静态配置；启动期就能校验模型是否被 provider 支持 |

不做的事：不按任务改 `max_tokens`、不改温度、不做 per-task 预算、不做按租户差异化路由。

---

## 2. 前置发现：三处"看起来已经有了"的东西

动手前先查了现状，发现三处容易误判的地方。它们的共同点是**看起来已经实现了，实际都没有接线**。

### 2.1 `RoutingPolicy.TASK_BASED` 是一个空占位

```java
public enum RoutingPolicy { COST_FIRST, QUALITY_FIRST, TASK_BASED }
```

```java
case TASK_BASED -> candidates.get(0);   // 占位：等于"永远选第一个"
```

它从没接过任何任务信息，也没有一条测试覆盖它的语义。看到这个枚举很容易以为"任务路由只差一个分支"，实际上那个分支只是把候选表的第 0 项返回。

### 2.2 `db/schema.sql` 里有一张 `model_route` 表，零代码引用

```sql
CREATE TABLE IF NOT EXISTS model_route (
  task_type      VARCHAR(32) NOT NULL,
  tier           VARCHAR(16) NOT NULL,
  model          VARCHAR(64) NOT NULL,
  ...
  PRIMARY KEY (task_type, tier)
);
```

表旁边还留着一句注释：*"留待候选表需要热更新、或需要按租户差异化时再让它接管"*。

这是本轮的一个真实选择点：路由表放 DB 还是放 YAML。选 YAML 的理由不是"简单"，而是**启动期校验**——DB 里的行在启动时看不到（除非启动时查一次并维护缓存失效），而"任务指向一个没有 qualityScore 的模型"会让级联静默永不升级。静态策略用静态配置，DB 表继续留给它原本的用途（热更新 / 按租户）。

### 2.3 `feature` 不等于 `task`

请求里已经有 `feature`（业务线标签，已落 `cost_ledger.feature`）。看起来可以直接复用，但两者的语义是正交的：

| | 回答什么 | 例子 |
|---|---|---|
| `feature` | 这条流量属于哪条业务线 | `resume-tailor` |
| `task` | 这次请求需要什么能力 | `code` / `extract-json` / `summarize` |

同一个业务线里会有多种任务（简历工具既可能摘要也可能生成代码）。用 `feature` 顶替 `task` 会让"按业务线看成本"和"按任务选模型"互相绑架，而且已经落库的历史数据无法解释。

**结论**：新增 `task`，不动 `feature`。

---

## 3. 核心判断：这是前置决策，不是第二条执行路径

这是整个方案唯一重要的架构决定。

### 错误做法

在 `dispatch` 里加分支：

```java
if (task == CODE) {
    return callPro(...);        // ← 不要这样
}
```

这样会和四件事分别打一次架：

- **缓存**（不变量 B）：namespace 是 `apiKey:model`，新路径得自己算一遍；
- **级联**（F0）：`shouldEscalate` / `escalateFrom` 要知道当前档位；
- **转移**（F2）：候选链要按模型名展开；
- **配额/账本**（不变量 C/D/E）：预扣、结算、失败路径三层口径都要重写一遍。

### 正确做法

只改 `resolveRoute` 的输出，让它仍然返回一个 `ModelTier`：

```
请求进入
  ├─ model 非 auto ──────────────→ EXPLICIT 档位（现状，不动）
  ├─ task-routing 关闭 ──────────→ 全局 policy（现状，不动）
  └─ task-routing 开启
       ├─ 显式 task
       ├─ 规则推断
       └─ default-task
            ↓
       查 task → model 映射表
            ↓
       Route(tier, task, source, reason)
            ↓
   ┌──────────────────────────────────────────┐
   │ 以下全部复用，一行不改：                    │
   │ 候选链 F2 → 重试·熔断 F1 → 超时 → F0 判定·级联 │
   └──────────────────────────────────────────┘
```

打个比方：任务路由是**上高速之前换个入口**，不是新修一条路。上了高速之后，限速、车道、救援全都一样。

这样做的实际收益不只是"少写代码"——下游四件事**不可能**被改错，因为它们根本不知道任务的存在。

---

## 4. 三层解析，以及"开关只能有一个"

`RouterService.resolve` 的顺序不可换：

```java
① 客户端点名了模型        → EXPLICIT，质量分 1.0，永不升级
② 任务路由关闭            → 全局 policy
③ 任务路由开启            → 判任务 → 查表 → 落 default-task
```

### 为什么显式模型必须最优先

用户点名了模型，网关不能因为"这个任务更适合另一个模型"就换掉它。那是未经同意的替换，也是未经同意的计费。此时 `task` 只用于日志（打一条 debug）。

### 为什么删掉 `TASK_BASED`

如果保留它，就同时存在两个开关：

- `gateway.router.policy: TASK_BASED`
- `gateway.router.task-routing.enabled: true`

四种组合里必有两种语义不明（policy 说按任务、开关说关闭，听谁的？）。**两个开关表达同一件事，一定会有人配出第三种意思。**

删除后老配置不会静默降级，而是启动时报一条说清缘由的错误：

```
gateway.router.policy 非法：[TASK_BASED]，可选：COST_FIRST / QUALITY_FIRST。
（TASK_BASED 已删除：任务路由是前置判定，不是一种全局策略）
```

### 开启后 policy 还生效吗

不生效。开启时 `default-task` 承担了原来 policy 的角色。这一点写在配置注释里——因为"配置项存在但不起作用"是这个项目最反感的一类问题。

---

## 5. 规则分类器

### 5.1 判据

| 任务 | 判据（都要求高置信） |
|---|---|
| `extract-json` | 含 ```` ```json ```` 围栏；或含 `{"`；或同时出现 `json` 与"返回/输出/格式/schema/字段/抽取"之一 |
| `code` | 含代码围栏；或命中 ≥3 **组**代码符号；或命中强关键词（重构/编译/报错/堆栈/单元测试/这段代码…） |
| `summarize` | prompt ≥ 2000 token；或含"总结/摘要/提炼/概括/归纳/summarize" |
| `chat` | 其余，即 `default-task` |

判定顺序：`json-fence` > `code` > `json-output` > `long-input` > `summary-keyword`。

顺序的理由是"输出格式约束 > 输入内容类型 > 输入长度"，但有一个必须处理的例外：

- ```` ```json ```` 围栏同时满足"像代码"和"像 JSON"，此时以围栏的语言标记为准（客户端已经明说是 json）；
- "把这段 Python 代码整理成 JSON" 只含裸围栏，应判 `code`——它真正需要的能力是读懂代码，不是套输出模板。

### 5.2 代码特征按"组"统计，不按次数

```java
{";"}  {"{", "}"}  {"(", ")"}  {"->", "=>"}  {"def ", "class ", "import "}  ...
```

命中 **≥3 组**才算代码。

两个理由：

1. **不按次数**：代码会重复用同一个符号（十个分号），自然语言也会重复（十个逗号）。次数没有区分度，种类有。
2. **按组**：`(a) (b)` 在自然语言里很常见。若把 `(` 和 `)` 各算一种，一对括号就贡献 2 分，一段带括号的正常提问会被误判成代码。分组之后"一对括号"只算 1 种，要凑够 3 种就必须同时出现分号、大括号、赋值或关键字这类真正属于代码的东西。

测试 `TaskClassifierTest.bracketsAloneAreNotEnough` 就守着这条。

### 5.3 为什么在线链路不用模型分类

每个请求先发一次分类调用，等于给全部流量加一笔固定税：多一次往返、多一份费用、多一个失败点。更麻烦的是它**不可静态解释**——线上出现"这个请求为什么走了贵模型"，规则版能指着代码说清命中了哪一条，模型版只能说"它当时觉得像"。

模型分类适合离线标注与抽样校准，不适合放在请求路径上。配置里保留了 `classifier: model` 这个值的位置，但**启动校验会直接拒绝它**（见 6.2），避免有人以为它已经实现。

### 5.4 三条纪律（都有测试）

1. **确定性**：同输入永远同输出，否则离线基线不可复现。
2. **永不抛异常**：分类失败是网关自己的问题，不能让用户的请求失败。捕获 `RuntimeException`（不是 `Throwable`——`Error` 应当照常上抛）后回落 `default-task` 并打 warn。
3. **只看最后一条 user 消息 + 元信息**，不读全量历史，否则 100 轮对话的分类开销不再是 O(1)。也刻意不看 system：那通常是被测应用注入的提示词模板，代表"它希望模型怎么答"，不代表"用户想做什么"。

### 5.5 `classifier: off` 关掉的是推断，不是任务路由

这两个开关是独立的：

- `enabled` 管"要不要按任务选模型"；
- `classifier` 管"要不要让网关猜"。

`off` 时显式 `task` 仍然生效，只是不再推断。混成一个开关，就会出现"想禁用推断结果把整个能力关了"。

---

## 6. 映射表

### 6.1 为什么指 model，不指 tier

`model_route` 那张遗留表是 `(task_type, tier) → model`，看起来像"每个任务在每个档位选一个模型"。但当前 candidates 每个档位基本只有一个模型，映射到 tier 只是多一次间接寻址、少一分可读性。

一个任务只需要一个**首选**；**升级路径是全局的**（按 `qualityScore` 排序），不需要为每个任务重定义档位。

```yaml
task-routing:
  enabled: true
  default-task: chat
  classifier: rule          # rule | off
  tasks:
    code:         { model: deepseek-v4-pro }
    extract-json: { model: deepseek-flash }
    summarize:    { model: deepseek-flash }
    chat:         { model: deepseek-flash }
```

### 6.2 三条启动期校验

| 校验 | 不校验的后果 |
|---|---|
| `default-task` 必须在 `tasks` 里 | 兜底路径断裂，未命中任务全部报错 |
| 每个 task 的 `model` 必须已在 `candidates` 里 | 该模型没有 `qualityScore`，级联**永远找不到更强档位**——表现为"怎么升级都升不上去"，不报错 |
| `enabled` 时 `tasks` 不能为空 | 开了个空壳，所有请求落 default，而 default 又必然不在表里 |
| `classifier` 只能是 `rule` / `off` | 写 `model` 的人以为接了模型分类，实际静默走规则 |

第二、四条是典型的"静默坏法"：不报错、不告警，只是行为与预期差一个数量级。只有启动期能拦。

### 6.3 两类"找不到"的处理不同

| 情况 | 处理 | 理由 |
|---|---|---|
| **显式声明**的未知任务 | `IllegalArgumentException` → **400** `invalid_request_error` | 调用方以为 `code` 生效了、实际走了 chat，只有翻账本才能发现。静默兜底是最坏的选择 |
| **推断出**的未知任务 | 落 `default-task` | 推断是网关自己的判断，判错不该让用户的请求失败 |

错误体里必须列出可用任务名：

```json
{"error":{"message":"未知任务 [cdoe]。当前可用：[chat, code, extract-json, summarize]；也可以在请求里省略 task，由网关推断","type":"invalid_request_error"}}
```

### 6.4 任务名归一化必须两边一致

配置侧和请求侧都做 `trim().toLowerCase(Locale.ROOT)`。否则配置写 `Code`、请求传 `code` 会匹配不上——而显式任务匹配不上是要 400 的，用户眼里他明明写对了。

用 `Locale.ROOT` 而不是默认 locale：土耳其语环境下 `"I".toLowerCase()` 会得到 `ı`。

---

## 7. 四个接触面

| 接触面 | 是否改动 | 说明 |
|---|---|---|
| 缓存（不变量 B） | **不用改** | namespace 是 `apiKey:model`，任务切换模型后天然分到不同段。但同一问题在 `task=code` 与未声明下会落不同段，命中率会降——这是**正确行为**，不同模型的答案不能混用 |
| 级联（F0） | **不用改** | 任务选中 STRONG 时已无更强档位，`shouldEscalate` 自动返回 false，靠 `qualityScore` 表达 |
| 转移（F2） | **不用改** | 模型名照旧进 `candidatesFor(model)` |
| 配额/账本（C/D/E） | **不用改** | 口径完全不变 |

「不用改」是这一层设计的直接结果，不是巧合——因为任务路由只产出一个 `ModelTier`。

唯一需要**单独断言**的是缓存那一条：如果 namespace 退化成 `apiKey:auto`，不同任务会命中彼此的答案，任务路由配了等于没配，而且没有任何报错。`ChatPipelineTaskRoutingTest.cacheNamespaceFollowsRoutedModel` 就是守它的。

---

## 8. 一个真 bug：同步抛出的异常绕过 `onErrorResume`

这是本轮唯一一个非预期的发现，值得单独记。

### 现象

测试 `unknownTaskSurfacesAsBadRequest` 报的是 **ERROR** 而不是 FAILURE：

```
java.lang.IllegalArgumentException: 未知任务 [cdoe]。当前可用：[chat]
    at RouterService.resolve(RouterService.java:98)
    at ChatPipeline.resolveRoute(ChatPipeline.java:657)
    at ChatPipeline.execute(ChatPipeline.java:122)
    at ChatPipelineTaskRoutingTest...
```

异常直接冒到了调用方，`StepVerifier` 连 Mono 都没拿到。

### 为什么这是真 bug

控制器的错误映射全在 `onErrorResume` 里：

```java
return pipeline.execute(key, request, traceId)
        .flatMap(...)
        .onErrorResume(e -> {
            setStatusIfUncommitted(response, OpenAiProtocol.statusOf(e));
            ...  // 状态码 + OpenAI 错误体
        });
```

`resolveRoute` 是**同步**调用（在 `execute` 的第 122 行，返回 Mono 之前），所以它抛出的异常不是"流里的错误"，`onErrorResume` 根本不会执行。异常冒泡给 Spring，客户端收到 **500**。

于是设计里写的"未知任务 → 400"完全失效，用户还会以为自己的请求触发了服务端故障。

### 修法

两侧都把同步异常显式转成流错误：

```java
Route route;
try {
    route = resolveRoute(request);
} catch (RuntimeException e) {
    return Mono.error(e);      // 非流式
}
...
} catch (RuntimeException e) {
    return Flux.error(e);      // 流式
}
```

流式那条尤其不能漏：否则同一个错误在流式客户端是 500、非流式是 400——同一个原因两种状态码，是最难向调用方解释的不一致。

### 教训

**"异常类型对"和"HTTP 状态码对"是两件事。** 如果测试只断言 `assertThrows(IllegalArgumentException.class, ...)`，它会全绿，而线上仍然是 500。断言必须打到 `OpenAiProtocol.statusOf(e)` 这一层。

---

## 9. 验证

### 9.1 单元测试

`mvn -o clean test` → **175 passed**（141 → 175，+34）。

| 测试类 | 用例数 | 守什么 |
|---|---|---|
| `TaskClassifierTest` | 17（新增） | 显式优先、归一化、代码/JSON/长文判据、括号不误判、只看最后一条 user、确定性、不抛异常、`classifier=off` |
| `RouterServiceTest` | 20（+12） | 任务命中、推断命中、推断未配置落 default、未知显式任务 400、显式模型优先、开关关闭走 policy、四条启动校验 |
| `ChatPipelineTaskRoutingTest` | 5（新增） | 非流式/流式都用路由结果、缓存段跟着走、未知任务 400、指标真的写了 |

另外三个既有编排测试（Failover / QuotaSettle / Resilience）补了 `routerService.resolve` 的桩。漏桩的后果很典型：Mockito 返回 `null` → 第一阶段 NPE，而报错会指向配额断言，排查方向跑偏。

### 9.2 真机脚本

`scripts/check-task-routing.py`，8 条断言：

| 段 | 断言 |
|---|---|
| A1/A2 | 显式 `task=code` / `summarize` → 上游真的收到 `stub-pro` / `stub-flash` |
| B1 | 不传 task + 代码围栏 → 推断成 code，走 `stub-pro` |
| B2 | 不传 task + 普通提问 → `default-task=chat`，走 `stub-flash`（零回归） |
| C1 | 未知 task → **400** `invalid_request_error`，且上游**零调用** |
| D1 | `model=stub-pro` + `task=summarize` → 仍走 `stub-pro` |
| E1 | 流式分片里的 `model` 就是路由结果 |
| F1 | `llmgw_route_task_total` 带 `task` / `source` 标签 |

> **本轮未实跑**：脚本需要 Redis（配额走 Redis + Lua，没有开关可以绕开），
> 而本机 Docker Desktop 启动被沙箱安全策略阻止（禁 `wsl.exe`，不可绕过）。
> 脚本自身已通过语法检查与前置检查（无 Redis 时会明确提示 `docker compose up -d`
> 并以退出码 2 结束，而不是崩栈）。
>
> 复跑方式：
> ```bash
> docker compose up -d && python scripts/check-task-routing.py
> ```

C1 那条是第 8 节那个 bug 的回归红线——它同时断言"状态码 400"和"上游零调用"，两者都对才说明错误发生在出网之前。

---

## 10. 已知边界

1. **任务表是静态配置**，不支持热更新。要热更得让 `model_route` 表接管（schema 里已经留着）。
2. **规则分类器只认高置信特征**：漏判多、误判少。漏判只损失优化，误判会烧钱，两害不对称，所以选这个方向。
3. **没有"翻译""改写"这类任务规则**。要加任务，得同时改 `TaskClassifier` 的判据和配置表——目前不支持"纯配置新增任务"（因为推断规则在代码里）。
4. **只看最后一条 user 消息**：多轮对话里如果第一轮定了任务、后面都在追问，会被判成 default。
5. **任务不影响采样参数**：`task=extract-json` 不会自动加 `response_format`，也不会调温度。它只回答"用哪个模型"。
6. **没有 per-task 预算**：任务不参与配额。成本控制仍归 quota，两件事不揉在一起。
7. **缓存命中率会下降**：同一问题在"声明 code"和"不声明"下走不同模型、不同缓存段。这是正确行为，但要预期。
8. **`classifier: model` 未实现**，配置它会直接启动失败。
9. **响应头 `X-Route-Task` / `X-Route-Source` 未做**：`ChatResponse` 有 22 处构造点，为两个诊断头改 record 不划算。R2 用 exchange attribute 做。
10. **账本没有 `task` 列**：现在只能从 `model` 反推"当时走了什么任务"，而"为什么走这个任务"（source/reason）只在日志里。
11. **`/admin/route` 只读端点未做**：排错时还是得看日志。

---

## 11. 下一步

**R2（建议紧接着做）**
- `X-Route-Task` / `X-Route-Source` 响应头（exchange attribute，不碰 `ChatResponse`）
- `cost_ledger` 加 `task` 列 + migration
- `/admin/route`：当前生效映射表 + 各任务命中次数
- 真机脚本在有容器环境时复跑

**R3（可选）**
- `classifier: model` 后端：异步、带置信度阈值、低置信升级给 `llm-eval` 的 judge
- 置信度低于阈值时不直接采信，落 default —— 模型分类的价值在"覆盖规则判不出的长尾"，不在替代规则

**B 批余项（仍未做）**
- P6：抽 `TenantService`（apiKey 同时是身份、凭证、日志字段三种角色）
- P7：补"最终由哪家 provider 服务了这次请求"
- F1 遗留：重试的每次尝试单独落账
- F2 遗留：给熔断器配 `ignoreException(...)`，让 4xx 不计入窗口
