# step-24 · B 批 P7：账本里的"谁答的" —— provider 与 model 是两个维度

> 目标：让账本回答"这次请求最终是哪家 provider 服务的"。
> 前置：F2（跨 provider 故障转移）已经让同一个 model 可能从多家 provider 出去。
> 范围：`cost_ledger.provider` 列 + Kafka 事件 + `/admin/ledger/by-provider`；不含新的路由能力。

---

## 1. 问题：F2 之后，`model` 这个字段开始说谎

F2 之前，一个 model 名只在一个地方部署。"`deepseek-flash` 答的"和"deepseek 那家答的"是同一句话，记 model 就够了。

F2 之后不是了。候选链里可以并列两个都叫 `deepseek-flash` 的部署（同模型异构通道、备用账号、不同区域），转移发生在它们之间。这时账本里那一行写着 `model = deepseek-flash`，但下面这些问题一个都答不了：

| 问题 | 只看 model |
|---|---|
| 故障那两小时，备用链路接管了多少流量？ | 答不了 —— 两家并成一行 |
| 备用链路的质量/延迟和主链路差多少？ | 答不了 |
| 这个月的钱有多少花在了"以防万一"的链路上？ | 答不了 |
| 某家 provider 涨价后，成本变了多少？ | 答不了 |

这个问题在 F2 落地那天就存在了，但直到要真的看归因报表才疼 —— 属于典型的"功能做对了，留证没跟上"。

R2（step-23）把 `task` 补上，回答的是"这笔钱按什么能力需求花出去"。P7 补的是另一半：**"这笔钱付给了谁"**。两者正交，谁也不替代谁。

---

## 2. 两个判断

### 2.1 provider 与 model 必须是两列，不能拼成一个字符串

很自然的偷懒写法是记 `"deepseek-flash@provider-b"`。省一列、省一个参数、看起来信息还更全。但它会输在三个地方：

1. **聚合查询要先拆字符串**。`GROUP BY provider` 变成 `GROUP BY split_part(model, '@', 2)`，索引直接用不上；每加一种统计口径就要重写一次拆分。
2. **拼法一改，历史数据断代**。分隔符从 `@` 换成 `#`、顺序从"模型@提供方"换成"提供方:模型"，旧数据就成了另一种格式，还得写兼容分支。
3. **两者会各自独立变化**，拼接等于强行绑定：
   - 级联升级（flash → pro）改 `model`，不改 `provider`；
   - 故障转移改 `provider`，不改 `model`。

第 3 点是本质。两个维度的取值可以四种组合都出现，"拼起来"只是把二维压成一维，压完就再也拆不开。

所以：`model` 一列、`provider` 一列，各自建索引，各自能 `GROUP BY`。

### 2.2 流式与非流式取 provider 的时机不同，必须分别实现

这是本轮最容易做错的地方。两条路径拿"是谁答的"这件事的时机根本不一样。

**非流式**：能拿到响应对象，在 `.map` 里记。

```java
.map(resp -> {
    // 先记"谁答的"，再判定答案能不能用：
    // 空正文也是一次真实的上游交付（钱花了），它的 provider 同样要留证。
    servedBy.set(adapter.name());
    if (UpstreamOutcome.of(resp.content(), resp.finishReason()).empty()) {
        discarded.hold(resp, adapter.name());
        throw new UpstreamEmptyResponseException(resp);
    }
    return resp;
});
```

顺序很关键：`servedBy.set(...)` 在**判空正文之前**。空正文也是一次真实的交付（上游计费了），如果先判空再记，这一次的 provider 就丢了；而它恰好是最需要留证的一次 —— 重试/转移之后，人要回答"那次空回答是谁给的"。

**流式**：拿不到"响应对象"。`Flux<DataBuffer>` 里没有任何字段说"我是谁给的"，第一个分片可能永远不来（上游挂死），也可能来了但内容为空。

能确定"是哪家"的唯一时刻，是**开始拉流的那一刻**：

```java
attempt = resilience.protectStream(() -> adapter.chatStream(model, request), ...)
        .doOnSubscribe(s -> servedByProvider.set(adapter.name()));
```

几个选择理由：

- 用 `doOnSubscribe` 而不是 `doOnNext`：首帧可能永远不来，但"我确实向这家发起了请求"是既定事实。等到首帧才记，等于把"上游一直不出字"这类最典型的故障排除在账本之外。
- 最后一次写入就是真正交付的那家。`attemptStreamChain` 是按候选顺序尝试的，某一家失败才轮到下一家，所以后写的覆盖先写的正好是"最终接管者"。
- 反过来，如果只在开始时记一次（不覆盖），首片前转移就会记成"第一家"—— 那家其实一个字都没交付。

这就是为什么代码里有两个 `AtomicReference` 而不是一个"统一变量"：

```java
// 流式路径（非流式没有这个：它的模型名直接取 tier.model() / stronger.model()）
AtomicReference<String> servedBy = new AtomicReference<>(tier.model());
// 实际服务这次请求的 provider（P7）。与上面的 servedBy 分开：
// 一个是模型名（会因级联升级变化），一个是提供方（会因转移变化），
// 两者可以各自独立地变 —— 合成一个"servedBy"字符串就再也拆不开了。
AtomicReference<String> servedByProvider = new AtomicReference<>();
```

> 命名上的一个细节：两条路径统一用 `servedByProvider` 表示 provider。
> 早期非流式那版把它叫 `servedBy`，和流式里"模型名"的 `servedBy` 同名不同义 ——
> 同一个类里同名不同义，读代码的人第一眼一定会读错，所以本轮一并改了名。

---

## 3. 三个落点

### 3.1 表与迁移

```sql
-- db/migrations/002-cost-ledger-provider.sql
ALTER TABLE cost_ledger ADD COLUMN IF NOT EXISTS provider VARCHAR(64);
COMMENT ON COLUMN cost_ledger.provider IS
  '实际服务这次请求的 provider（缓存命中为 cache）；用于 F2 转移后的成本归因';
CREATE INDEX IF NOT EXISTS idx_cost_ledger_provider_time
  ON cost_ledger (provider, created_at);
```

和 step-23 的 `001` 同样两个约束：

- **只改 `schema.sql` 会静默失效**。它挂在容器的 `/docker-entrypoint-initdb.d` 下，只在数据目录为空时执行。现有库必须走 migration。
- **每条语句自己幂等**（`IF NOT EXISTS`）。项目不引入 Flyway/Liquibase —— 为两张列加一套迁移框架，等于多一套要解释的失败语义。

### 3.2 服务与 Kafka 事件

`CostLedgerService.record(...)` 在 `task` 之后加了 `provider`，透传给仓储、同时写进 Kafka 事件：

```java
event.put("task", task);
event.put("provider", provider);
```

Kafka 这条不是顺手的。生产链路上账本通常先进消息队列再入仓，如果事件里没有 `provider`，下游仓永远补不回来 —— 这是"网关记了但报表看不到"的经典形态。

绑定仍是 `bindNull`，理由与 `task` 完全一致：

```java
spec = provider == null ? spec.bindNull("provider", String.class)
        : spec.bind("provider", provider);
```

空串会被 `GROUP BY` 变成一个"名字叫空字符串的 provider"的假桶，NULL 才是"不知道"。

### 3.3 `/admin/ledger/by-provider`

```sql
SELECT COALESCE(provider, '(none)') AS provider,
       count(*) AS calls, sum(prompt_tokens) AS prompt_tokens,
       sum(completion_tokens) AS completion_tokens,
       round(sum(cost), 6) AS total_cost
FROM cost_ledger
GROUP BY COALESCE(provider, '(none)')
ORDER BY total_cost DESC
```

与 step-23 的 `summaryByTask()` 是同一条理由的两面。故障期最该能回答的问题就是"备用那家接管了多少"，而这正是 `model` 维度答不了的那个。

---

## 4. `(none)` 桶：加列之后的历史数据怎么办

真机第一次跑 G7 时，输出是：

```
providers=['(none)', 'stub']  stub={'calls': 21, ...}
```

那个 `(none)` 就是 P7 之前写的账 —— 它们确实没有这个信息。

**不做 backfill**。老数据没有记录过 provider，编一个 `unknown` 或 `legacy` 都是编造；用 `model` 反推回去更糟：F2 之后的 model 本来就可能对多家，反推出来的值一半是错的，而错得看起来很合理。留 NULL 最诚实，报表上用 `(none)` 呈现，让人一眼看出"这部分是加列之前的"。

于是 `GROUP BY provider` 里的三类值各有明确含义：

| 值 | 含义 |
|---|---|
| `(none)` | 不知道 —— P7 之前写的账 |
| `cache` | 没有上游参与 —— 缓存命中 |
| 具体名字 | 这次确实由这家交付 |

### 4.1 缓存命中为什么写 `"cache"` 而不是 NULL

缓存命中时确实没有 provider 参与。写 NULL 会和"P7 之前的老账"混成一个桶，两件完全不同的事实被合并。

写 `"cache"` 是一个真实答案："这次没有走上游"。它顺带让 `by-provider` 报表多了一个有意义的数字 —— 缓存挡掉了多少上游调用，可以直接从成本维度看出来。

---

## 5. 验证

### 5.1 单元测试：200 → **202 passed**

新增两条，各自钉一个不同的东西：

| 用例 | 钉住的事实 |
|---|---|
| `ledgerRecordsTheProviderThatActuallyServed` | A 返回 500 → B 接管，账本里记的是 **PROVIDER_B** |
| `providerIsPassedToThePostgresRowSeparatelyFromModel` | model 相同、provider 不同时，两列各自独立透传 |

第一条是本轮的核心断言。它不是"provider 字段被填了"—— 填成 `PROVIDER_A`（第一个候选）也能通过那种弱断言。它断言的是**真正答话的那家**，这才是故障转移之后成本归因唯一的依据。

第二条故意让 `model` 保持相同：如果实现把两者混成一个字段灌两次，这条立刻露馅。

> 顺带一个细节：`CostLedgerServiceTest` 的桩必须用 `any()` 而不是 `anyString()` 匹配 `provider`。
> Mockito 的 `anyString()` **不匹配 null**，桩会静默不命中、返回 `null`，然后在 `subscribe` 处 NPE。
> 本轮和上一轮都在这里绊过一次 —— 报错信息完全指向别处。

### 5.2 真机：`check-task-routing.py` 13 → **15/15 PASS**

新增三条：

| 断言 | 验的是 |
|---|---|
| G4（加强） | 非流式账本的 `task` / `provider` / `model` 三列同时正确 |
| G6（新增） | **流式**账本同样写入 `provider` / `task` |
| G7（新增） | `/admin/ledger/by-provider` 能按链路归因 |

G6 不是 G4 的重复。见 2.2：两条路径取 provider 的时机完全不同（`.map` vs `doOnSubscribe`）。只验非流式，等于把一半实现留在未验证状态，而这半边恰好是最容易写错的。

G4 一次真实输出：

```
trace=fe1e0b301bfe4640af3d018edd4dc756
行={'task': 'code', 'provider': 'stub', 'model': 'stub-pro'}
```

同时给脚本加了**断言条数下限** `EXPECTED_CASES = 15`。判据 `passed == len(RESULTS)` 只保证"跑了的都通过"，保证不了"该跑的都跑了" —— step-23 第 6 节就是栽在这上面（分段启动失败被静默跳过，汇总照样打印"全部通过"）。

### 5.3 另两条真机脚本

| 脚本 | 结果 |
|---|---|
| `check-failover.py` | **13/13 PASS** |
| `check-resilience.py` | **11/11 PASS** |

P7 改的是 `recordAsync` 的签名与落账内容，不碰候选链与重试/熔断逻辑，复跑是确认没有副作用。

---

## 6. 已知边界（本轮新增）

1. **流式被丢弃的尝试仍不落账**。流式拿不到 usage，这一条从 step-22 起就在，P7 没有改变它。所以"流式请求在 A 家失败、B 家接管"时，A 那次的成本是丢的。
2. **`provider` 记的是 adapter 的配置名**，不是上游返回的任何标识。如果某家 provider 背后自己又是个网关，真实的后端是谁，本网关不知道也不该猜。
3. **全链路失败时账本不记**（没有 usage 可记）。因此"某家 provider 挂了"这件事在 `by-provider` 里体现为"它没出现"，而不是"它有 0 次" —— 两者的区别要心里有数。
4. **`(none)` 桶会一直存在**，直到 P7 之前的账被归档清理。它不是 bug。
5. **`/admin/ledger/by-provider` 没有时间窗口参数**，是全量聚合。要做"最近 7 天"得直接用 SQL，或后续加 `from` / `to`。

---

## 7. 下一步

| 项 | 内容 | 状态 |
|---|---|---|
| R3 | `classifier: model` —— 异步分类、置信度阈值、低置信升级给 llm-eval 的 judge | 未开始 |
| P6 | 抽 `TenantService` —— 现在 apiKey 同时是身份、凭证、日志字段三种角色 | 未开始 |
| — | `/admin/*` 鉴权 | 上公网前必须做 |
| — | 流式丢弃尝试的落账（需要拿得到 usage，或明确放弃并写进边界） | 权衡中 |

B 批到此收口：P1~P7 全部落地。下一步按 R3 推进。
