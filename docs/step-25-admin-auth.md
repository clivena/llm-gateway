# step-25 · 运维接口鉴权：默认收紧，而不是"配了才校验"

> 目标：堵住 `/admin/**` 全裸的口子。
> 范围：只做访问控制，不做多租户隔离、不做审计、不做令牌轮换。
> 一句话结论：**没配 token 时只允许本机，配了 token 就必须带**。

---

## 1. 问题：这些端点原来谁都能读

| 端点 | 泄露了什么 |
|---|---|
| `/admin/ledger/recent` | 全部租户的调用明细：`apiKey`、`feature`、`task`、`provider`、token 数、成本、`trace_id` |
| `/admin/ledger/summary` / `by-task` / `by-provider` | 按租户/任务/链路的完整成本结构 |
| `/admin/quota/{apiKey}` | 任一租户的配额用量 —— 配上上一条，`apiKey` 可以被枚举 |
| `/admin/route`、`/admin/pricing`、`/admin/circuit` | 内部模型映射、单价、各 provider 健康状态 |
| `/admin/status` | 依赖拓扑（Redis / Postgres 是否健康） |

这不是"看到了几个数字"。`apiKey` 在这个项目里同时是**身份**（配额归属）和
**凭证**（调用时要带 `X-Api-Key`）—— 账本里把它明文列出来，等于把租户的钥匙
和账单一起放在一个没有门的房间里。

之前每一轮都把它写进"已知边界"，标着"上公网前必须做"。这一轮把它做掉。

---

## 2. 判断一：不引入 Spring Security

要解决的问题只有一句："这个请求能不能读运维数据。"

Spring Security 能解决，但代价是一整套要解释的失败语义：filter chain 的顺序、
`SecurityContext` 的传播、`permitAll` 与 `authenticated` 的组合、以及配错时那种
"整个应用 401"或"静默放行"的表现。对一个只有五六个只读端点的运维面，这是
用一门语言去说一个词。

一个 `WebFilter` 六十行，把四个判断写清楚就够了。**能读懂的代码比"正确的框架"更值钱**——
尤其是当它出问题的时候。

---

## 3. 判断二：默认收紧，而不是"配了才校验"

常见的实现是：配了 `admin.token` 才校验，没配就放行。它的失败形态最糟：

> 忘配 = 全部租户的账本公开在公网上，**而且没有任何信号**。

三种默认放在一起看：

| 默认策略 | 本地开发 | 公网误部署 | 有信号吗 |
|---|---|---|---|
| 没配就放行 | 可用 | **全裸** | 没有 |
| 没配就全拒 | **全挂**（脚本、dashboard 全废） | 安全 | 有，但每天都在喊 |
| **没配只许本机** | 可用（都是 127.0.0.1） | **拒绝** | 有，且只在真出问题时喊 |

选了第三种：

- 本地 `mvn spring-boot:run`、`curl localhost`、dashboard、四条真机脚本**全都不用改**；
- 一旦从外部地址访问，直接 403，并在日志里留一条 warn；
- 想远程运维？配 `gateway.admin.token`，然后带 `X-Admin-Token`。

### 3.1 拿不到来源地址时按"非本机"处理

```java
InetSocketAddress remote = exchange.getRequest().getRemoteAddress();
if (remote == null || remote.getAddress() == null) {
    return false;   // fail-closed
}
return remote.getAddress().isLoopbackAddress();
```

反过来的写法（null 当本机）在本机测试里**永远不会暴露**，却是一条直通口：
反代的某些配置、Unix socket 转发、换运行时，都可能让 `remoteAddress` 为空。
这是那条只能在 mock 里钉住的断言。

---

## 4. 四个实现细节

### 4.1 路径匹配不能只写 `startsWith("/admin")`

```java
return path.equals("/admin") || path.startsWith("/admin/");
```

`startsWith("/admin")` 会把 `/administrator`、`/admin-backup` 一起拦下。
将来谁加了一个同前缀的公开端点，这个 bug 会以"莫名其妙的 401"出现，
而排查的人第一眼会去看鉴权配置，不会去看前缀匹配。

### 4.2 常量时间比较

```java
MessageDigest.isEqual(a.getBytes(UTF_8), b.getBytes(UTF_8));
```

普通的 `equals` 在第一个不同字节处返回，耗时与"猜中了多少位"成正比。
对一个可以被无限次尝试的入口，这足够把 token 逐位试出来。

这条在本地跑不出任何差别 —— 属于"写对了没人知道、写错了也没人知道"的代码。
写它的理由不是当前的威胁模型，而是**这类东西一旦漏了，事后没有补救的机会**。

### 4.3 403 与 401 要分开

| 状态码 | type | 什么时候 | 运维看到它该做什么 |
|---|---|---|---|
| 403 | `admin_token_not_configured` | 没配 token 且来自外部 | 去配 token |
| 401 | `unauthorized` | 配了 token 但没带/带错 | 检查请求头 |

两个都用 401 也能"work"，但运维看到 401 的第一反应是"我 token 配错了"，
而真实原因是"我根本没配"。**错误码是给人看的，不是给状态机看的。**

### 4.4 错误响应不能因为序列化失败变成 500

```java
try {
    bytes = mapper.writeValueAsBytes(body);
} catch (Exception e) {
    bytes = ("{\"error\":{\"type\":\"" + type + "\"}}").getBytes(UTF_8);
}
```

兜底的必要性：如果鉴权失败时的响应体序列化抛了异常，客户端会收到 500 ——
那会让人以为"服务挂了"，而事实是"你没权限"。**拒绝必须是一次体面的拒绝。**

日志里也不打印 token 的值，只说"缺了"还是"不匹配"：日志本身也会被人看到。

---

## 5. 边界：只管 `/admin`，不管 `/v1`

业务接口有自己的鉴权维度（`apiKey` + 配额），两件事不能塞进同一个 filter。

混在一起的后果不是"更安全"，而是**排查时说不清**：一次 401 到底是
"没配额"、"key 无效"，还是"没运维权限"？三个原因挤在一个状态码里，
最后只能靠翻日志猜。

验证里专门有一条钉住这个边界：

```
✓ B4 业务接口不受运维鉴权影响 —— HTTP=400（期望 400，绝不该是 401/403）
```

---

## 6. 验证

### 6.1 单元测试：202 → **210 passed**

`AdminAuthFilterTest` 8 例，用 `MockServerWebExchange` 精确构造 `remoteAddress`：

| 用例 | 钉住的事实 |
|---|---|
| `withoutTokenLoopbackIsAllowed` | 本机零配置可用 |
| `withoutTokenExternalAddressIsForbidden` | 未配 token 时外部 403 |
| `withoutTokenAndUnknownRemoteIsForbidden` | **来源未知 → fail-closed**（只能在 mock 里测） |
| `configuredTokenRequiresTheHeaderEvenFromLoopback` | 配了 token 后"我是本机"不再能绕过 |
| `configuredTokenRejectsWrongValue` | 错 token → 401 |
| `configuredTokenAcceptsCorrectValueFromAnyAddress` | 对 token 从哪来都放行 |
| `businessPathIsNotProtected` | `/v1/**` 不受影响 |
| `pathWithSamePrefixButDifferentSegmentIsNotProtected` | `/administrator` 不被误拦 |

### 6.2 真机：`check-admin-auth.py` —— **7/7 PASS**

单测用 mock 构造 `remoteAddress`，但它证明不了两件事，只有真机能给：
真实的 TCP 连接来源是什么、过滤器真的挂进了 WebFlux 的链里。

| 断言 | 结果 |
|---|---|
| A1 未配 token 时本机可访问 | HTTP=200 |
| A2 未配 token 时外部被拒 | 来源 `172.22.102.90` → HTTP=403 + `admin_token_not_configured` |
| B1 配 token 后本机不带凭证也被拒 | HTTP=401 + `unauthorized` |
| B2 错误的 token 被拒 | HTTP=401 |
| B3 正确的 token 放行 | HTTP=200 |
| B4 业务接口不受运维鉴权影响 | HTTP=400（不是 401/403） |
| B5 远程带对 token 可访问 | 来源 `172.22.102.90` → HTTP=200 |

A2 / B5 的"外部来源"是真的：脚本用 UDP connect 拿本机局域网地址，
再用这个地址访问自己 —— 服务端看到的 `remoteAddress` 就不是环回了。
拿不到局域网地址时这两条会标 `SKIP` 并在汇总里说明，**不会假装通过**。

### 6.3 一个自己踩的 bug：`phase()` 把服务关了才返回

第一版把生命周期写成这样：

```python
def phase(title, token):
    proc, tail = start_app(token)
    try:
        if not wait_ready(tail): return False
        return True
    finally:
        stop_app(proc)      # ← 先停服务，再 return
```

调用方拿到 `True` 之后去发请求，得到的是 `ConnectionRefused`。
现象看起来是"服务起不来"，实际是"服务被我自己关了"。

修法是把生命周期归调用方持有：`start_phase()` 只负责启动与就绪检查，
`try/finally` 写在调用方那一层。这类时序 bug 单测抓不到 ——
它的表现与"环境没起来"完全一样。

---

## 7. 已知边界（本轮新增）

1. **配了 token 之后，浏览器直接打开 `/dashboard` 取不到数据**。面板前端是编译产物，
   fetch 不带自定义 header。本地开发建议不配 token（环回放行够用）；
   远程部署要么给面板加一个 token 输入框，要么走反代注入 header。
2. **单 token，没有租户/角色/过期/审计**。它回答"能不能进运维面"，不回答"是谁进的"。
   要做审计得引入身份，那是另一件事。
3. **`/actuator/prometheus` 未保护**。指标里没有 `apiKey`，但暴露了业务量与错误率。
   内网抓取可以接受；上公网时应在反代层限制来源。
4. **反代后面必须配 token**。经过 nginx 之后 `remoteAddress` 是 nginx 的地址（非环回），
   默认策略会直接拒绝 —— 这是对的，但部署时要知道。
5. **token 存在配置文件里**，与 `api-key` 同级。容器化部署时请走环境变量 `ADMIN_TOKEN`。

---

## 8. 下一步

| 项 | 内容 | 状态 |
|---|---|---|
| R3 | `classifier: model` —— 异步分类、置信度阈值、低置信升级给 llm-eval 的 judge | 未开始 |
| P6 | 抽 `TenantService` —— apiKey 现在同时是身份、凭证、日志字段三种角色 | 未开始 |
| — | 流式丢弃尝试的落账（需要拿得到 usage，或明确放弃并写进边界） | 权衡中 |

B 批与安全收口都已完成。剩下的是 R3（能力增强）与 P6（结构整理）。
