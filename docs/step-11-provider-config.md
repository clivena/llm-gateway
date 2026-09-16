# Step 11 · 接入真实模型：密钥落位与配置接线

> 目标：让 llm-gateway 能连上一家真实厂商把链路跑通。这一步不写业务代码，只解决
> **「key 放哪儿、配置怎么和代码里的解析逻辑对齐、哪些地方现在还不支持」**。

## 1. 目标

前置条件是 key 不能进代码仓库、不能进 jar。同时要让 `application.yml` 里的占位配置
被真实厂商的值替换掉。选定的厂商是 **DeepSeek**（用它的 OpenAI 兼容端点）。

## 2. 改动

| 文件 | 动作 | 说明 |
|---|---|---|
| `llm-gateway/config/application-local.yml` | 新增 | 真实私有配置，含 key，**已 git-ignore** |
| `llm-gateway/config/application-local.yml.example` | 新增 | 可提交的模板，不含真实 key |
| `llm-gateway/.gitignore` | 修改 | 追加 `config/application-local.yml`（原来只忽略了 `.env`）|

### 2.1 为什么放 `config/` 而不是 `src/main/resources/`

Spring Boot 的默认配置搜索路径包含 **`file:./config/`**（与 `classpath:/`、`file:./` 并列）。
所以只要在项目根目录下建 `config/application-local.yml`，启动时激活 `local` profile 就会自动加载，
**不需要写任何代码去读它，也不需要额外的 `--spring.config.location` 参数**。

放在 `src/main/resources/` 也能生效，但会被 `mvn package` 打进 jar —— 一旦这个 jar 被分发或
误传到公开仓库，key 就泄露了。放在 `config/` 下则在源码树之外，物理上进不了 jar。

### 2.2 为什么 `providers` 要整块重写

`gateway.providers` 是 `List<Provider>`。**Spring Boot 对 List 类型的属性不做逐项合并，只做整体替换**：
外部文件里出现 `gateway.providers` 就会把 `application.yml` 里那份整个顶掉。
所以本地配置里必须写完整的 provider 块，不能只覆盖 `api-key` 一行。

### 2.3 为什么 `name` 必须叫 `openai`

`OpenAiAdapter` 的构造函数是这样找配置的：

```java
this.config = properties.getProviders().stream()
        .filter(p -> "openai".equalsIgnoreCase(p.getName()))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("provider 'openai' not configured"));
```

这里的 `openai` 是**「OpenAI 兼容协议」的标识**，跟用哪家厂商无关。改成 `deepseek` 会在启动时
直接抛异常。国内厂商（DeepSeek / 硅基流动 / 通义 compatible-mode / 智谱 / Kimi）都实现了这套协议，
所以全部复用同一个适配器。

## 3. 关键配置

```yaml
gateway:
  cache:
    enabled: false            # 见 4.1

  providers:
    - name: openai            # 不能改
      base-url: https://api.deepseek.com/v1
      api-key: "sk-你的key"
      embedding-model: ""     # DeepSeek 无向量接口
      models:
        - deepseek-chat
        - deepseek-reasoner
```

启动方式（`local` profile 激活即可，无需指定文件路径）：

```powershell
mvn spring-boot:run "-Dspring-boot.run.arguments=--spring.profiles.active=local"
```

## 4. 三个必须知道的限制（都是读代码读出来的，不是猜的）

### 4.1 语义缓存对 DeepSeek 直接失效 —— 所以先关掉

`RedisSemanticCacheService` 的命中判定完全依赖 `embed()`。DeepSeek 官方**没有 `/embeddings` 接口**，
`embedOne()` 会把异常吞掉返回 `null`，于是 `lookup()` 永远返回空、`write()` 永远直接 return。

它不会让服务崩，但代价是：**每次请求要多打两次注定 404 的 HTTP 调用**（lookup 一次、write 一次），
白白增加延迟，还会污染压测数据。所以本地配置里 `cache.enabled: false`。

> 想让语义缓存真跑起来，有两条路：
> ① 换成有向量能力的厂商（硅基流动 `BAAI/bge-m3` 有免费额度，最省事）；
> ② 保留 DeepSeek 做对话、另接一家做 embedding —— 但这条路**目前代码走不通**，见 4.2。

### 4.2 「多供应商适配」目前只是单供应商

`OpenAiAdapter` 是 `@Component` 单例，构造时只取**第一个**名为 `openai` 的 provider。
所以配置里写第二个 provider 不会有任何效果 —— 不会多出一个 adapter bean，
`List<ProviderAdapter> adapters` 里永远只有 1 个元素。

**DESIGN.md 里写的"多供应商适配"是设计意图，当前实现是单供应商。** 要真正支持多家，
需要把 `OpenAiAdapter` 从 `@Component` 改成普通类，再由一个 `@Configuration` 遍历
`properties.getProviders()` 为每个 provider 构建一个实例并注册成 bean。

这是下一步实际开发时的一个明确 TODO，也正好是面试里可以讲的"我知道我的系统边界在哪"。

### 4.3 `model=auto` 会失败，但显式传 model 不用改代码

`RouterService` 的候选模型是硬编码的：

```java
private final List<ModelTier> candidates = List.of(
        new ModelTier("gpt-4o-mini", "CHEAP", 0.15, 0.72),
        new ModelTier("gpt-4o", "STRONG", 2.50, 0.90)
);
```

而 `ChatPipeline.adapterFor(model)` 会拿模型名去 adapter 的 `models` 列表里比对，对不上就抛
`no adapter for model ...`。所以：

| 请求里的 model | 走向 | 结果 |
|---|---|---|
| `"auto"`（或不传） | `routerService.pick(COST_FIRST)` → `gpt-4o-mini` | ❌ 抛 no adapter for model |
| `"deepseek-chat"` | `resolveTier` 判定为 `EXPLICIT` 层，直接按名字找 adapter | ✅ 走通 |

`ChatRequest.resolvedModel()` 的逻辑是：`model` 为空或等于 `"auto"` 才返回 `auto`，否则原样返回。

**所以打通链路阶段，请求体里显式写 `model: "deepseek-chat"` 即可，一行 Java 代码都不用改。**

但有个副作用要提前知道：显式模型会走 `EXPLICIT` 层，而它的 `costPer1k = 0.0`，
于是 `estimateCost()` 算出来是 **0**，成本账本里记的金额全是 0。
**想看真实成本数字 / 自动路由 / 级联降级，才需要把 `RouterService` 改成配置驱动。**

## 5. 验证方式

写了一个校验脚本，做三件事：解析两个 YAML 确认语法、按 Java 侧的查找规则断言 provider 块形状、
把 `RouterService` 的硬编码模型名和本地配置的模型做交集比对。结果：

```
[OK] application-local.yml 语法可解析
  cache.enabled = False  (期望 False)
  providers 数量 = 1
  OK name = 'openai'
  OK base-url = 'https://api.deepseek.com/v1'
  [TODO] api-key 仍是占位符，需要你粘贴真实 key
  models = ['deepseek-chat', 'deepseek-reasoner']
[OK] application.yml 语法可解析
[INFO] RouterService 硬编码模型: ['gpt-4o-mini', 'gpt-4o']
  => 结论：请求里若不显式指定 model，路由会挑到 gpt-4o-mini，本地配置不认它，会抛 no adapter
=== 总结: 配置可用（填完 key 即可启动） ===
```

## 6. 坑

- **不要把 key 丢进 `.env` 就以为完事**：`.gitignore` 里虽然有 `.env`，但 Spring Boot
  **不会自动读 `.env` 文件**。要么用真实环境变量 `OPENAI_API_KEY`，要么走本步的 `config/` 方案。
- **`X-Api-Key` 请求头不是上游 key**。`ChatController` 拿它当**网关自己的租户标识**，
  用于配额 key 和成本归因，不传则记为 `anonymous`。上游 key 只从配置里来。
- 改完配置记得 **重新编译**（`mvn compile` 或 `spring-boot:run` 会自动做），
  否则 `target/classes` 下还是旧的配置。

## 7. 下一步

1. 把 key 粘进 `config/application-local.yml`；
2. `mvn spring-boot:run` 带 `--spring.profiles.active=local` 启动；
3. `curl` 用显式 `model: "deepseek-chat"` 打一次非流式 + 一次流式，看能不能通；
4. 通完之后再决定要不要做 `RouterService` 配置化（拿到真实成本数字）。
