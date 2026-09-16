# Step 03 · 编排管线（含流式容错）

## 目标
把一次请求拆成可讲清楚的 6 步，并解决"流式下该不该重试"这个真问题。

## 改动
- `core/pipeline/ChatPipeline.java`：`execute`（非流式）与 `executeStream`（SSE）两条路径
- `api/ChatController.java`：同一路径手动写响应，兼容 stream=true/false

## 关键代码（流式的重试边界）
```java
AtomicBoolean emitted = new AtomicBoolean(false);
Flux<String> primary = cheap.chatStream(tier.model())
        .doOnNext(chunk -> { emitted.set(true); collected.append(chunk); });

return primary.onErrorResume(e -> emitted.get()
        ? Flux.error(e)                                  // 已吐字：不能重试，否则答案拼接
        : Flux.defer(() -> adapterFor(stronger).chatStream(stronger.model())));  // 未吐字：可无缝升级
```

## 为什么这么写
- 首 token 前失败 = 用户什么都没看到 → 直接换更强模型重试，体验无损。
- 已输出后失败 = 重试会让用户看到"两段拼接"的答案 → 只能截断或让客户端重发（配合幂等键）。

## 另外两个决策
- **同一路径兼容两种响应**：用 `ServerHttpResponse` 手动写 JSON / SSE，避免 Spring 因 `produces` 冲突报映射歧义。
- **显式指定模型**时跳过路由策略（`EXPLICIT` 档），把策略只用在 `model=auto`。

## 验证方式
- 非流式：返回 `ChatResponse`（含 usage、cacheHit 字段）
- 流式：`curl -N` 看到逐帧输出；把 API key 改错，观察是否有降级行为

## 坑
`Flux.error` 在 `onErrorResume` 之后若没有兜底，客户端会收到 500 而无任何提示 → 控制器里补了 SSE 错误帧。

## 下一步
Step 04：加语义缓存。
