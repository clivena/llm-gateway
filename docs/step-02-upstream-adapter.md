# Step 02 · 上游适配：非流式 + SSE 流式 + embeddings

## 目标
用一个适配器把 OpenAI 兼容协议吃下来，并统一归一化响应与用量。

## 改动
- `core/protocol/OpenAiAdapter.java`：三个方法全部实装（`chat` / `chatStream` / `embed`）

## 关键代码
```java
return webClient.post().uri("/chat/completions")
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.TEXT_EVENT_STREAM)
        .bodyValue(payload)
        .retrieve()
        .bodyToFlux(SSE_TYPE)                 // ServerSentEvent<String>
        .map(ServerSentEvent::data)
        .takeUntil("[DONE]"::equals)
        .map(this::extractDelta)
        .filter(Objects::nonNull);
```

## 为什么用 ServerSentEvent 而不是按行切
SSE 的分帧由框架负责才可靠：手工 `split("\n")` 会在 TCP 分片时把一条 JSON 切成两半。
用 `ParameterizedTypeReference<ServerSentEvent<String>>` 让 Spring 的 SSE reader 解析 `data:` 行，天然处理粘包/半包。

## 验证方式
配好 `OPENAI_API_KEY` 后：
```bash
curl -N -X POST localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" -H "X-Api-Key: demo" \
  -d '{"model":"auto","stream":true,"messages":[{"role":"user","content":"hi"}]}'
```
应看到逐帧 `data: {...}`。

## 坑
- 流式请求必须带 `Accept: text/event-stream`，否则部分网关返回普通 JSON，解析直接失败。
- 末帧 `[DONE]` 不是 JSON，先 `takeUntil` 再解析。

## 下一步
Step 03：把适配器接进编排管线。
