# Step 06 · 成本账本与全链路 traceId

## 目标
回答老板那个问题："这个月模型花了多少钱、哪个功能最烧钱？"

## 改动
- `core/cost/CostLedgerService.java`：调用事件 JSON 化后发 Kafka
- `observability/TraceIdFilter.java`：traceId 生成/透传（与旧项目 Zipkin 思路一致）
- `observability/GatewayMetrics.java`：缓存命中/未命中、级联升级计数

## 关键实现
```java
event: traceId, apiKey, feature, model, cacheHit,
       promptTokens, completionTokens, cost, latencyMs, ts
kafkaTemplate.send(TOPIC, apiKey, json).whenComplete(...);   // 失败只记日志
```

## 为什么异步
写账本绝不能拖慢主链路：Kafka 削峰 + 消费端批量落库，报表按 key/feature 聚合。
对账口径：网关账本 vs 厂商账单，误差 < 1% 才算验收通过。

## 成本怎么算
`usage.total() / 1000 * tier.costPer1k()`，模型单价来自 `model_route` 表，改价不用改代码。

## 验证方式
跑一轮请求后查 `cost_ledger`：条数 = 请求数；`cacheHit=true` 的记录 cost 应为 0。

## 坑
- traceId 必须在过滤器里放进响应头，否则跨服务对不上账。
- Kafka 未启动时必须降级（当前实现为吞异常打日志），否则本地开发跑不起来。

## 下一步
进入项目二（评测平台）：Step 07。
