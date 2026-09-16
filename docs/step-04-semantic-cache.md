# Step 04 · 语义缓存（重点在"防误命中"）

## 目标
让重复/近似问题不再打上游，同时**不允许"最像的一条"蒙混过关**。

## 改动
- `core/cache/RedisSemanticCacheService.java`：embedding + Redis 存储 + 应用内余弦 + 阈值判定

## 关键实现
```java
double similarity = cosine(query, vec);
if (similarity > bestSimilarity) { bestSimilarity = similarity; best = entry; }
...
if (best == null || bestSimilarity < threshold) {
    return Optional.empty();      // 低相似度直接 MISS
}
```
存储：`LPUSH sem:{namespace}` + `LTRIM 0..499` + `EXPIRE`，条目上限控制成本。

## 设计取舍（面试可讲）
| 方案 | 优点 | 缺点 |
|---|---|---|
| 当前：List + 应用内余弦 | 任意 Redis 可跑，无额外组件 | O(n) 扫描，仅适合小规模 |
| Redis Stack FT.KNN | O(log n)，真正的向量检索 | 需要 Redis Stack/Query 版本 |

升级路径明确：`FT.CREATE sem_idx ON HASH PREFIX 1 sem: SCHEMA vector VECTOR HNSW 6 ...`，业务代码不动。

## 防误命中的三层保险
1. 阈值以下直接 MISS，不返回"最像的一条"；
2. 命中结果带上 similarity，落账时记录，便于事后抽检计算误命中率；
3. 高风险请求（事务类）可按 namespace 关闭缓存。

## 验证方式
同一问题问两次：第二次应立即返回且 `cacheHit=true`；改问法（同义）应命中；问不相关问题必须 MISS。

## 坑
`embed()` 失败时不能抛给主链路——缓存是"优化项"，降级为直连上游。代码里对每个 adapter 做了 try/catch。

## 下一步
Step 05：配额。
