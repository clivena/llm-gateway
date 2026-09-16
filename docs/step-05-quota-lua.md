# Step 05 · 分布式配额：Redis + Lua

## 目标
多实例部署下不超发，且不引入竞态。

## 改动
- `resources/lua/quota.lua`：原子"检查+扣减+续期"
- `core/quota/QuotaService.java`：加载脚本并执行

## 关键代码
```lua
local used = tonumber(redis.call('GET', KEYS[1]) or '0')
if used + tonumber(ARGV[1]) > tonumber(ARGV[2]) then return 1 end
redis.call('INCRBY', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
return 0
```
Java 侧用 `conn.eval(script, ReturnType.INTEGER, 1, key, tokens, limit, ttl)`。

## 为什么必须用 Lua
"先 GET 再 INCRBY" 在并发下会超发（检查与写入之间被插队）。放到 Redis 单线程里原子执行，才真正精确。

## 失败策略
Redis 不可用时 **fail-open**（放行 + 告警日志）：配额是成本控制手段，不该因它让业务全挂。
生产可配置成 fail-close，或降级到本地令牌桶（下一步演进）。

## 验证方式
```bash
seq 1 100 | xargs -P 20 -I{} curl -s -o /dev/null -w "%{http_code}\n" ...  # 观察是否精确在阈值处开始拒绝
```

## 坑
key 必须带周期（`q:{key}:{yyyyMM}`），否则配额永不过期累积成"永久限额"。

## 下一步
Step 06：成本账本。
