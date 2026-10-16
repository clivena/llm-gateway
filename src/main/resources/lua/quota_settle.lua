-- 配额结算（reserve-then-settle 的第二段）
--
-- 预扣（quota.lua）发生在请求**之前**，那时只能按估算的 prompt 量扣；
-- 真实用量要等上游返回才知道。不结算的话，配额记的就只是"prompt 估算之和" ——
-- 而实测 completion 是 prompt 的 20~100 倍（推理型模型尤甚，reasoning token
-- 也计入 completion_tokens），于是 gateway.quota.default-limit-tokens
-- 实际约束的东西与它名字宣称的**不是一回事**。F0 修的就是这条。
--
-- KEYS[1] = quota key
-- ARGV[1] = delta（= 真实用量 - 已预扣量），可为负 —— 负值即退还
-- ARGV[2] = ttl 秒
--
-- 返回：结算后的已用量；返回 -1 表示"本周期键已不存在、跳过结算"

local key = KEYS[1]
local delta = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])

if delta == nil or delta == 0 then
  local cur = redis.call('GET', key)
  return cur and tonumber(cur) or -1
end

-- 键不存在 = 周期已经翻篇（上一个月的键过期了）。
-- 这时候把上个月的差额补到本月头上是**错**的：会让上月用量污染本月计数。
-- 直接跳过。代价是"跨周期边界上的最后几笔请求不结算"，
-- 换来的是"每个周期的用量只含本周期"—— 后者才是配额要的语义。
if redis.call('EXISTS', key) == 0 then
  return -1
end

local used = tonumber(redis.call('GET', key) or '0')
local target = used + delta
if target < 0 then
  target = 0   -- 退还不得把已用量压到负数
end

-- 刻意**不做上限检查**：reserve-then-settle 的定义就是"事后才知道真实值"，
-- 所以它天然允许小幅超发（这是选这个口径时已知情的代价）。
-- 在这里拦下超额没有意义 —— 上游的钱已经花出去了。
-- 要收紧就换"上界预留"口径（预扣时按 max_tokens 预算），而不是在这里补一道闸。
--
-- 注意：SET 的返回值是 status reply（"OK"），**不能直接 return** ——
-- Lua 里 status reply 会变成 {ok="OK"} 表，Redis 再转回 status，
-- 而 Java 侧声明的是 DefaultRedisScript<Long>，反序列化会直接失败。
-- 必须显式 return 数值。这个坑只有真的执行一次才会暴露（单测与静态检查都看不出来）。
redis.call('SET', key, target, 'EX', ttl)
return target
