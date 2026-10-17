-- ============================================================================
-- 002 · cost_ledger 增加 provider 列
--
-- 为什么需要它：F2 引入故障转移后，同一个 model 可能在多家 provider 上部署。
-- 只记 model 时，账本回答不了"备用那家接管之后，成本与用量变成了什么样" ——
-- 而那正是故障期最该被看见的数字。
--
-- 取值约定：
--   - provider 名 = gateway.providers[].name
--   - 缓存命中写 'cache'（与 model='cache' 一致：这次没有走上游）
--   - NULL 只在"历史数据 / 极端异常"时出现，按 NULL 分组会显示为 (none)
--
-- 幂等：重复执行安全。应用方式：
--   docker compose exec -T postgres psql -U postgres -d llmgw < db/migrations/002-cost-ledger-provider.sql
-- ============================================================================

ALTER TABLE cost_ledger ADD COLUMN IF NOT EXISTS provider VARCHAR(64);

COMMENT ON COLUMN cost_ledger.provider IS
  '实际服务这次请求的 provider（缓存命中为 cache）；用于 F2 转移后的成本归因';

CREATE INDEX IF NOT EXISTS idx_cost_ledger_provider_time ON cost_ledger (provider, created_at);
