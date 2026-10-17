-- ============================================================================
-- 001 · cost_ledger 增加 task 列
--
-- 为什么要单独一个迁移文件，而不是只改 schema.sql：
--   schema.sql 挂在容器的 /docker-entrypoint-initdb.d 下，**只在数据目录为空时**
--   执行。已经跑起来的库永远不会再读它一遍 —— 只改 schema.sql 的结果是
--   "新克隆的人能跑，现有环境一写账本就因为列不存在而失败"，
--   而且失败被 CostLedgerService 吞成一条 warn，表现为"账本悄悄少了数据"。
--
-- 幂等：ADD COLUMN IF NOT EXISTS / CREATE INDEX IF NOT EXISTS，
-- 重复执行安全（这一点比"用版本表记录执行过没有"更重要 ——
-- 这个项目没有迁移框架，靠的是每条语句自己幂等）。
--
-- 应用方式：
--   docker compose exec -T postgres psql -U gateway -d gateway < db/migrations/001-cost-ledger-task.sql
-- ============================================================================

ALTER TABLE cost_ledger ADD COLUMN IF NOT EXISTS task VARCHAR(32);

COMMENT ON COLUMN cost_ledger.task IS
  '网关判定的任务（code/json/summarize/chat）；NULL 表示任务路由关闭、未发生任务判定';

CREATE INDEX IF NOT EXISTS idx_cost_ledger_task_time ON cost_ledger (task, created_at);
