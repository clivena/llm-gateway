CREATE TABLE IF NOT EXISTS quota_ledger (
  id           BIGSERIAL PRIMARY KEY,
  api_key      VARCHAR(64) NOT NULL,
  period       VARCHAR(7)  NOT NULL,
  used_tokens  BIGINT      NOT NULL DEFAULT 0,
  limit_tokens BIGINT      NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (api_key, period)
);

CREATE TABLE IF NOT EXISTS cost_ledger (
  id                BIGSERIAL PRIMARY KEY,
  trace_id          VARCHAR(64)  NOT NULL,
  api_key           VARCHAR(64)  NOT NULL,
  feature           VARCHAR(64),
  model             VARCHAR(64)  NOT NULL,
  cache_hit         BOOLEAN      NOT NULL DEFAULT false,
  prompt_tokens     INT          NOT NULL DEFAULT 0,
  completion_tokens INT          NOT NULL DEFAULT 0,
  cost              NUMERIC(12,6) NOT NULL DEFAULT 0,
  latency_ms        INT,
  created_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cost_ledger_key_time ON cost_ledger (api_key, created_at);

CREATE TABLE IF NOT EXISTS model_route (
  task_type      VARCHAR(32) NOT NULL,
  tier           VARCHAR(16) NOT NULL,
  model          VARCHAR(64) NOT NULL,
  cost_per_1k    NUMERIC(10,6) NOT NULL,
  quality_score  NUMERIC(4,3),
  PRIMARY KEY (task_type, tier)
);
