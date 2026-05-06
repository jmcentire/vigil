-- vigil 001 — anomalies table.
--
-- One row per detected anomaly. ADR-001 locks quantile-based detection
-- (NOT z-score). The `multiplier` column is observed_value / baseline_value,
-- expressed as a numeric ratio (e.g., 4.7 means observed was 4.7x the
-- baseline_p95). When multiplier < threshold (env VIGIL_MULTIPLIER, default 3),
-- no row is written; this table only stores actual anomaly firings.
--
-- Forensic, not real-time. dismissed_at + dismiss_reason capture operator
-- triage decisions; un-dismissed rows are the live "weirdness" feed.

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS anomalies (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  -- Stable identity for the same anomaly recurring. Hash of
  -- (component, op, tenant_id, day-bucket of detected_at) so re-firings
  -- on the same day collapse. Cross-day re-firings get fresh pattern_ids
  -- so operators can see "this came back".
  pattern_id        text NOT NULL,
  -- The component that emitted the events. Free-form, e.g. 'reeve.adapters.llm'.
  component         text NOT NULL,
  -- The op within the component. Free-form, e.g. 'anthropic.messages.create'.
  op                text NOT NULL,
  -- NULL when the anomaly is unattributed (cron-triggered ops).
  tenant_id         uuid,
  -- The window the baseline was computed over. ADR-001 V1 = '7d'.
  baseline_window   text NOT NULL,
  -- The baseline value (rolling 7d 95th-percentile of the metric).
  baseline_value    numeric NOT NULL,
  -- The window the observed value was measured over. ADR-001 V1 = '5m'.
  observed_window   text NOT NULL,
  -- The observed value over the observed_window.
  observed_value    numeric NOT NULL,
  -- observed_value / baseline_value. The detection threshold compares
  -- this against env VIGIL_MULTIPLIER (default 3.0).
  multiplier        numeric NOT NULL,
  -- IDs of trace_samples (or other source events) that contributed to the
  -- observed_value. ID-only — vigil does NOT mirror payloads. Operators
  -- dereference into Reeve's trace_samples (or future Baton event log).
  sample_event_ids  text[] NOT NULL DEFAULT '{}',
  detected_at       timestamptz NOT NULL DEFAULT now(),
  -- Operator triage: when an anomaly is acknowledged as noise / known issue.
  dismissed_at      timestamptz,
  dismiss_reason    text,
  CONSTRAINT anomalies_dismiss_consistent CHECK (
    (dismissed_at IS NULL AND dismiss_reason IS NULL)
    OR (dismissed_at IS NOT NULL AND dismiss_reason IS NOT NULL)
  ),
  CONSTRAINT anomalies_multiplier_positive CHECK (multiplier > 0),
  CONSTRAINT anomalies_baseline_positive CHECK (baseline_value > 0)
);

-- Forensic queries: "show me un-dismissed anomalies for component X / op Y
-- in the last hour, newest first".
CREATE INDEX IF NOT EXISTS anomalies_recent_idx
  ON anomalies (component, op, detected_at DESC)
  WHERE dismissed_at IS NULL;

-- Per-tenant slice: "what's been weird for tenant X recently?"
CREATE INDEX IF NOT EXISTS anomalies_tenant_idx
  ON anomalies (tenant_id, detected_at DESC)
  WHERE dismissed_at IS NULL AND tenant_id IS NOT NULL;

-- Recurrence detection: pull all firings of the same pattern over time.
CREATE INDEX IF NOT EXISTS anomalies_pattern_idx
  ON anomalies (pattern_id, detected_at DESC);

-- Idempotency support for ingest: vigil records the cursor of the
-- highest source-event id it has consumed, per (component, source).
-- This table is small (one row per (source, component, op) tuple);
-- writes are infrequent (once per ingest cycle).
CREATE TABLE IF NOT EXISTS ingest_cursors (
  source        text NOT NULL,
  component     text NOT NULL,
  op            text NOT NULL,
  -- The highest ended_at timestamp consumed from the source. Vigil ingest
  -- pulls events with ended_at > last_event_at.
  last_event_at timestamptz NOT NULL,
  updated_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source, component, op)
);

COMMIT;
