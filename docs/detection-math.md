# Detection math (V1)

vigil's V1 anomaly detector is intentionally one-method, one-paragraph,
one-screen. This doc is the formal description; the math is simple
because the value is in *picking the right simple math*, not in the
sophistication of the model.

## The model

For each `(component, op, tenant)` tuple, vigil maintains a rolling
time series of **bucket values** — counts of events landing in
fixed-grid 5-minute windows over the past 7 days. Call this series
`B_{c,o,t}`. At evaluation time:

1. Compute `baseline_p95 = quantile(B_{c,o,t}, 0.95)` over the rolling
   window. Use linear interpolation (numpy's default).
2. Compute `observed = max bucket value in the last 5 minutes` for the
   same key. (V1 observed window equals one bucket period; the `max`
   is degenerate. The shape generalizes: when the observed window
   spans multiple buckets — ADR-002 — `max` over the buckets is the
   right summary because we want to fire on a 5-minute spike inside a
   30-minute window, not its average.)
3. Fire an `Anomaly` iff `observed > MULTIPLIER * baseline_p95`, where
   `MULTIPLIER` defaults to 3.0 and is set per deployment via the
   `VIGIL_MULTIPLIER` env var.

`baseline_p95` is the 95th-percentile of bucket values over the
rolling 7-day window, **not** of raw events. This matters: bursty
traffic that fits inside one bucket spikes that bucket's value, which
shows up as a tail in the bucket-value distribution where vigil can
see it. Aggregating to per-event quantiles instead would launder the
burst back into the body of the distribution and miss the signal.

The `pattern_id` of an emitted anomaly is the SHA-256 of
`f"{component}|{op}|{tenant_id}|{utc_date}"`. Same key on the same UTC
day collapses to one `pattern_id`; cross-day re-firings get a fresh
one (operators see "this came back today" instead of "this is still
firing"). Within-day repeated firings produce multiple anomaly rows
sharing one `pattern_id` — useful for forensic queries like "when in
the day did this start".

## Why quantile, not z-score

A first draft of vigil proposed rolling z-score (mean ± k·sigma).
Sim's review (locked in `ADR-001-extraction.md`) rejected it. The
argument is short and final:

**Request rates are not normal.** They are log-normal, multimodal, or
bursty. A z-score against a non-normal distribution produces
systematic false positives — every Monday batch job crosses z=2,
every backfill run lights up like Christmas, every burst-prone
endpoint trains operator alert fatigue inside a week.

The simulacrum's exact words: *"z-score on log-normal data isn't
unbiased; it's hostile to the operator."* If vigil's anomalies become
noise, vigil becomes counter-productive — the operator stops
checking, the signal vanishes inside the noise, and we ship a service
that costs more attention than it saves.

The quantile + multiplicative formulation:

- **Distribution-free.** Quantiles don't assume normality. They work
  for log-normal, multimodal, and bursty data with no transformation.
  A traffic distribution that looks bimodal on Mondays still has a
  stable 95th percentile; we fire on multiples of *that*.
- **Self-tuning per tenant.** A high-volume tenant has a high
  baseline_p95; a low-volume tenant has a low baseline_p95. Fifty
  events in five minutes is noise for the first and a genuine
  anomaly for the second. The multiplicative threshold scales
  automatically — no per-tenant tuning needed.
- **One operator dial.** `VIGIL_MULTIPLIER` is the only knob.
  Operators understand "fire when observed is more than Nx the
  baseline 95th-percentile" without statistical training. This is
  rare for an anomaly detector and we should keep it that way.

## What V1 catches

- **Sustained rate spikes** in a (component, op, tenant) bucket that
  exceed 3x the rolling-7d 95th-percentile. The canonical case from
  the SPEC: "47 LLM timeouts in an hour for tenant X on the same
  prompt pattern." Each timeout is uneventful; the cluster is the
  signal, and the cluster shows up as a multi-bucket sequence each
  exceeding the `3x p95` line.
- **Per-tenant context.** A tenant who normally sends 1 message/min
  and suddenly sends 50/min lights up; a tenant who normally sends
  5000/min and goes to 5500/min does not. This is the right
  asymmetry — operators care about the *change* relative to normal
  for that tenant, not the absolute rate.

## What V1 misses (honestly)

This list is the V1 acceptance criteria for "we know what we
shipped":

- **Slow rises below the multiplier.** A 2x increase that drifts up
  for hours is still 2x — V1 does not fire. ADR-002+ would add CUSUM
  or an EWMA-derivative trigger to catch slow burns. Operators who
  need this today get it from slice-6 alarms with declared
  thresholds.
- **Distribution shifts that don't move the p95.** If a tenant's
  traffic shape changes — same volume, different ops — V1 doesn't
  see it. ADR-002+ would add per-(component, op) anomaly correlation
  (the same anomaly seen from two views).
- **Rare-event anomalies.** Events that fire less than 1% of the time
  contribute to the *body* of the bucket-value distribution at zero,
  not the tail. A 0→1 transition is currently suppressed by the
  zero-baseline guard (see "Degenerate cases" below) — this is
  intentional but worth knowing. A future "rare event" detector is
  separate math.
- **Latency anomalies.** V1 metric is event-count per bucket, not
  per-event latency. ADR-002 will add per-component-class baselines:
  latency-p95, error-rate, etc. The current shape generalizes but
  the math doesn't yet support it.
- **Cross-tenant patterns.** A single attacker hitting twenty tenants
  at low rate per tenant won't fire on any one (component, op,
  tenant) bucket. Cross-tenant aggregation is V2.
- **Correlation between two anomalies.** If component A's anomaly is
  caused by component B's anomaly, V1 reports them independently.
  Composition is V2.
- **Cold start.** Until `min_buckets_for_baseline` (default 24)
  buckets exist for a key, the key is invisible to detection. This
  is intentional — false positives during warmup train operator
  fatigue more aggressively than false negatives during warmup hide
  real problems. The real problems are still in the rolling-90d data
  Reeve is keeping; vigil sees them after the warmup window.

## Distribution assumptions (explicit)

vigil's quantile-based detection is **distribution-free in the body**
and **threshold-based in the tail**. We assume:

- The rolling-7d p95 of bucket values is a stable representation of
  "normal upper traffic" for a (component, op, tenant) tuple.
  Empirically this holds for traffic with a regular weekly cycle
  (which most workloads have), even when the distribution within
  that cycle is bimodal or bursty. It does NOT hold for "permanently
  growing" workloads with a non-stationary trend.
- The metric (event count per bucket) is non-negative.
- Buckets are time-aligned to a fixed UTC grid. Two events arriving
  in the same physical 5-minute window land in the same bucket
  regardless of timezone.
- Late-arriving events (more than ~7 days late) are dropped silently
  by the trim policy. This is correct for all current upstream
  emitters (Reeve writes trace_samples synchronously; Baton's
  event channel will write within seconds of the event).

## Degenerate cases

vigil refuses to emit an anomaly in three cases. Each is documented
because the absence of an emit is itself a design statement:

1. **Cold baseline.** Fewer than `min_buckets_for_baseline` buckets
   exist for the key in the rolling window. Returns `None`. Operator
   visible only via `vigil baseline` — by design.
2. **Zero baseline.** All buckets in the window have value zero
   (`baseline_p95 == 0`). Returns `None`. The "observed > 3 * 0"
   case is mathematically infinite-multiplier; in practice it
   represents a brand-new (component, op) emitting its first events.
   We refuse to fire because every new code path would otherwise
   trigger an anomaly on its first deploy. ADR-002 may add a
   "first-event" detector with its own threshold.
3. **At-threshold equality.** `observed == multiplier * baseline_p95`
   does NOT fire (strict-greater-than). This is a tie-breaking
   convention; the only practical effect is at integer-rounded test
   data.

## Tuning guidance

The default multiplier (3.0) is calibrated for the SPEC scenario:
"47 LLM timeouts in an hour for tenant X". On a baseline_p95 of ~3
timeouts per 5-minute bucket, 3x is exactly the boundary where the
cluster becomes visible.

If V1 is noisy in a deployment:

- Raise `VIGIL_MULTIPLIER` first — 4x or 5x for cases where the
  baseline_p95 is itself noisy.
- Raise `VIGIL_MIN_BUCKETS` for keys with infrequent activity (e.g.
  cron jobs that emit once an hour need more warmup before vigil
  trusts their baseline).
- Use `dismiss_reason="noise"` aggressively. The dismissal table
  becomes the second-pass training data when ADR-002 ships
  noise-suppression heuristics.

If V1 is silent in a deployment that has known anomalies:

- Lower `VIGIL_MULTIPLIER` to 2.0 first. Below 2.0 the multiplier is
  in the body of most bucket distributions and the false-positive
  rate climbs sharply.
- Inspect baselines via `vigil baseline` and check that the
  expected (component, op, tenant) keys are present. Cold baselines
  (skipped warmups) account for most "vigil missed it" reports.

## Performance notes

- Per-key memory: `(window_seconds / bucket_seconds)` floats. For
  the 7d/5m default, that's 2016 floats — about 16KB per key.
- Per-key p95 cost: O(n log n) on numpy, which for n=2016 is about
  20µs on commodity hardware. The full sweep (all keys) is bounded
  by the number of distinct (component, op, tenant) tuples in
  practice — observed in development at ~100 keys per Reeve tenant.
- The dominant cost is the ingest cycle's pull from Reeve's
  `trace_samples`; the in-memory math is a rounding error.

## Provenance

- Formal lock: `ADR-001-extraction.md`, 2026-05-06.
- The simulacrum's z-score-rejection argument is in
  `~/.claude/skills/simulacrum/run.py` against payload
  `["vigil", "z-score detection"]`. Do not bring back z-score
  without a fresh sim review.
