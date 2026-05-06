# vigil — Sub-threshold Anomaly Detection

## Charter

Detect when execution patterns diverge from baseline before any single
predicate trips an alarm. The signal vigil catches is "47 LLM timeouts
in an hour for tenant X on the same prompt pattern" — no individual
timeout is alarming; the cluster is.

vigil is the cross-stack home for the anomaly-detection layer that
sits below the alarm-as-code primitive (Reeve roadmap slice 6). Where
alarms fire on declared predicates, vigil fires on pattern divergence
from observed history.

## Why this is not "just more alarms"

Alarms ask: "is this metric over its threshold right now?" Anomalies
ask: "is the distribution of recent activity unusual compared to the
last 24 hours?" The thresholds for "unusual" cannot be hand-tuned per
endpoint per tenant — there are too many of them, and they shift over
time. vigil owns the rolling baselines and the divergence math.

## Interface (proposed)

```typescript
export type EventKind = 'request' | 'tool_call' | 'llm_call' | 'fallback' | 'budget_exceeded' | 'violation';

export type ExecutionEvent = {
  kind: EventKind;
  outcome: 'ok' | 'fallback' | 'error' | 'budget_exceeded';
  tags: {
    component: string;
    op: string;
    tenantId?: string;
    correlationId?: string;
  };
  timing: { startedAt: number; durationMs: number };
  // Free-form fields used as feature dimensions for clustering.
  features?: Readonly<Record<string, string | number>>;
};

export type Anomaly = {
  detectedAt: number;
  patternId: string;            // stable across firings of the same anomaly
  baseline: { window: '1h' | '24h' | '7d'; metric: string; value: number };
  observed: { window: '1h'; metric: string; value: number };
  divergence: number;           // standardized — how many sigmas
  sample: ExecutionEvent[];     // up to N matching events for forensic context
};

export interface Vigil {
  recordEvent(event: ExecutionEvent): void;
  detect(): readonly Anomaly[];
  // For investigation: query historical events matching a pattern.
  query(filter: { tenantId?: string; op?: string; outcome?: string; since: number }): ExecutionEvent[];
}
```

## What vigil consumes

vigil is event-driven. It consumes:
- aegis budget-exceeded events
- covenant violation events
- baton canary state-change events
- reeve / apprentice / chronicler request and tool-call events

The expected emitter is each component's existing observability layer
(via baton's event channel). vigil is a separate consumer.

## What vigil emits

When an anomaly is detected:
1. An `Anomaly` event to baton's control port (so other consumers can
   subscribe).
2. A row in `anomalies` table with the sample events.
3. Optionally — with operator opt-in — a witness notification.

Anomalies do NOT auto-rollback. Rollback is a slice-5 canary-threshold
decision; an anomaly is a SIGNAL that one of those thresholds may need
to fire. The composition is: vigil detects → ops investigates → ops
adjusts thresholds → next deploy with the new thresholds catches it
automatically.

## Stack consumers

- **reeve** — operator dashboard surfaces vigil anomalies as a sub-tab
  of the alarm view.
- **baton** — could consume anomalies as one input to canary decision;
  not in initial release (signal quality unproven).
- **chronicler** — anomalies become "stories" for narrative event-log
  timelines.

## Sub-threshold trace sampling (slice 6.5 home)

The "1% sampled trace + queryable interface" from the roadmap's slice
6.5 lives here. vigil's `query()` is the operator-facing surface; the
sampling itself is an aegis tag-propagation feature where 1% of
requests get a `trace=true` annotation that triggers full event
emission.

## Open questions

1. Anomaly detection algorithm: simple z-score against rolling
   baseline, or statistical-process-control (CUSUM, EWMA), or
   ML-based clustering? Lean: start with z-score; it's interpretable.
2. Storage: rolling window in-memory + event-stream replay, or
   time-series DB (Prometheus, InfluxDB)? Lean: pg with tiered tables
   for now (operations don't need <1s detection latency).
3. False-positive control: how do we keep vigil from being noisy?
   Lean: every detection has a `dismiss-as-noise` operator action;
   vigil records dismissals and adjusts thresholds.

## Initial implementation plan

1. Spec lock: this doc + interface file.
2. First implementation lives at `reeve/src/observability/vigil/` as a
   private module.
3. Sampling first — the 1% trace sampling is shippable before the
   detection logic. Detection follows when there's enough data.
4. Extract when a second stack component needs the detection logic.

## Provenance

Spec'd 2026-05-05 from sim's NASA-bar review. Roadmap slices: 6
(alarm-as-code) and 6.5 (sub-threshold trace sampling).
