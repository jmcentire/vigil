# vigil

Sub-threshold anomaly detection for the Exemplar stack.

vigil watches event streams + trace samples + contract violations for
pattern divergence that doesn't trip a single alarm but represents
real systemic weirdness — the canonical case being "47 LLM timeouts
in an hour for tenant X on the same prompt pattern". No single
timeout is alarming; the cluster is.

vigil is the cross-stack home for the anomaly-detection layer that
sits *below* the alarm-as-code primitive. Where alarms fire on
declared predicates, vigil fires on pattern divergence from observed
history.

- `SPEC.md` — charter
- `ADR-001-extraction.md` — architecture lock
- `docs/detection-math.md` — formal V1 detection model

## V1 in one paragraph

For each `(component, op, tenant)` bucket, vigil maintains a rolling
7-day 95th-percentile of bucket-values (5-minute event counts).
When a 5-minute observed bucket exceeds `MULTIPLIER * baseline_p95`
(default `3.0`), an `Anomaly` row is written. Operators query the
forensic API, investigate, and either dismiss with a reason
(noise / known-issue) or escalate via the operator dashboard. vigil
NEVER auto-rolls back; humans decide.

## Install

```bash
pip install -e .[dev]
```

Quality gate:

```bash
pytest
```

## Run

vigil reads from Reeve's `trace_samples` table and writes to its own
`anomalies` table. Set the connection strings:

```bash
export REEVE_DATABASE_URL="postgresql://reeve_ro@host/reeve_main"
export VIGIL_DATABASE_URL="postgresql://vigil_rw@host/vigil_main"
vigil migrate                # create the anomalies table
vigil run                    # ingest+detect loop + HTTP API on :8080
```

Inspection:

```bash
vigil query --since 24h               # tail recent anomalies
vigil query --component reeve.adapters.llm --json | jq
vigil baseline                        # current baselines per key
```

## Operational workflow

1. **Ingest.** Every 60s, the run loop pulls new `trace_samples` rows
   from Reeve since the cursor (idempotent — re-running on the same
   data leaves baselines unchanged). It aggregates each row's `entries`
   into per-(component, op, tenant) 5-minute event-count buckets.
2. **Detect.** For each key with a warm baseline (>=
   `VIGIL_MIN_BUCKETS`), the most recent 5-minute bucket value is
   compared to `3 * baseline_p95`. Above the line → an `Anomaly` is
   inserted.
3. **Review.** Operators query `/v1/anomalies` (or via Reeve's future
   operator dashboard tab — see "Wiring from Reeve" below). Each
   anomaly carries `sample_event_ids` pointing back to the
   `trace_samples` rows that contributed.
4. **Dismiss.** Anomalies confirmed as noise or known-issue are
   `POST /v1/anomalies/{id}/dismiss`-ed with a reason. Dismissed
   anomalies fall out of the default queries; they're available with
   `?include_dismissed=true` for retrospectives.

## Configuration

All env vars; no config file.

| var | default | description |
|---|---|---|
| `VIGIL_MULTIPLIER` | `3.0` | Multiplicative threshold against baseline_p95 |
| `VIGIL_BASELINE_WINDOW_SECONDS` | `604800` (7d) | Rolling baseline window |
| `VIGIL_OBSERVED_WINDOW_SECONDS` | `300` (5m) | Observed window |
| `VIGIL_BUCKET_SECONDS` | `300` (5m) | Time-bucket size for aggregation |
| `VIGIL_MIN_BUCKETS` | `24` | Minimum warmed buckets before a key is detectable |
| `VIGIL_LOOP_INTERVAL_SECONDS` | `60` | How often the ingest+detect loop runs |
| `VIGIL_BASELINE_SNAPSHOT_PATH` | `/var/lib/vigil/baselines.json` | Where baselines persist between restarts |
| `VIGIL_BASELINE_SNAPSHOT_INTERVAL_SECONDS` | `300` | How often the snapshot is written |
| `VIGIL_API_HOST` | `0.0.0.0` | HTTP API host |
| `VIGIL_API_PORT` | `8080` | HTTP API port |
| `VIGIL_DEFAULT_PAGE_SIZE` | `100` | Default `/v1/anomalies` page size |
| `VIGIL_MAX_PAGE_SIZE` | `500` | Max `?limit=` |
| `REEVE_DATABASE_URL` | (none) | Read-only DSN for Reeve's `trace_samples` |
| `VIGIL_DATABASE_URL` | (none) | Read-write DSN for vigil's own DB |

If `REEVE_DATABASE_URL` is unset, the loop logs `ingest skipped` and
the API stays available — vigil degrades to "stale baselines, no new
detection" rather than crashing.

## HTTP API

```
GET  /v1/health
GET  /v1/anomalies?component=&op=&tenant=&since=24h&limit=100&offset=0
GET  /v1/anomalies/{id}
POST /v1/anomalies/{id}/dismiss   { "reason": "..." }
```

`?since=` accepts `60s`, `30m`, `24h`, `7d`, or ISO-8601. Default
behavior excludes dismissed anomalies; pass `include_dismissed=true`
to see them.

FastAPI auto-generates `/docs` (Swagger) and `/openapi.json`.

## Wiring from Reeve

vigil's intended operator surface is a tab in Reeve's operator
dashboard ("Anomalies" alongside "Alarms"). The intended wiring is a
read-mostly proxy from Reeve's web layer to vigil's `/v1/anomalies`
endpoint, scoped to the operator's tenant set:

```ts
// reeve/src/web/routes/operator/anomalies.ts (Wave 3 work)
const resp = await fetch(
  `${VIGIL_BASE_URL}/v1/anomalies?since=24h&limit=50&tenant=${tenantId}`,
  { headers: { 'X-Reeve-Operator': operatorId } },
);
const { anomalies } = await resp.json();
return c.html(<AnomaliesTable rows={anomalies} />);
```

Dismissals flow through the same proxy:

```ts
await fetch(`${VIGIL_BASE_URL}/v1/anomalies/${id}/dismiss`, {
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ reason: form.get('reason') }),
});
```

vigil itself does not know who Reeve's operators are; it trusts the
network boundary (Fly.io private IPv6, or a Tailscale/Cloudflare
Access tunnel — operator's choice). Per-operator audit lives in
Reeve's request log, not vigil's. This is intentional: vigil is
forensic, not authoritative.

## Deployment

vigil is a single-instance service. Stateful only in `anomalies`,
`ingest_cursors`, and `vigil_migrations` (all in `VIGIL_DATABASE_URL`)
plus the on-disk baseline snapshot. Recover by restoring the DB and
deleting the snapshot — the loop will recompute the baseline from
the rolling 7-day window of `trace_samples` on next ingest.

A `Dockerfile` and `fly.toml` are included; `fly deploy` from the
repo root deploys to a single shared-cpu-1x VM with a 1GB volume
mounted at `/var/lib/vigil` for the baseline snapshot.

## Out of scope (V1)

- Per-component-class detection math (latency vs rate vs error-rate).
  ADR-002.
- Baton event-channel subscription. V1 pulls from Reeve only; V2
  subscribes to Baton's pub/sub for sub-second detection.
- CUSUM / EWMA / ML-based clustering. ADR-002+.
- Operator UI in Reeve. Wave 3.
- Per-tenant override tables. Operators tune via env vars + dismissal
  reasons; persistent overrides ADR-003.

## License

MIT.

## Open questions (Wave 1 implementation surfaced)

- Should `sample_event_ids` ever store IDs from sources other than
  Reeve's `trace_samples`? Currently they're all UUIDs from one
  table; if Baton's event log gets included in V2, we may need
  source-tagged IDs (`reeve:uuid`, `baton:uuid`).
- The `_run_loop` thread coexists with uvicorn's signal handlers
  ungracefully on macOS — works fine on Linux. Production runs on
  Linux so V1 ships as-is; a clean async-only refactor is on deck
  if local dev pain mounts.
- `MIN_BUCKETS_FOR_BASELINE=24` is a guess. Real production data
  will tell us whether 24 (=2 hours of 5-min buckets) is right or
  whether keys with regular weekly cycles need overnight to warm.
