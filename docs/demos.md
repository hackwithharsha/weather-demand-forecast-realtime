# Demos

## SET vs HSET: feature fields disappearing under concurrent writes

**File**: `tools/demo_set_vs_hset.py`
**Run**: `python3 tools/demo_set_vs_hset.py` (no live Redis required; uses an in-process simulator)

### The scenario

Two independent jobs write to the same Redis key `feat:route:london`:

| Writer | Fields | Trigger |
|---|---|---|
| Batch job | `avg_bookings_90d`, `seasonality_mon`, `lead_time_p50`, `batch_computed_at` | Nightly at 02:30 UTC |
| Stream consumer | `searches_5m`, `bookings_15m`, `look_to_book_1h`, `stream_computed_at` | Every demand event |

The field sets are **disjoint** — neither job cares about the other's fields.
The key is the same — both jobs target `feat:route:london`.

---

### Part 1 — Broken: sequential SET writes

Each job serialises only its own fields into a JSON blob and calls `SET key blob`.
`SET` replaces the **entire value**; the previous writer's fields are gone.

```
[1] batch job  → SET key {avg_bookings_90d, seasonality_mon, lead_time_p50, batch_computed_at}
    value : {"avg_bookings_90d": "312.5", "seasonality_mon": "1.12"} … +2 more
    state : ✓ batch   ✗ STREAM FIELDS GONE

[2] stream consumer → SET key {searches_5m, bookings_15m, look_to_book_1h, stream_computed_at}
    value : {"searches_5m": "7.0", "bookings_15m": "3.0"} … +2 more
    state : ✗ BATCH FIELDS GONE   ✓ stream
      ^^ batch fields are gone — nightly compute is overwritten

[3] batch job  → SET key again (next nightly run)
    value : {"avg_bookings_90d": "312.5", "seasonality_mon": "1.12"} … +2 more
    state : ✓ batch   ✗ STREAM FIELDS GONE
      ^^ stream fields are gone — real-time signals are overwritten
```

The key ping-pongs between containing only batch fields and only stream fields.
It **never** holds both at the same time.

---

### Part 2 — Broken: concurrent SET (80 rounds each writer)

Two threads writing continuously.  A checker thread samples the key every 0.5 ms
and counts samples where either writer's fields are absent.

```
  Samples taken       :   159
  Batch fields gone   :    76 / 159  ( 47%)
  Stream fields gone  :    83 / 159  ( 52%)

  ✗  159 samples had missing fields — the key never holds both writers' work at once.
```

Every single sample is corrupt.  At any instant the key contains at most one
writer's fields — whichever job happened to run last.  The other's work is
silently discarded.

This is not a timing fluke.  It is **structural**: `SET` is a complete
replacement.  Two writers with disjoint field sets cannot both be right.

---

### Part 3 — Fixed: sequential HSET writes

Each job calls `HSET key mapping={only_their_fields}`.
`HSET` writes only the specified fields; all other hash fields are untouched.

```
[1] batch job  → HSET key {avg_bookings_90d, seasonality_mon, lead_time_p50, batch_computed_at}
    value : {"avg_bookings_90d": "312.5", "seasonality_mon": "1.12"} … +2 more
    state : ✓ batch   ✗ STREAM FIELDS GONE
      (stream fields not yet written — expected)

[2] stream consumer → HSET key {searches_5m, bookings_15m, look_to_book_1h, stream_computed_at}
    value : {"avg_bookings_90d": "312.5", "seasonality_mon": "1.12"} … +6 more
    state : ✓ batch   ✓ stream
      ^^ both field-sets coexist ✓

[3] batch job  → HSET key again (next nightly run)
    value : {"avg_bookings_90d": "312.5", "seasonality_mon": "1.12"} … +6 more
    state : ✓ batch   ✓ stream
      ^^ stream fields untouched — real-time signals survive the nightly sync ✓
```

After the first write from each job, both field-sets coexist permanently.
Subsequent runs by either job update only their own fields.

---

### Part 4 — Fixed: concurrent HSET (80 rounds each writer)

Same two-thread / checker setup as Part 2.

```
  Samples taken       :   158
  Batch fields gone   :     0 / 158  (  0%)
  Stream fields gone  :     0 / 158  (  0%)

  ✓  Zero field loss across all samples.
```

---

### Why this matters for the feature store

The batch job and stream consumer write to `feat:route:{route_id}` on completely
different schedules — nightly vs. per-event.  They share the key because the API
reads **all** features in a single `HGETALL`.  Keeping the key unified means the
serving path needs one round-trip regardless of how many independent writers exist.

If either job used `SET`:

* Every nightly batch run would erase `searches_5m`, `bookings_15m`,
  `look_to_book_1h`, `weather_*`, and `stream_computed_at`.
* Every stream event would erase `avg_bookings_90d`, `seasonality_*`,
  `lead_time_*`, `cancellation_rate_180d`, `elasticity_estimate`, and
  `batch_computed_at`.

The model would silently receive stale or missing features — no error raised,
no alert fired.  The only symptom would be degraded forecast accuracy.

`HSET` eliminates the problem at the protocol level: Redis itself enforces
field-granularity writes, so no amount of scheduling coincidence can cause one
job to overwrite the other's fields.

See `docs/decisions.md §"HSET vs SET for the feature store"` for the full ADR.

---

### Code diff: broken → fixed

```python
# ── BROKEN ────────────────────────────────────────────────────────────────────
# Batch job
r.set(
    f"feat:route:{route_id}",
    json.dumps({f.name: str(row[f.name]) for f in ROUTE_FEATURES.by_source("batch")}),
)

# Stream consumer
r.set(
    f"feat:route:{route_id}",
    json.dumps({
        "searches_5m":        str(searches_5m),
        "bookings_15m":       str(bookings_15m),
        "stream_computed_at": datetime.now(timezone.utc).isoformat(),
    }),
)

# ── FIXED ─────────────────────────────────────────────────────────────────────
# Batch job  (feature_store.py:_sync_to_redis)
pipe.hset(
    f"feat:route:{route_id}",
    mapping={f.name: str(row[f.name]) for f in ROUTE_FEATURES.by_source("batch")},
)

# Stream consumer  (stream_features/redis_ops.py:update_demand_features)
r.hset(
    f"feat:route:{route_id}",
    mapping={
        _F_SEARCHES_5M:          str(float(searches_5m)),
        _F_BOOKINGS_15M:         str(float(bookings_15m)),
        _F_LOOK_TO_BOOK:         str(round(look_to_book_1h, 6)),
        STREAM_COMPUTED_AT_FIELD: datetime.now(timezone.utc).isoformat(),
    },
)
```

The change is **one word** per call site — `set` → `hset` and `json.dumps({...})`
→ `mapping={...}`.  The Redis hash model does the rest.
