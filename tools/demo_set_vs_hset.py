#!/usr/bin/env python3
"""
SET vs HSET race-condition demo.

Simulates two concurrent feature writers:
  • Batch job    – nightly run, writes offline features (avg_bookings_90d, etc.)
  • Stream consumer – per-event, writes online features (searches_5m, etc.)

When both call SET(key, json_blob_of_only_their_fields), the second writer
always clobbers the first.  The demo shows fields disappearing under load,
then proves the HSET fix eliminates the problem.

Run:
    python3 tools/demo_set_vs_hset.py        # no live Redis needed
    python3 tools/demo_set_vs_hset.py --live  # connect to localhost:6379
"""

from __future__ import annotations

import argparse
import json
import sys
import textwrap
import threading
import time
from typing import Any

# ─── In-process Redis simulator ──────────────────────────────────────────────
# Implements exactly the SET / HSET semantics we need, without a live container.


class _FakeRedis:
    """Thread-safe minimal Redis subset: SET / GET / HSET / HGETALL / DELETE."""

    def __init__(self) -> None:
        self._store: dict[str, Any] = {}
        self._lock = threading.Lock()

    def set(self, key: str, value: str) -> None:
        with self._lock:
            self._store[key] = ("str", value)

    def get(self, key: str) -> str | None:
        with self._lock:
            entry = self._store.get(key)
            return entry[1] if entry and entry[0] == "str" else None

    def hset(self, key: str, *, mapping: dict[str, str]) -> None:
        with self._lock:
            entry = self._store.get(key)
            current: dict[str, str] = (
                entry[1] if (entry and entry[0] == "hash") else {}
            )
            current.update(mapping)
            self._store[key] = ("hash", current)

    def hgetall(self, key: str) -> dict[str, str]:
        with self._lock:
            entry = self._store.get(key)
            return dict(entry[1]) if (entry and entry[0] == "hash") else {}

    def delete(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)


# ─── Feature payloads ─────────────────────────────────────────────────────────

KEY = "feat:route:london"

# Fields the nightly batch job writes.
BATCH_FIELDS: dict[str, str] = {
    "avg_bookings_90d":      "312.5",
    "seasonality_mon":       "1.12",
    "lead_time_p50":         "2.4",
    "batch_computed_at":     "2024-01-17T02:30:00+00:00",
}

# Fields the stream consumer writes.
STREAM_FIELDS: dict[str, str] = {
    "searches_5m":           "7.0",
    "bookings_15m":          "3.0",
    "look_to_book_1h":       "0.4286",
    "stream_computed_at":    "2024-01-17T14:32:01+00:00",
}


# ─── Printing helpers ─────────────────────────────────────────────────────────

def _banner(title: str) -> None:
    print()
    print("═" * 78)
    print(f"  {title}")
    print("═" * 78)


def _hr() -> None:
    print("─" * 78)


def _show_set_key(r: Any, step: str) -> tuple[bool, bool]:
    """Read a SET key (JSON string), print status, return (has_batch, has_stream)."""
    raw = r.get(KEY)
    current: dict[str, str] = json.loads(raw) if raw else {}
    return _print_status(current, step)


def _show_hset_key(r: Any, step: str) -> tuple[bool, bool]:
    """Read an HSET key (hash), print status, return (has_batch, has_stream)."""
    current = r.hgetall(KEY)
    return _print_status(current, step)


def _print_status(current: dict[str, str], step: str) -> tuple[bool, bool]:
    has_batch  = all(f in current for f in BATCH_FIELDS)
    has_stream = all(f in current for f in STREAM_FIELDS)

    # Abbreviate the displayed value so it fits on one line.
    items = list(current.items())
    shown = dict(items[:2])
    suffix = f" … +{len(items) - 2} more" if len(items) > 2 else ""
    print(f"  {step}")
    print(f"    value : {json.dumps(shown)}{suffix}")

    b_marker = "✓ batch"  if has_batch  else "✗ BATCH FIELDS GONE"
    s_marker = "✓ stream" if has_stream else "✗ STREAM FIELDS GONE"
    print(f"    state : {b_marker}   {s_marker}")
    return has_batch, has_stream


# ─── Part 1 ── Sequential SET (broken) ───────────────────────────────────────

def part1_broken_sequential(r: Any) -> None:
    _banner("PART 1 — Broken: sequential SET writes")
    print(textwrap.dedent("""
      Each job serialises ALL of its own fields into a JSON blob and calls
      SET(key, blob).  SET replaces the entire key value, so whichever job
      runs second clobbers the first job's fields.
    """).strip())

    r.delete(KEY)

    print()
    r.set(KEY, json.dumps(BATCH_FIELDS))
    _show_set_key(r, "[1] batch job  → SET key {avg_bookings_90d, seasonality_mon, "
                     "lead_time_p50, batch_computed_at}")

    print()
    r.set(KEY, json.dumps(STREAM_FIELDS))
    _show_set_key(r, "[2] stream consumer → SET key {searches_5m, bookings_15m, "
                     "look_to_book_1h, stream_computed_at}")
    print("      ^^ batch fields are gone — nightly compute is overwritten")

    print()
    r.set(KEY, json.dumps(BATCH_FIELDS))
    _show_set_key(r, "[3] batch job  → SET key again (next nightly run)")
    print("      ^^ stream fields are gone — real-time signals are overwritten")


# ─── Part 2 ── Concurrent SET (broken) ───────────────────────────────────────

def part2_broken_concurrent(r: Any, rounds: int = 80) -> None:
    _banner(f"PART 2 — Broken: concurrent SET  ({rounds} rounds each writer)")
    print(textwrap.dedent(f"""
      Two threads run simultaneously.  A checker thread samples the key every
      0.5 ms and flags whenever a writer's fields are absent from the key.
    """).strip())

    r.delete(KEY)

    batch_gone  = 0
    stream_gone = 0
    samples     = 0
    done        = threading.Event()
    lock        = threading.Lock()

    def batch_writer() -> None:
        for _ in range(rounds):
            r.set(KEY, json.dumps(BATCH_FIELDS))
            time.sleep(0.001)

    def stream_writer() -> None:
        for _ in range(rounds):
            r.set(KEY, json.dumps(STREAM_FIELDS))
            time.sleep(0.001)

    def checker() -> None:
        nonlocal batch_gone, stream_gone, samples
        while not done.is_set():
            raw = r.get(KEY)
            if raw:
                current = json.loads(raw)
                with lock:
                    samples += 1
                    if not all(f in current for f in BATCH_FIELDS):
                        batch_gone += 1
                    if not all(f in current for f in STREAM_FIELDS):
                        stream_gone += 1
            time.sleep(0.0005)

    t_checker = threading.Thread(target=checker,       daemon=True)
    t_batch   = threading.Thread(target=batch_writer,  daemon=True)
    t_stream  = threading.Thread(target=stream_writer, daemon=True)

    t_checker.start()
    t_batch.start()
    t_stream.start()
    t_batch.join()
    t_stream.join()
    done.set()
    t_checker.join(timeout=1.0)

    print()
    print(f"  Samples taken       : {samples:5d}")
    pct_b = 100 * batch_gone  // max(1, samples)
    pct_s = 100 * stream_gone // max(1, samples)
    print(f"  Batch fields gone   : {batch_gone:5d} / {samples}  ({pct_b:3d}%)")
    print(f"  Stream fields gone  : {stream_gone:5d} / {samples}  ({pct_s:3d}%)")
    print()
    total_corrupt = batch_gone + stream_gone
    if total_corrupt > 0:
        print(f"  ✗  {total_corrupt} samples had missing fields — "
              "the key never holds both writers' work at once.")
    else:
        print("  (no corruption observed — rerun to see the race)")


# ─── Part 3 ── Sequential HSET (fixed) ───────────────────────────────────────

def part3_fixed_sequential(r: Any) -> None:
    _banner("PART 3 — Fixed: sequential HSET writes")
    print(textwrap.dedent("""
      Each job calls HSET(key, mapping=only_their_fields).
      HSET writes only the specified fields; all other hash fields are
      left untouched.  Concurrent writers operate on disjoint field sets,
      so neither can overwrite the other.
    """).strip())

    r.delete(KEY)

    print()
    r.hset(KEY, mapping=BATCH_FIELDS)
    _show_hset_key(r, "[1] batch job  → HSET key {avg_bookings_90d, seasonality_mon, "
                      "lead_time_p50, batch_computed_at}")
    print("      (stream fields not yet written — expected)")

    print()
    r.hset(KEY, mapping=STREAM_FIELDS)
    _show_hset_key(r, "[2] stream consumer → HSET key {searches_5m, bookings_15m, "
                      "look_to_book_1h, stream_computed_at}")
    print("      ^^ both field-sets coexist ✓")

    print()
    r.hset(KEY, mapping=BATCH_FIELDS)
    _show_hset_key(r, "[3] batch job  → HSET key again (next nightly run)")
    print("      ^^ stream fields untouched — real-time signals survive the nightly sync ✓")


# ─── Part 4 ── Concurrent HSET (fixed) ───────────────────────────────────────

def part4_fixed_concurrent(r: Any, rounds: int = 80) -> None:
    _banner(f"PART 4 — Fixed: concurrent HSET  ({rounds} rounds each writer)")

    r.delete(KEY)
    # Pre-seed both field sets so the checker never fires during the first
    # write (before the other writer has had a chance to run).
    r.hset(KEY, mapping=BATCH_FIELDS)
    r.hset(KEY, mapping=STREAM_FIELDS)

    batch_gone  = 0
    stream_gone = 0
    samples     = 0
    done        = threading.Event()
    lock        = threading.Lock()

    def batch_writer() -> None:
        for _ in range(rounds):
            r.hset(KEY, mapping=BATCH_FIELDS)
            time.sleep(0.001)

    def stream_writer() -> None:
        for _ in range(rounds):
            r.hset(KEY, mapping=STREAM_FIELDS)
            time.sleep(0.001)

    def checker() -> None:
        nonlocal batch_gone, stream_gone, samples
        while not done.is_set():
            current = r.hgetall(KEY)
            if current:
                with lock:
                    samples += 1
                    if not all(f in current for f in BATCH_FIELDS):
                        batch_gone += 1
                    if not all(f in current for f in STREAM_FIELDS):
                        stream_gone += 1
            time.sleep(0.0005)

    t_checker = threading.Thread(target=checker,       daemon=True)
    t_batch   = threading.Thread(target=batch_writer,  daemon=True)
    t_stream  = threading.Thread(target=stream_writer, daemon=True)

    t_checker.start()
    t_batch.start()
    t_stream.start()
    t_batch.join()
    t_stream.join()
    done.set()
    t_checker.join(timeout=1.0)

    print()
    print(f"  Samples taken       : {samples:5d}")
    pct_b = 100 * batch_gone  // max(1, samples)
    pct_s = 100 * stream_gone // max(1, samples)
    print(f"  Batch fields gone   : {batch_gone:5d} / {samples}  ({pct_b:3d}%)")
    print(f"  Stream fields gone  : {stream_gone:5d} / {samples}  ({pct_s:3d}%)")
    print()
    total_corrupt = batch_gone + stream_gone
    if total_corrupt == 0:
        print("  ✓  Zero field loss across all samples.")
    else:
        print(f"  ✗  Unexpected: HSET should never lose fields "
              f"({total_corrupt} violations).")


# ─── Entry point ─────────────────────────────────────────────────────────────

def _make_redis(live: bool) -> Any:
    if not live:
        return _FakeRedis()
    try:
        import redis
        r = redis.Redis(host="localhost", port=6379, db=15, decode_responses=True)
        r.ping()
        print("Connected to localhost:6379 db=15", file=sys.stderr)
        return r
    except Exception as exc:
        print(f"Redis unavailable ({exc}); falling back to in-process fake.",
              file=sys.stderr)
        return _FakeRedis()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--live", action="store_true",
                    help="Use localhost:6379 instead of in-process fake")
    args = ap.parse_args()

    r = _make_redis(args.live)

    part1_broken_sequential(r)
    part2_broken_concurrent(r)
    part3_fixed_sequential(r)
    part4_fixed_concurrent(r)

    print()
    _hr()
    print()
    print("  SUMMARY")
    print()
    print("  SET  replaces the entire key value.  Two writers each storing")
    print("       only their own fields means the key always holds exactly one")
    print("       writer's work — the other's is silently lost.")
    print()
    print("  HSET operates at field granularity.  Writers with disjoint field")
    print("       sets can run concurrently without interfering.  The hash")
    print("       accumulates fields from all writers and never loses any.")
    print()
    _hr()
    print()


if __name__ == "__main__":
    main()
