#!/usr/bin/env python3
"""
bench_sliding_window.py — latency / throughput benchmark for the Redis
sliding-window feature write path.

Tests exactly the two round-trips performed by update_demand_features():

  Round trip 1  pipeline(
                    ZADD? + ZREMRANGEBYSCORE×2 + ZCOUNT×4
                ).execute()
  Round trip 2  r.hset(route_key, mapping={…})

A concurrency sweep finds the aggregate event/s rate at which p99 write
latency first exceeds the 5 ms SLA target.

Usage:
    python3 tools/bench_sliding_window.py
    python3 tools/bench_sliding_window.py --host redis --port 6379
    python3 tools/bench_sliding_window.py --threads 1 2 4 8 16 32

Environment (all optional — flags take precedence):
    REDIS_HOST      default localhost
    REDIS_PORT      default 6379
    REDIS_PASSWORD  default changeme
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from typing import NamedTuple

import numpy as np
import redis

# ─── Benchmark knobs ─────────────────────────────────────────────────────────

ROUTES        = [f"bench_route_{i}" for i in range(10)]   # 10 simulated routes
WARMUP_S      = 3.0           # seconds per level to discard before measuring
MEASURE_S     = 10.0          # seconds of measurement per concurrency level
P99_TARGET_MS = 5.0           # SLA: p99 must stay below this
DEFAULT_LEVELS = [1, 2, 4, 8, 16, 32, 64]

# Mirrors the exact key patterns and window widths from redis_ops.py.
_W_5M   =   300
_W_15M  =   900
_W_1H   = 3_600

_SK = "bench:stream:{r}:searches"
_BK = "bench:stream:{r}:bookings"
_RK = "bench:feat:route:{r}"


# ─── Redis connection ─────────────────────────────────────────────────────────

def _connect(host: str, port: int, password: str) -> redis.Redis:
    pool = redis.ConnectionPool(
        host=host, port=port, password=password or None,
        decode_responses=True,
        max_connections=256,
        socket_connect_timeout=5,
        socket_timeout=5,
    )
    r = redis.Redis(connection_pool=pool)
    r.ping()
    return r


# ─── Single-event write (mirrors update_demand_features exactly) ──────────────

def _write_event(
    r: redis.Redis,
    route: str,
    event_id: str,
    event_type: str,   # "search" | "booking" | "cancellation"
    sim_epoch: float,
) -> None:
    sk = _SK.format(r=route)
    bk = _BK.format(r=route)
    rk = _RK.format(r=route)

    lo_5m  = sim_epoch - _W_5M
    lo_15m = sim_epoch - _W_15M
    lo_1h  = sim_epoch - _W_1H

    pipe = r.pipeline(transaction=False)
    if event_type == "search":
        pipe.zadd(sk, {event_id: sim_epoch})
    elif event_type == "booking":
        pipe.zadd(bk, {event_id: sim_epoch})
    pipe.zremrangebyscore(sk, "-inf", lo_1h - 1)
    pipe.zremrangebyscore(bk, "-inf", lo_1h - 1)
    pipe.zcount(sk, lo_5m,  sim_epoch)
    pipe.zcount(bk, lo_15m, sim_epoch)
    pipe.zcount(sk, lo_1h,  sim_epoch)
    pipe.zcount(bk, lo_1h,  sim_epoch)
    results = pipe.execute()

    searches_5m, bookings_15m, searches_1h, bookings_1h = results[-4:]
    look_to_book = float(bookings_1h) / max(1.0, float(searches_1h))

    r.hset(rk, mapping={
        "searches_5m":        str(float(searches_5m)),
        "bookings_15m":       str(float(bookings_15m)),
        "look_to_book_1h":    str(round(look_to_book, 6)),
        "stream_computed_at": "2024-01-17T14:32:01+00:00",
    })


# ─── Load level runner ───────────────────────────────────────────────────────

class _LevelResult(NamedTuple):
    n_threads:    int
    events_total: int
    elapsed_s:    float
    latencies_ms: list[float]   # one entry per event (measurement phase only)

    @property
    def events_per_s(self) -> float:
        return self.events_total / self.elapsed_s if self.elapsed_s else 0.0

    @property
    def p50(self) -> float:
        return float(np.percentile(self.latencies_ms, 50)) if self.latencies_ms else 0.0

    @property
    def p95(self) -> float:
        return float(np.percentile(self.latencies_ms, 95)) if self.latencies_ms else 0.0

    @property
    def p99(self) -> float:
        return float(np.percentile(self.latencies_ms, 99)) if self.latencies_ms else 0.0

    @property
    def p999(self) -> float:
        return float(np.percentile(self.latencies_ms, 99.9)) if self.latencies_ms else 0.0


def _run_level(
    r: redis.Redis,
    n_threads: int,
    warmup_s: float = WARMUP_S,
    measure_s: float = MEASURE_S,
) -> _LevelResult:
    """Warm up, then measure for *measure_s* seconds at *n_threads* concurrency."""

    stop      = threading.Event()
    phase     = threading.Event()   # set → switch from warmup to measure
    lock      = threading.Lock()
    all_lats: list[float] = []
    total_events = 0

    # Event type cycle: 70% search, 30% booking
    _TYPES = ["search"] * 7 + ["booking"] * 3

    def worker(thread_id: int) -> None:
        nonlocal total_events
        local_lats: list[float] = []
        n = 0
        base_epoch = time.time() - _W_1H / 2   # start mid-window so trim fires

        while not stop.is_set():
            route      = ROUTES[n % len(ROUTES)]
            event_type = _TYPES[n % len(_TYPES)]
            event_id   = f"t{thread_id}-e{n}"
            # Advance sim_epoch by 1 s per event so window stays bounded.
            sim_epoch  = base_epoch + n

            measuring = phase.is_set()
            t0 = time.perf_counter()
            _write_event(r, route, event_id, event_type, sim_epoch)
            elapsed_ms = (time.perf_counter() - t0) * 1_000

            if measuring:
                local_lats.append(elapsed_ms)
            n += 1

        with lock:
            all_lats.extend(local_lats)
            total_events += len(local_lats)

    threads = [
        threading.Thread(target=worker, args=(i,), daemon=True)
        for i in range(n_threads)
    ]
    for t in threads:
        t.start()

    # Warmup phase: run but don't record.
    time.sleep(warmup_s)
    phase.set()

    # Measurement phase.
    t_start = time.perf_counter()
    time.sleep(measure_s)
    stop.set()

    for t in threads:
        t.join(timeout=2.0)

    elapsed = time.perf_counter() - t_start
    return _LevelResult(n_threads, total_events, elapsed, all_lats)


# ─── Cleanup bench keys ───────────────────────────────────────────────────────

def _cleanup(r: redis.Redis) -> None:
    keys = r.keys("bench:*")
    if keys:
        r.delete(*keys)


# ─── Output ───────────────────────────────────────────────────────────────────

_W = 80

def _hdr() -> None:
    print("─" * _W)

def _banner(title: str) -> None:
    print("═" * _W)
    print(f"  {title}")
    print("═" * _W)


def _print_results(results: list[_LevelResult], target_ms: float) -> None:
    _hdr()
    print(
        f"  {'threads':>7}  {'ev/s':>8}  {'p50 ms':>7}  "
        f"{'p95 ms':>7}  {'p99 ms':>7}  {'p99.9 ms':>9}  {'events':>8}"
    )
    _hdr()
    for res in results:
        flag = " ← p99 > 5ms" if res.p99 > target_ms else ""
        print(
            f"  {res.n_threads:>7}  {res.events_per_s:>8,.0f}  "
            f"{res.p50:>7.2f}  {res.p95:>7.2f}  {res.p99:>7.2f}  "
            f"{res.p999:>9.2f}  {res.events_total:>8,}{flag}"
        )
    _hdr()


def _print_summary(results: list[_LevelResult], target_ms: float) -> None:
    # Find first level where p99 > target.
    crossover: _LevelResult | None = None
    prev: _LevelResult | None = None
    for res in results:
        if res.p99 > target_ms:
            crossover = res
            break
        prev = res

    print()
    print("  RESULT")
    print()
    if crossover is not None:
        print(f"  p99 first exceeds {target_ms:.0f} ms at "
              f"{crossover.n_threads} concurrent writer(s):")
        print(f"    throughput  : {crossover.events_per_s:,.0f} events/s")
        print(f"    p99 latency : {crossover.p99:.2f} ms")
        if prev is not None:
            print(f"  Last safe level ({prev.n_threads} threads): "
                  f"{prev.events_per_s:,.0f} events/s  p99={prev.p99:.2f} ms")
    else:
        last = results[-1]
        print(f"  p99 stayed below {target_ms:.0f} ms at all tested concurrency levels.")
        print(f"  Highest tested: {last.n_threads} threads, "
              f"{last.events_per_s:,.0f} events/s, p99={last.p99:.2f} ms")
    print()


# ─── Entry point ─────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host",     default=os.getenv("REDIS_HOST", "localhost"))
    ap.add_argument("--port",     type=int, default=int(os.getenv("REDIS_PORT", "6379")))
    ap.add_argument("--password", default=os.getenv("REDIS_PASSWORD", "changeme"))
    ap.add_argument("--threads",  type=int, nargs="+", default=DEFAULT_LEVELS,
                    metavar="N", help="Concurrency levels to test")
    ap.add_argument("--measure",  type=float, default=MEASURE_S,
                    help=f"Measurement seconds per level (default {MEASURE_S})")
    ap.add_argument("--warmup",   type=float, default=WARMUP_S,
                    help=f"Warmup seconds per level (default {WARMUP_S})")
    args = ap.parse_args()


    print(f"\nConnecting to Redis {args.host}:{args.port} …", end=" ", flush=True)
    try:
        r = _connect(args.host, args.port, args.password)
    except redis.RedisError as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    info = r.info("server")
    print(f"OK  (Redis {info['redis_version']})")

    _cleanup(r)

    _banner(
        f"Redis sliding-window bench  "
        f"— {args.host}:{args.port}  "
        f"— {len(ROUTES)} routes  "
        f"— {MEASURE_S:.0f}s per level"
    )
    print()
    print("  Operations per event (mirrors update_demand_features):")
    print("    RT1  pipeline( ZADD? + ZREMRANGEBYSCORE×2 + ZCOUNT×4 ).execute()")
    print("    RT2  r.hset(route_key, mapping={4 fields})")
    print()
    print(f"  Target SLA  : p99 < {P99_TARGET_MS:.0f} ms")
    print(f"  Warmup      : {WARMUP_S:.0f}s per level (discarded)")
    print(f"  Measurement : {MEASURE_S:.0f}s per level")
    print()

    results: list[_LevelResult] = []
    for n in args.threads:
        print(f"  {n:2d} thread(s) …", end=" ", flush=True)
        res = _run_level(r, n, warmup_s=args.warmup, measure_s=args.measure)
        flag = " ← p99 EXCEEDS 5ms" if res.p99 > P99_TARGET_MS else ""
        print(
            f"{res.events_per_s:7,.0f} ev/s  "
            f"p50={res.p50:.2f}ms  p99={res.p99:.2f}ms{flag}"
        )
        results.append(res)

    print()
    _print_results(results, P99_TARGET_MS)
    _print_summary(results, P99_TARGET_MS)

    _cleanup(r)


if __name__ == "__main__":
    main()
