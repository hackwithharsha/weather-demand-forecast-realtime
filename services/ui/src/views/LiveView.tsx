import { useMemo, useState, useEffect, useRef } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceLine,
} from 'recharts';
import { getCities, getHistory, predict } from '../api';
import { useLiveStream } from '../hooks/useLiveStream';
import Card from '../components/Card';
import type { LiveChartPoint, PredictResponse } from '../types';

// ── Constants ────────────────────────────────────────────────────────────────

const SIX_H_MS  = 6 * 3600 * 1000;
const HOUR_MS   = 3600 * 1000;

// ── Tick / label helpers ─────────────────────────────────────────────────────

/** Format a Unix-ms timestamp for the x-axis tick labels. */
function fmtAxisTick(ts: number): string {
  const d  = new Date(ts);
  const hh = d.getHours().toString().padStart(2, '0');
  const mm = d.getMinutes().toString().padStart(2, '0');
  // At midnight show the day abbreviation instead of "00:00"
  if (hh === '00' && mm === '00') {
    return d.toLocaleDateString('en', { weekday: 'short', month: 'short', day: 'numeric' });
  }
  return `${hh}:${mm}`;
}

/** Full date+time string for tooltip header and metadata row. */
function fmtFull(isoOrMs: string | number): string {
  const d = typeof isoOrMs === 'number' ? new Date(isoOrMs) : new Date(isoOrMs);
  return d.toLocaleString('en', {
    weekday: 'short', month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit', hour12: false,
  });
}

/** "3 min ago", "just now", etc. */
function relAge(isoString: string): string {
  const diff = (Date.now() - new Date(isoString).getTime()) / 1000;
  if (diff < 10)   return 'just now';
  if (diff < 60)   return `${Math.floor(diff)}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  return `${Math.floor(diff / 3600)}h ago`;
}

// ── Pre-compute x-axis ticks every 6 h, aligned to clock boundaries ──────────

function buildTicks(data: LiveChartPoint[]): number[] {
  if (data.length === 0) return [];
  const start = data[0].ts;
  const end   = data[data.length - 1].ts;
  const first = Math.ceil(start / SIX_H_MS) * SIX_H_MS;
  const ticks: number[] = [];
  for (let t = first; t <= end; t += SIX_H_MS) ticks.push(t);
  return ticks;
}

// ── WebSocket status badge ────────────────────────────────────────────────────

type WsBadgeProps = { status: 'connecting' | 'connected' | 'disconnected'; attempts: number };

function WsBadge({ status, attempts }: WsBadgeProps) {
  const reconnecting = status !== 'connected' && attempts > 0;

  const dot = status === 'connected'
    ? 'bg-emerald-400 animate-pulse'
    : reconnecting
      ? 'bg-amber-400 animate-pulse'
      : 'bg-gray-500';

  const text = status === 'connected'
    ? 'live'
    : reconnecting
      ? `reconnecting… (attempt ${attempts})`
      : 'connecting…';

  const ring = status === 'connected'
    ? 'border-emerald-800 bg-emerald-900/50 text-emerald-400'
    : reconnecting
      ? 'border-amber-800 bg-amber-900/30 text-amber-400'
      : 'border-gray-700 bg-gray-800 text-gray-500';

  return (
    <span className={`flex items-center gap-1.5 text-xs font-medium px-2 py-1 rounded-full border ${ring}`}>
      <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${dot}`} />
      {text}
    </span>
  );
}

// ── Main view ─────────────────────────────────────────────────────────────────

export default function LiveView() {
  const [city, setCity] = useState('london');

  // liveForecast supersedes the React Query result whenever the WS pushes
  // a fresher prediction for the selected city.
  const [liveForecast, setLiveForecast] = useState<PredictResponse | null>(null);

  // Track when the last live update arrived so the UI can say "updated 5s ago".
  const lastLiveTsRef = useRef<Date | null>(null);
  const [flashSeq, setFlashSeq] = useState(0); // increment to trigger re-render after WS update

  // ── Data queries ─────────────────────────────────────────────────────────

  const { data: cities } = useQuery({
    queryKey: ['cities'],
    queryFn:  getCities,
    staleTime: 60_000,
  });

  const { data: historyData, isFetching: historyFetching } = useQuery({
    queryKey:       ['history', city],
    queryFn:        () => getHistory(city, 48),
    enabled:        !!city,
    refetchInterval: 60_000,
  });

  const {
    data:     forecastData,
    isFetching: forecastFetching,
    isError:    forecastError,
  } = useQuery({
    queryKey:       ['predict', city],
    queryFn:        () => predict(city, 24),
    enabled:        !!city,
    staleTime:      60_000,
    refetchInterval: 120_000,
  });

  // ── WebSocket ─────────────────────────────────────────────────────────────

  const { lastEvent, status: wsStatus, attempts: wsAttempts } = useLiveStream();

  // Apply inbound WS events for the selected city.
  useEffect(() => {
    if (!lastEvent) return;
    if (lastEvent.data.city !== city) return; // ignore other cities
    setLiveForecast(lastEvent.data);
    lastLiveTsRef.current = new Date();
    setFlashSeq((n) => n + 1); // trigger metadata re-render
  }, [lastEvent, city]);

  // Drop stale live forecast whenever the city selector changes.
  useEffect(() => {
    setLiveForecast(null);
    lastLiveTsRef.current = null;
  }, [city]);

  // ── Derived display values ────────────────────────────────────────────────

  // WS data wins over React Query; fall back gracefully.
  const forecast = liveForecast ?? forecastData;
  const isLoading = historyFetching || forecastFetching;

  // ── Chart data assembly ───────────────────────────────────────────────────
  //
  // Use a Map<unixMs, LiveChartPoint> so we can merge actuals and predictions
  // in O(1) without scanning the array for every forecast step.

  const chartData = useMemo<LiveChartPoint[]>(() => {
    const map = new Map<number, LiveChartPoint>();

    // Actuals (API returns newest-first → sort ascending)
    if (historyData) {
      const sorted = [...historyData.history].sort((a, b) =>
        a.hour_ts < b.hour_ts ? -1 : 1,
      );
      for (const h of sorted) {
        const ts = new Date(h.hour_ts).getTime();
        map.set(ts, { ts, actual: h.total_demand });
      }
    }

    // Predictions — may overlap the last actual hour (bridge point)
    if (forecast) {
      for (const f of forecast.forecasts) {
        const ts = new Date(f.target_hour).getTime();
        const existing = map.get(ts);
        if (existing) {
          existing.predicted = f.predicted_demand;
        } else {
          map.set(ts, { ts, predicted: f.predicted_demand });
        }
      }
    }

    return Array.from(map.values()).sort((a, b) => a.ts - b.ts);
  }, [historyData, forecast]);

  const ticks = useMemo(() => buildTicks(chartData), [chartData]);

  // Snap the "now" marker to the nearest completed-hour boundary so the
  // dashed line always falls on a real data point when the chart has one.
  const nowMs = Math.floor(Date.now() / HOUR_MS) * HOUR_MS;

  const featureStatus = forecast?.feature_status;
  const asOf          = forecast?.as_of;
  const modelVer      = forecast?.model_version;
  const cityLabel     = forecast?.city ?? city;
  const lastLiveTs    = lastLiveTsRef.current;

  // Suppress flash-related dep in the above block; flashSeq just forces a
  // re-render so relAge() shows a fresh "just now".
  void flashSeq;

  return (
    <div className="space-y-4">

      {/* ── Header row ───────────────────────────────────────────────────── */}
      <div className="flex items-center gap-3 flex-wrap">
        <h1 className="text-lg font-semibold text-gray-100">Live Forecast</h1>

        <select
          value={city}
          onChange={(e) => setCity(e.target.value)}
          className="bg-gray-800 border border-gray-700 text-gray-100 text-sm rounded-lg
                     px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          {(cities ?? [city]).map((c) => (
            <option key={c} value={c}>
              {c.replace(/_/g, ' ')}
            </option>
          ))}
        </select>

        <WsBadge status={wsStatus} attempts={wsAttempts} />

        {isLoading && (
          <span className="text-xs text-gray-500 animate-pulse">loading…</span>
        )}
      </div>

      {/* ── Chart ────────────────────────────────────────────────────────── */}
      <Card>
        {forecastError && (
          <p className="mb-3 text-sm text-amber-400">
            Model unavailable — showing historical actuals only.
            Predictions will appear once the Production model is loaded.
          </p>
        )}

        {/* Dim the chart while new data is fetching for the selected city */}
        <div className={isLoading ? 'opacity-50 transition-opacity' : 'transition-opacity'}>
          <ResponsiveContainer width="100%" height={360}>
            <LineChart data={chartData} margin={{ top: 8, right: 20, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />

              {/* Numeric time axis — ReferenceLine x={nowMs} lands precisely */}
              <XAxis
                type="number"
                dataKey="ts"
                scale="time"
                domain={['dataMin', 'dataMax']}
                ticks={ticks}
                tickFormatter={fmtAxisTick}
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                minTickGap={40}
              />

              <YAxis
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                width={54}
                tickFormatter={(v: number) => v.toLocaleString()}
              />

              <Tooltip
                contentStyle={{
                  background: '#111827',
                  border: '1px solid #1f2937',
                  borderRadius: 8,
                }}
                labelStyle={{ color: '#e5e7eb', marginBottom: 4 }}
                labelFormatter={(ts: number) => fmtFull(ts)}
                formatter={(v: number, name: string) => [
                  v.toLocaleString(undefined, { maximumFractionDigits: 1 }),
                  name === 'actual' ? 'Actual demand' : 'Predicted demand',
                ]}
              />

              <Legend
                wrapperStyle={{ paddingTop: 12 }}
                formatter={(value) =>
                  value === 'actual' ? 'Actual' : 'Predicted'
                }
              />

              {/* "now" reference line — numeric domain → exact position */}
              <ReferenceLine
                x={nowMs}
                stroke="#374151"
                strokeDasharray="4 4"
                label={{
                  value: 'now',
                  position: 'insideTopRight',
                  fill: '#6b7280',
                  fontSize: 10,
                }}
              />

              {/* Solid blue line for historical actuals */}
              <Line
                type="monotone"
                dataKey="actual"
                name="actual"
                stroke="#60a5fa"
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
              />

              {/* Dashed green line for predicted values */}
              <Line
                type="monotone"
                dataKey="predicted"
                name="predicted"
                stroke="#34d399"
                strokeWidth={2}
                strokeDasharray="5 3"
                dot={false}
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </Card>

      {/* ── Metadata row ──────────────────────────────────────────────────── */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">

        <Card>
          <p className="text-xs text-gray-400 mb-1">City</p>
          <p className="text-sm font-medium capitalize">
            {cityLabel.replace(/_/g, ' ')}
          </p>
        </Card>

        <Card>
          <p className="text-xs text-gray-400 mb-1">Model version</p>
          <p className="text-sm font-medium">
            {modelVer != null ? `v${modelVer}` : '—'}
          </p>
        </Card>

        <Card>
          <p className="text-xs text-gray-400 mb-1">Feature store</p>
          {featureStatus ? (
            <p className="text-sm font-medium">
              {featureStatus.hash_present
                ? `Redis hit${featureStatus.age_s != null ? ` · ${Math.round(featureStatus.age_s)}s old` : ''}`
                : 'Redis miss — Postgres fallback'}
            </p>
          ) : (
            <p className="text-sm text-gray-500">—</p>
          )}
        </Card>

        <Card>
          <p className="text-xs text-gray-400 mb-1">
            {lastLiveTs ? 'Updated (live)' : 'Computed at'}
          </p>
          <p className="text-sm font-medium">
            {lastLiveTs
              ? relAge(lastLiveTs.toISOString())
              : asOf
                ? relAge(asOf)
                : '—'}
          </p>
          {asOf && (
            <p className="text-xs text-gray-500 mt-0.5">{fmtFull(asOf)}</p>
          )}
        </Card>

      </div>

      {/* ── Feature warnings ──────────────────────────────────────────────── */}
      {featureStatus &&
        (featureStatus.missing.length > 0 || featureStatus.degraded.length > 0) && (
          <Card>
            <p className="text-xs font-semibold uppercase tracking-wider text-amber-400 mb-2">
              Feature warnings
            </p>
            {featureStatus.missing.length > 0 && (
              <p className="text-sm text-amber-300">
                Missing:{' '}
                <span className="font-mono">{featureStatus.missing.join(', ')}</span>
              </p>
            )}
            {featureStatus.degraded.length > 0 && (
              <p className="text-sm text-amber-300 mt-1">
                Degraded:{' '}
                <span className="font-mono">{featureStatus.degraded.join(', ')}</span>
              </p>
            )}
          </Card>
        )}

    </div>
  );
}
