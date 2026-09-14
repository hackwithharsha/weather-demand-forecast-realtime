import { useMemo } from 'react';
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
  BarChart,
  Bar,
  Cell,
} from 'recharts';
import { getDriftScores, getDriftHistory } from '../api';
import Card from '../components/Card';
import type { DriftChartPoint } from '../types';

// ── Constants ─────────────────────────────────────────────────────────────────

const DRIFT_THRESHOLD = 0.2;

const FEATURE_COLORS: Record<string, string> = {
  temperature_c:   '#60a5fa',
  precip_mm:       '#34d399',
  humidity_pct:    '#fb923c',
  event_count:     '#a78bfa',
  demand_lag_1h:   '#f472b6',
  demand_lag_24h:  '#facc15',
  demand_lag_168h: '#22d3ee',
  demand_roll_3h:  '#4ade80',
  demand_roll_24h: '#f87171',
  is_holiday:      '#c084fc',
};

function colorFor(feature: string): string {
  return FEATURE_COLORS[feature] ?? '#94a3b8';
}

/** Format a Unix-second timestamp as HH:MM. */
function fmtTime(unixSec: number): string {
  return new Date(unixSec * 1000).toLocaleTimeString('en', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
}

// ── View ──────────────────────────────────────────────────────────────────────

export default function DriftView() {
  const opts = { refetchInterval: 60_000, staleTime: 30_000 };

  const scoresQ = useQuery({
    queryKey: ['drift', 'scores'],
    queryFn:  getDriftScores,
    ...opts,
  });

  const historyQ = useQuery({
    queryKey: ['drift', 'history'],
    queryFn:  getDriftHistory,
    ...opts,
  });

  // ── Derived values ──────────────────────────────────────────────────────

  const scores      = scoresQ.data?.scores       ?? [];
  const lastRunTs   = scoresQ.data?.last_run_ts  ?? null;
  const refCount    = scoresQ.data?.reference_count ?? null;
  const curCount    = scoresQ.data?.current_count   ?? null;
  const promAvail   = scoresQ.data?.prometheus_available ?? true;

  const driftingCount = scores.filter(
    (s) => s.detected || s.score > DRIFT_THRESHOLD,
  ).length;

  // Build the recharts-friendly trend data from the history API response
  const { points: trendPoints, features: trendFeatures } = useMemo<{
    points: DriftChartPoint[];
    features: string[];
  }>(() => {
    const series = historyQ.data?.series ?? [];
    const tsMap = new Map<number, DriftChartPoint>();
    const featureSet = new Set<string>();

    for (const s of series) {
      featureSet.add(s.feature);
      for (const p of s.points) {
        if (!tsMap.has(p.ts)) {
          tsMap.set(p.ts, { t: fmtTime(p.ts), ts: p.ts });
        }
        tsMap.get(p.ts)![s.feature] = p.score;
      }
    }

    const points = Array.from(tsMap.values()).sort((a, b) => a.ts - b.ts);
    return { points, features: Array.from(featureSet).sort() };
  }, [historyQ.data]);

  // ── Render ──────────────────────────────────────────────────────────────

  return (
    <div className="space-y-4">
      <h1 className="text-lg font-semibold text-gray-100">Feature Drift</h1>

      {!promAvail && !scoresQ.isFetching && (
        <div className="bg-amber-900/30 border border-amber-800 rounded-lg px-4 py-3 text-amber-300 text-sm">
          Prometheus is unreachable. Start the obs profile with{' '}
          <code className="font-mono text-amber-200">make up-obs</code>.
          Drift data requires the worker service to have run a drift check.
        </div>
      )}

      {/* ── Job metadata row ──────────────────────────────────────────────── */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-4">
          <p className="text-xs text-gray-400 mb-1">Features drifting</p>
          <p
            className={`text-3xl font-bold tabular-nums ${
              driftingCount > 0 ? 'text-amber-400' : 'text-emerald-400'
            }`}
          >
            {scores.length > 0 ? driftingCount : '—'}
          </p>
          <p className="text-xs text-gray-500 mt-0.5">threshold {DRIFT_THRESHOLD}</p>
        </div>

        <div className="bg-gray-900 border border-gray-800 rounded-xl p-4">
          <p className="text-xs text-gray-400 mb-1">Last check</p>
          <p className="text-sm font-medium text-gray-100">
            {lastRunTs
              ? new Date(lastRunTs * 1000).toLocaleTimeString('en', {
                  hour: '2-digit',
                  minute: '2-digit',
                  hour12: false,
                })
              : '—'}
          </p>
          <p className="text-xs text-gray-500 mt-0.5">
            {lastRunTs
              ? new Date(lastRunTs * 1000).toLocaleDateString()
              : 'no run yet'}
          </p>
        </div>

        <div className="bg-gray-900 border border-gray-800 rounded-xl p-4">
          <p className="text-xs text-gray-400 mb-1">Reference rows</p>
          <p className="text-sm font-medium text-gray-100 tabular-nums">
            {refCount != null ? Math.round(refCount).toLocaleString() : '—'}
          </p>
        </div>

        <div className="bg-gray-900 border border-gray-800 rounded-xl p-4">
          <p className="text-xs text-gray-400 mb-1">Current window rows</p>
          <p className="text-sm font-medium text-gray-100 tabular-nums">
            {curCount != null ? Math.round(curCount).toLocaleString() : '—'}
          </p>
        </div>
      </div>

      {/* ── Current scores bar chart ──────────────────────────────────────── */}
      {scores.length > 0 && (
        <Card title="Current drift scores">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart
              data={scores.map((s) => ({ feature: s.feature, score: s.score }))}
              layout="vertical"
              margin={{ left: 20, right: 40, top: 0, bottom: 0 }}
            >
              <CartesianGrid
                strokeDasharray="3 3"
                stroke="#1f2937"
                horizontal={false}
              />
              <XAxis
                type="number"
                domain={[0, 1]}
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                tickFormatter={(v: number) => v.toFixed(1)}
              />
              <YAxis
                type="category"
                dataKey="feature"
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                width={110}
              />
              <ReferenceLine
                x={DRIFT_THRESHOLD}
                stroke="#f59e0b"
                strokeDasharray="4 4"
              />
              <Tooltip
                cursor={{ fill: '#1f2937' }}
                contentStyle={{
                  background: '#111827',
                  border: '1px solid #1f2937',
                  borderRadius: 8,
                }}
                formatter={(v: number) => [v.toFixed(4), 'Drift score']}
              />
              <Bar dataKey="score" radius={[0, 3, 3, 0]}>
                {scores.map((s) => (
                  <Cell
                    key={s.feature}
                    fill={
                      s.detected || s.score > DRIFT_THRESHOLD
                        ? '#f59e0b'
                        : colorFor(s.feature)
                    }
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          <p className="text-xs text-gray-500 mt-2 text-center">
            Amber bars exceed the {DRIFT_THRESHOLD} threshold · updated every 60 s
          </p>
        </Card>
      )}

      {/* ── 6-hour trend ──────────────────────────────────────────────────── */}
      {trendPoints.length > 0 && (
        <Card title="Drift score over 6 h">
          <ResponsiveContainer width="100%" height={300}>
            <LineChart
              data={trendPoints}
              margin={{ top: 4, right: 16, left: 0, bottom: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis
                dataKey="t"
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                interval="preserveStartEnd"
                minTickGap={60}
              />
              <YAxis
                domain={[0, 1]}
                tick={{ fill: '#9ca3af', fontSize: 11 }}
                width={42}
                tickFormatter={(v: number) => v.toFixed(1)}
              />
              <ReferenceLine
                y={DRIFT_THRESHOLD}
                stroke="#f59e0b"
                strokeDasharray="4 4"
              />
              <Tooltip
                contentStyle={{
                  background: '#111827',
                  border: '1px solid #1f2937',
                  borderRadius: 8,
                }}
                formatter={(v: number, name: string) => [v.toFixed(4), name]}
              />
              <Legend wrapperStyle={{ paddingTop: 12 }} />
              {trendFeatures.map((feat) => (
                <Line
                  key={feat}
                  type="monotone"
                  dataKey={feat}
                  stroke={colorFor(feat)}
                  strokeWidth={1.5}
                  dot={false}
                  isAnimationActive={false}
                  connectNulls
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
          <p className="text-xs text-gray-500 mt-2 text-center">
            Dashed amber line = drift threshold ({DRIFT_THRESHOLD}) · 5-min resolution
          </p>
        </Card>
      )}

      {/* ── Feature status badges ──────────────────────────────────────────── */}
      {scores.length > 0 && (
        <Card title="Feature status">
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-2">
            {scores.map((s) => (
              <div
                key={s.feature}
                className={[
                  'rounded-lg px-3 py-2 border text-sm',
                  s.detected || s.score > DRIFT_THRESHOLD
                    ? 'border-amber-800 bg-amber-900/20 text-amber-300'
                    : 'border-gray-800 bg-gray-800/50 text-gray-300',
                ].join(' ')}
              >
                <p className="font-mono text-xs truncate mb-1">{s.feature}</p>
                <p className="font-bold tabular-nums">{s.score.toFixed(3)}</p>
                {(s.detected || s.score > DRIFT_THRESHOLD) && (
                  <p className="text-xs mt-0.5">DRIFT</p>
                )}
              </div>
            ))}
          </div>
        </Card>
      )}
    </div>
  );
}
