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
import { promQuery, promQueryRange, singleValue } from '../api';
import Card from '../components/Card';
import type { DriftChartPoint, PromInstant, PromSeries } from '../types';

const DRIFT_THRESHOLD = 0.2;

const FEATURE_COLORS: Record<string, string> = {
  temperature_c:    '#60a5fa',
  precip_mm:        '#34d399',
  humidity_pct:     '#fb923c',
  event_count:      '#a78bfa',
  demand_lag_1h:    '#f472b6',
  demand_lag_24h:   '#facc15',
  demand_lag_168h:  '#22d3ee',
  demand_roll_3h:   '#4ade80',
  demand_roll_24h:  '#f87171',
  is_holiday:       '#c084fc',
};

function colorFor(feature: string): string {
  return FEATURE_COLORS[feature] ?? '#94a3b8';
}

function fmtTime(unix: number): string {
  return new Date(unix * 1000).toLocaleTimeString('en', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
}

function buildTimeSeriesData(series: PromSeries[]): { points: DriftChartPoint[]; features: string[] } {
  const tsMap = new Map<number, DriftChartPoint>();
  const features = new Set<string>();

  for (const s of series) {
    const feat = s.metric.feature;
    if (!feat) continue;
    features.add(feat);
    for (const [ts, val] of s.values) {
      if (!tsMap.has(ts)) {
        tsMap.set(ts, { t: fmtTime(ts), ts });
      }
      tsMap.get(ts)![feat] = parseFloat(val);
    }
  }

  const points = Array.from(tsMap.values()).sort((a, b) => a.ts - b.ts);
  return { points, features: Array.from(features).sort() };
}

export default function DriftView() {
  const now = Math.floor(Date.now() / 1000);
  const opts = { refetchInterval: 60_000, staleTime: 30_000 };

  // Current drift scores (instant)
  const currentQ = useQuery({
    queryKey: ['drift', 'current'],
    queryFn: () => promQuery('drift_score'),
    ...opts,
  });

  // Drift detected flags
  const detectedQ = useQuery({
    queryKey: ['drift', 'detected'],
    queryFn: () => promQuery('drift_detected'),
    ...opts,
  });

  // Job metadata
  const lastRunQ = useQuery({
    queryKey: ['drift', 'lastRun'],
    queryFn: () => promQuery('drift_job_last_run_timestamp'),
    ...opts,
  });
  const refCountQ = useQuery({
    queryKey: ['drift', 'refCount'],
    queryFn: () => promQuery('drift_job_reference_count'),
    ...opts,
  });
  const curCountQ = useQuery({
    queryKey: ['drift', 'curCount'],
    queryFn: () => promQuery('drift_job_current_count'),
    ...opts,
  });

  // 6-hour range query for trend chart
  const rangeQ = useQuery({
    queryKey: ['drift', 'range'],
    queryFn: () => promQueryRange('drift_score', now - 6 * 3600, now, '5m'),
    ...opts,
  });

  // Build current scores bar chart data
  const currentScores = useMemo(() => {
    const data = (currentQ.data ?? []) as PromInstant[];
    return data
      .map((r) => ({
        feature: r.metric.feature ?? '?',
        score: parseFloat(r.value[1]),
      }))
      .sort((a, b) => b.score - a.score);
  }, [currentQ.data]);

  // Build detected map for quick lookup
  const detectedMap = useMemo(() => {
    const map = new Map<string, boolean>();
    for (const r of (detectedQ.data ?? []) as PromInstant[]) {
      map.set(r.metric.feature ?? '', parseFloat(r.value[1]) >= 0.5);
    }
    return map;
  }, [detectedQ.data]);

  // Build time-series data
  const { points: trendPoints, features: trendFeatures } = useMemo(
    () => buildTimeSeriesData((rangeQ.data ?? []) as PromSeries[]),
    [rangeQ.data],
  );

  const lastRunTs  = singleValue((lastRunQ.data  ?? []) as PromInstant[]);
  const refCount   = singleValue((refCountQ.data ?? []) as PromInstant[]);
  const curCount   = singleValue((curCountQ.data ?? []) as PromInstant[]);

  const driftingCount = currentScores.filter((s) => s.score > DRIFT_THRESHOLD).length;
  const promUnavailable = currentQ.isError;

  return (
    <div className="space-y-4">
      <h1 className="text-lg font-semibold text-gray-100">Feature Drift</h1>

      {promUnavailable && (
        <div className="bg-amber-900/30 border border-amber-800 rounded-lg px-4 py-3 text-amber-300 text-sm">
          Prometheus is unreachable. Start the obs profile with{' '}
          <code className="font-mono text-amber-200">make up-obs</code>.
          Drift data requires the worker service running a drift check.
        </div>
      )}

      {/* Job metadata row */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-4">
          <p className="text-xs text-gray-400 mb-1">Features drifting</p>
          <p className={`text-3xl font-bold tabular-nums ${driftingCount > 0 ? 'text-amber-400' : 'text-emerald-400'}`}>
            {currentScores.length > 0 ? driftingCount : '—'}
          </p>
          <p className="text-xs text-gray-500 mt-0.5">threshold {DRIFT_THRESHOLD}</p>
        </div>
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-4">
          <p className="text-xs text-gray-400 mb-1">Last check</p>
          <p className="text-sm font-medium text-gray-100">
            {lastRunTs
              ? new Date(lastRunTs * 1000).toLocaleTimeString('en', { hour: '2-digit', minute: '2-digit', hour12: false })
              : '—'}
          </p>
          <p className="text-xs text-gray-500 mt-0.5">
            {lastRunTs ? new Date(lastRunTs * 1000).toLocaleDateString() : 'no run yet'}
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

      {/* Current scores bar chart */}
      {currentScores.length > 0 && (
        <Card title="Current drift scores">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={currentScores} layout="vertical" margin={{ left: 20, right: 40, top: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" horizontal={false} />
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
              <ReferenceLine x={DRIFT_THRESHOLD} stroke="#f59e0b" strokeDasharray="4 4" />
              <Tooltip
                cursor={{ fill: '#1f2937' }}
                contentStyle={{ background: '#111827', border: '1px solid #1f2937', borderRadius: 8 }}
                formatter={(v: number) => [v.toFixed(4), 'Drift score']}
              />
              <Bar dataKey="score" radius={[0, 3, 3, 0]}>
                {currentScores.map((entry) => {
                  const isDrifting = detectedMap.get(entry.feature) || entry.score > DRIFT_THRESHOLD;
                  return (
                    <Cell
                      key={entry.feature}
                      fill={isDrifting ? '#f59e0b' : colorFor(entry.feature)}
                    />
                  );
                })}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          <p className="text-xs text-gray-500 mt-2 text-center">
            Amber bars exceed the {DRIFT_THRESHOLD} threshold · updated every 30 s
          </p>
        </Card>
      )}

      {/* 6-hour trend */}
      {trendPoints.length > 0 && (
        <Card title="Drift score over 6 h">
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={trendPoints} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
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
              <ReferenceLine y={DRIFT_THRESHOLD} stroke="#f59e0b" strokeDasharray="4 4" />
              <Tooltip
                contentStyle={{ background: '#111827', border: '1px solid #1f2937', borderRadius: 8 }}
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

      {/* Detected flags table */}
      {currentScores.length > 0 && (
        <Card title="Feature status">
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-2">
            {currentScores.map(({ feature, score }) => {
              const drifting = detectedMap.get(feature) || score > DRIFT_THRESHOLD;
              return (
                <div
                  key={feature}
                  className={[
                    'rounded-lg px-3 py-2 border text-sm',
                    drifting
                      ? 'border-amber-800 bg-amber-900/20 text-amber-300'
                      : 'border-gray-800 bg-gray-800/50 text-gray-300',
                  ].join(' ')}
                >
                  <p className="font-mono text-xs truncate mb-1">{feature}</p>
                  <p className="font-bold tabular-nums">{score.toFixed(3)}</p>
                  {drifting && <p className="text-xs mt-0.5">DRIFT</p>}
                </div>
              );
            })}
          </div>
        </Card>
      )}
    </div>
  );
}
