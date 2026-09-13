import { useMemo, useState, useEffect } from 'react';
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
import Badge from '../components/Badge';
import type { LiveChartPoint, PredictResponse } from '../types';

const NOW_LABEL = '▶ now';

function fmtHour(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleTimeString('en', { hour: '2-digit', minute: '2-digit', hour12: false });
}

function fmtFull(iso: string): string {
  const d = new Date(iso);
  return (
    d.toLocaleDateString('en', { weekday: 'short', month: 'short', day: 'numeric' }) +
    ' ' +
    d.toLocaleTimeString('en', { hour: '2-digit', minute: '2-digit', hour12: false })
  );
}

export default function LiveView() {
  const [city, setCity] = useState('london');
  const [liveForecast, setLiveForecast] = useState<PredictResponse | null>(null);

  const { data: cities } = useQuery({
    queryKey: ['cities'],
    queryFn: getCities,
    staleTime: 60_000,
  });

  const { data: historyData } = useQuery({
    queryKey: ['history', city],
    queryFn: () => getHistory(city, 48),
    enabled: !!city,
    refetchInterval: 60_000,
  });

  const { data: forecastData, isError: forecastError } = useQuery({
    queryKey: ['predict', city],
    queryFn: () => predict(city, 24),
    enabled: !!city,
    staleTime: 60_000,
    refetchInterval: 120_000,
  });

  // WebSocket: update live forecast when data arrives for the selected city
  const { lastEvent, connected } = useLiveStream();
  useEffect(() => {
    if (lastEvent && lastEvent.data.city === city) {
      setLiveForecast(lastEvent.data);
    }
  }, [lastEvent, city]);

  // Reset live forecast when city changes
  useEffect(() => {
    setLiveForecast(null);
  }, [city]);

  const forecast = liveForecast ?? forecastData;

  // Build combined chart data (actual history + predicted future)
  const chartData = useMemo<LiveChartPoint[]>(() => {
    const points: LiveChartPoint[] = [];

    if (historyData) {
      // API returns newest-first; reverse to get oldest-first
      const sorted = [...historyData.history].sort((a, b) =>
        a.hour_ts.localeCompare(b.hour_ts),
      );
      for (const h of sorted) {
        points.push({ t: h.hour_ts, label: fmtHour(h.hour_ts), actual: h.total_demand });
      }
    }

    if (forecast) {
      for (const f of forecast.forecasts) {
        const existing = points.find((p) => p.t === f.target_hour);
        if (existing) {
          existing.predicted = f.predicted_demand;
        } else {
          points.push({
            t: f.target_hour,
            label: fmtHour(f.target_hour),
            predicted: f.predicted_demand,
          });
        }
      }
    }

    return points.sort((a, b) => a.t.localeCompare(b.t));
  }, [historyData, forecast]);

  // Reference line at current hour boundary
  const nowIso = new Date(
    Math.floor(Date.now() / 3_600_000) * 3_600_000,
  ).toISOString();
  const nowLabel = chartData.find((p) => p.t >= nowIso)?.label ?? '';

  const cityLabel = forecast?.city ?? city;
  const modelVer = forecast?.model_version;
  const asOf = forecast?.as_of;
  const featureStatus = forecast?.feature_status;

  return (
    <div className="space-y-4">
      {/* Header row */}
      <div className="flex items-center gap-4 flex-wrap">
        <h1 className="text-lg font-semibold text-gray-100">Live Forecast</h1>

        {/* City selector */}
        <select
          value={city}
          onChange={(e) => setCity(e.target.value)}
          className="bg-gray-800 border border-gray-700 text-gray-100 text-sm rounded-lg px-3 py-1.5
                     focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          {(cities ?? [city]).map((c) => (
            <option key={c} value={c}>
              {c.replace(/_/g, ' ')}
            </option>
          ))}
        </select>

        {/* WebSocket badge */}
        <span
          className={[
            'flex items-center gap-1.5 text-xs font-medium px-2 py-1 rounded-full',
            connected
              ? 'bg-emerald-900/50 text-emerald-400 border border-emerald-800'
              : 'bg-gray-800 text-gray-500 border border-gray-700',
          ].join(' ')}
        >
          <span
            className={`w-1.5 h-1.5 rounded-full ${connected ? 'bg-emerald-400 animate-pulse' : 'bg-gray-500'}`}
          />
          {connected ? 'live' : 'connecting…'}
        </span>
      </div>

      {/* Main chart */}
      <Card>
        {forecastError && (
          <p className="text-amber-400 text-sm mb-3">
            Model unavailable — historical actuals only.
          </p>
        )}
        <ResponsiveContainer width="100%" height={360}>
          <LineChart data={chartData} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
            <XAxis
              dataKey="label"
              tick={{ fill: '#9ca3af', fontSize: 11 }}
              interval="preserveStartEnd"
              minTickGap={60}
            />
            <YAxis
              tick={{ fill: '#9ca3af', fontSize: 11 }}
              width={52}
              tickFormatter={(v) => v.toLocaleString()}
            />
            <Tooltip
              contentStyle={{ background: '#111827', border: '1px solid #1f2937', borderRadius: 8 }}
              labelStyle={{ color: '#e5e7eb' }}
              formatter={(v: number, name: string) => [
                v.toLocaleString(undefined, { maximumFractionDigits: 1 }),
                name === 'actual' ? 'Actual demand' : 'Predicted demand',
              ]}
            />
            <Legend wrapperStyle={{ paddingTop: 12 }} />
            {nowLabel && (
              <ReferenceLine
                x={nowLabel}
                stroke="#6b7280"
                strokeDasharray="4 4"
                label={{ value: NOW_LABEL, position: 'insideTopRight', fill: '#6b7280', fontSize: 10 }}
              />
            )}
            <Line
              type="monotone"
              dataKey="actual"
              name="Actual"
              stroke="#60a5fa"
              strokeWidth={2}
              dot={false}
              connectNulls
            />
            <Line
              type="monotone"
              dataKey="predicted"
              name="Predicted"
              stroke="#34d399"
              strokeWidth={2}
              strokeDasharray="5 3"
              dot={false}
              connectNulls
            />
          </LineChart>
        </ResponsiveContainer>
      </Card>

      {/* Metadata row */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <Card>
          <p className="text-xs text-gray-400 mb-1">City</p>
          <p className="text-sm font-medium capitalize">{cityLabel.replace(/_/g, ' ')}</p>
        </Card>
        <Card>
          <p className="text-xs text-gray-400 mb-1">Model version</p>
          <p className="text-sm font-medium">{modelVer != null ? `v${modelVer}` : '—'}</p>
        </Card>
        <Card>
          <p className="text-xs text-gray-400 mb-1">Feature store</p>
          <p className="text-sm font-medium">
            {featureStatus
              ? featureStatus.hash_present
                ? `Redis hit · age ${featureStatus.age_s != null ? Math.round(featureStatus.age_s) + 's' : '?'}`
                : 'Redis miss (Postgres fallback)'
              : '—'}
          </p>
        </Card>
        <Card>
          <p className="text-xs text-gray-400 mb-1">As of</p>
          <p className="text-sm font-medium">{asOf ? fmtFull(asOf) : '—'}</p>
        </Card>
      </div>

      {/* Feature warnings */}
      {featureStatus && (featureStatus.missing.length > 0 || featureStatus.degraded.length > 0) && (
        <Card>
          <p className="text-xs font-semibold uppercase tracking-wider text-amber-400 mb-2">
            Feature warnings
          </p>
          {featureStatus.missing.length > 0 && (
            <p className="text-sm text-amber-300">
              Missing: {featureStatus.missing.join(', ')}
            </p>
          )}
          {featureStatus.degraded.length > 0 && (
            <p className="text-sm text-amber-300 mt-1">
              Degraded: {featureStatus.degraded.join(', ')}
            </p>
          )}
        </Card>
      )}
    </div>
  );
}
