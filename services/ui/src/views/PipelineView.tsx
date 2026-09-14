import { useQuery } from '@tanstack/react-query';
import { getPipelineStats } from '../api';
import StatCard from '../components/StatCard';
import Card from '../components/Card';
import type { TopicLag } from '../types';

// ── Helpers ──────────────────────────────────────────────────────────────────

function relTime(unixSec: number): string {
  const diff = Date.now() / 1000 - unixSec;
  if (diff < 60)    return `${Math.round(diff)}s ago`;
  if (diff < 3600)  return `${Math.round(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.round(diff / 3600)}h ago`;
  return `${Math.round(diff / 86400)}d ago`;
}

function fmt(v: number | null, decimals = 0): string | null {
  if (v === null) return null;
  return v.toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

// ── View ──────────────────────────────────────────────────────────────────────

export default function PipelineView() {
  const { data, isFetching } = useQuery({
    queryKey: ['pipeline', 'stats'],
    queryFn:  getPipelineStats,
    refetchInterval: 30_000,
    staleTime:       15_000,
  });

  const lagVal      = data?.kafka_lag            ?? null;
  const rowsVal     = data?.rows_per_min         ?? null;
  const dlqVal      = data?.dlq_total            ?? null;
  const lastRunTs   = data?.last_pipeline_run_ts ?? null;
  const consumedVal = data?.msgs_consumed_per_min      ?? null;
  const valFailVal  = data?.validation_failed_per_min  ?? null;
  const lagByTopic  = data?.lag_by_topic         ?? [];
  const promAvail   = data?.prometheus_available ?? true;

  const lagStatus = lagVal === null
    ? 'neutral'
    : lagVal > 1000 ? 'error'
    : lagVal > 100  ? 'warn'
    : 'ok';

  const dlqStatus    = dlqVal === null ? 'neutral' : dlqVal > 0 ? 'warn' : 'ok';
  const lastRunAgo   = lastRunTs ? relTime(lastRunTs) : null;
  const lastRunStale = lastRunTs ? (Date.now() / 1000 - lastRunTs > 300) : false;

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <h1 className="text-lg font-semibold text-gray-100">Pipeline Health</h1>
        {isFetching && (
          <span className="text-xs text-gray-500 animate-pulse">refreshing…</span>
        )}
      </div>

      {!promAvail && !isFetching && (
        <div className="bg-amber-900/30 border border-amber-800 rounded-lg px-4 py-3 text-amber-300 text-sm">
          Prometheus is unreachable — metric values are unavailable. Start the
          obs profile with{' '}
          <code className="font-mono text-amber-200">make up-obs</code> to see
          live pipeline stats.
        </div>
      )}

      {/* ── Top stat cards ────────────────────────────────────────────────── */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          label="Kafka consumer lag"
          value={fmt(lagVal)}
          unit="msgs"
          status={lagStatus}
          sub={lagVal !== null ? 'sum across all partitions' : 'Prometheus unavailable'}
        />
        <StatCard
          label="Rows ingested / min"
          value={fmt(rowsVal, 1)}
          status="neutral"
          sub="5-min rolling rate"
        />
        <StatCard
          label="DLQ total"
          value={fmt(dlqVal)}
          unit="msgs"
          status={dlqStatus}
          sub="dead-letter queue (all topics)"
        />
        <StatCard
          label="Last pipeline run"
          value={lastRunAgo}
          status={lastRunStale ? 'warn' : 'ok'}
          sub={lastRunTs ? new Date(lastRunTs * 1000).toLocaleString() : 'no data'}
        />
      </div>

      {/* ── Detail cards ──────────────────────────────────────────────────── */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        <Card title="Ingestor throughput (5-min rate)">
          <div className="space-y-3">
            <ThroughputRow
              label="Messages consumed / min"
              value={consumedVal}
            />
            <ThroughputRow
              label="Rows written to Postgres / min"
              value={rowsVal}
            />
            <ThroughputRow
              label="Validation failures / min"
              value={valFailVal}
              warn={valFailVal !== null && valFailVal > 0}
            />
          </div>
        </Card>

        <Card title="Consumer group lag by topic">
          <LagByTopic data={lagByTopic} />
        </Card>
      </div>
    </div>
  );
}

// ── Sub-components ────────────────────────────────────────────────────────────

function ThroughputRow({
  label,
  value,
  warn = false,
}: {
  label: string;
  value: number | null;
  warn?: boolean;
}) {
  return (
    <div className="flex items-center justify-between">
      <span className="text-sm text-gray-400">{label}</span>
      <span
        className={`text-sm font-medium tabular-nums ${
          warn ? 'text-amber-400' : 'text-gray-100'
        }`}
      >
        {value !== null
          ? value.toLocaleString(undefined, { maximumFractionDigits: 1 })
          : '—'}
      </span>
    </div>
  );
}

function LagByTopic({ data }: { data: TopicLag[] }) {
  if (data.length === 0) {
    return <p className="text-sm text-gray-500">No consumer lag data</p>;
  }

  return (
    <div className="space-y-2">
      {data.map((row) => {
        const pct      = Math.min((row.lag / 2000) * 100, 100);
        const barColor =
          row.lag > 1000 ? 'bg-red-500' :
          row.lag > 100  ? 'bg-amber-500' :
          'bg-emerald-500';

        return (
          <div key={row.topic}>
            <div className="flex justify-between text-xs text-gray-400 mb-1">
              <span className="truncate max-w-[200px]">{row.topic}</span>
              <span className="ml-2 tabular-nums">
                {Math.round(row.lag).toLocaleString()}
              </span>
            </div>
            <div className="h-1.5 bg-gray-800 rounded-full overflow-hidden">
              <div
                className={`h-full rounded-full ${barColor}`}
                style={{ width: `${pct}%` }}
              />
            </div>
          </div>
        );
      })}
    </div>
  );
}
