import { useQuery } from '@tanstack/react-query';
import { promQuery, singleValue } from '../api';
import StatCard from '../components/StatCard';
import Card from '../components/Card';

// ── Metric queries ──────────────────────────────────────────────────────────

const QUERIES = {
  kafkaLag:    'sum(redpanda_kafka_consumer_group_lag)',
  rowsPerMin:  'sum(rate(ingestor_batch_write_rows_total[5m])) * 60',
  dlqTotal:    'sum(ingestor_messages_dlq_total)',
  lastRun:     'worker_pipeline_last_run_timestamp',
  consumed:    'sum(rate(ingestor_messages_consumed_total[5m])) * 60',
  valFailed:   'sum(rate(ingestor_messages_validation_failed_total[5m])) * 60',
} as const;

function relTime(unixTs: number): string {
  const diff = Date.now() / 1000 - unixTs;
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

export default function PipelineView() {
  const opts = { refetchInterval: 30_000, staleTime: 15_000 };

  const lag      = useQuery({ queryKey: ['prom', 'kafkaLag'],   queryFn: () => promQuery(QUERIES.kafkaLag),   ...opts });
  const rows     = useQuery({ queryKey: ['prom', 'rowsPerMin'], queryFn: () => promQuery(QUERIES.rowsPerMin), ...opts });
  const dlq      = useQuery({ queryKey: ['prom', 'dlqTotal'],   queryFn: () => promQuery(QUERIES.dlqTotal),   ...opts });
  const lastRun  = useQuery({ queryKey: ['prom', 'lastRun'],    queryFn: () => promQuery(QUERIES.lastRun),    ...opts });
  const consumed = useQuery({ queryKey: ['prom', 'consumed'],   queryFn: () => promQuery(QUERIES.consumed),   ...opts });
  const valFail  = useQuery({ queryKey: ['prom', 'valFailed'],  queryFn: () => promQuery(QUERIES.valFailed),  ...opts });

  const lagVal     = singleValue(lag.data     ?? []);
  const rowsVal    = singleValue(rows.data    ?? []);
  const dlqVal     = singleValue(dlq.data     ?? []);
  const lastRunVal = singleValue(lastRun.data ?? []);
  const consumedVal = singleValue(consumed.data ?? []);
  const valFailVal  = singleValue(valFail.data  ?? []);

  const lagStatus = lagVal === null ? 'neutral' : lagVal > 1000 ? 'error' : lagVal > 100 ? 'warn' : 'ok';
  const dlqStatus = dlqVal === null ? 'neutral' : dlqVal > 0 ? 'warn' : 'ok';

  const lastRunTs = lastRunVal ?? 0;
  const lastRunAgo = lastRunVal ? relTime(lastRunTs) : null;
  const lastRunStale = lastRunVal ? (Date.now() / 1000 - lastRunTs > 300) : false;

  const promUnavailable = lag.isError && rows.isError && dlq.isError;

  return (
    <div className="space-y-4">
      <h1 className="text-lg font-semibold text-gray-100">Pipeline Health</h1>

      {promUnavailable && (
        <div className="bg-amber-900/30 border border-amber-800 rounded-lg px-4 py-3 text-amber-300 text-sm">
          Prometheus is unreachable. Start the obs profile with{' '}
          <code className="font-mono text-amber-200">make up-obs</code> to see live metrics.
        </div>
      )}

      {/* Top stat cards */}
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
          sub={lastRunVal ? new Date(lastRunVal * 1000).toLocaleString() : 'no data'}
        />
      </div>

      {/* Throughput detail */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        <Card title="Ingestor throughput (5-min rate)">
          <div className="space-y-3">
            <ThroughputRow label="Messages consumed / min" value={consumedVal} />
            <ThroughputRow label="Rows written to Postgres / min" value={rowsVal} />
            <ThroughputRow label="Validation failures / min" value={valFailVal} warn={valFailVal !== null && valFailVal > 0} />
          </div>
        </Card>

        <Card title="Consumer group lag by topic">
          <LagByTopic data={lag.data ?? []} />
        </Card>
      </div>
    </div>
  );
}

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
      <span className={`text-sm font-medium tabular-nums ${warn ? 'text-amber-400' : 'text-gray-100'}`}>
        {value !== null ? value.toLocaleString(undefined, { maximumFractionDigits: 1 }) : '—'}
      </span>
    </div>
  );
}

function LagByTopic({ data }: { data: Array<{ metric: Record<string, string>; value: [number, string] }> }) {
  if (data.length === 0) {
    return <p className="text-sm text-gray-500">No consumer lag data</p>;
  }

  return (
    <div className="space-y-2">
      {data.map((row, i) => {
        const topic = row.metric.topic ?? row.metric.group ?? `series-${i}`;
        const val = parseFloat(row.value[1]);
        const pct = Math.min((val / 2000) * 100, 100);
        const barColor = val > 1000 ? 'bg-red-500' : val > 100 ? 'bg-amber-500' : 'bg-emerald-500';
        return (
          <div key={i}>
            <div className="flex justify-between text-xs text-gray-400 mb-1">
              <span className="truncate max-w-[200px]">{topic}</span>
              <span className="ml-2 tabular-nums">{Math.round(val).toLocaleString()}</span>
            </div>
            <div className="h-1.5 bg-gray-800 rounded-full overflow-hidden">
              <div className={`h-full rounded-full ${barColor}`} style={{ width: `${pct}%` }} />
            </div>
          </div>
        );
      })}
    </div>
  );
}
