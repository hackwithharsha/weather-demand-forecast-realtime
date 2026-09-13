interface StatCardProps {
  label: string;
  value: string | number | null;
  unit?: string;
  status?: 'ok' | 'warn' | 'error' | 'neutral';
  sub?: string;
}

const STATUS_COLORS = {
  ok:      'text-emerald-400',
  warn:    'text-amber-400',
  error:   'text-red-400',
  neutral: 'text-gray-100',
} as const;

export default function StatCard({ label, value, unit, status = 'neutral', sub }: StatCardProps) {
  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
      <p className="text-xs font-semibold uppercase tracking-wider text-gray-400 mb-2">
        {label}
      </p>
      <p className={`text-3xl font-bold tabular-nums ${STATUS_COLORS[status]}`}>
        {value ?? '—'}
        {value !== null && unit && (
          <span className="ml-1 text-base font-normal text-gray-400">{unit}</span>
        )}
      </p>
      {sub && <p className="mt-1 text-xs text-gray-500">{sub}</p>}
    </div>
  );
}
