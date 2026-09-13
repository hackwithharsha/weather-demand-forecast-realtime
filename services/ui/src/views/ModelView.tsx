import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getModelInfo, getModelVersions, promoteStaging, reloadModels } from '../api';
import Card from '../components/Card';
import Badge from '../components/Badge';
import type { ModelStage } from '../types';

function fmtMae(v: number | null): string {
  return v != null ? v.toFixed(4) : '—';
}

function fmtTs(iso: string | null | undefined): string {
  if (!iso) return '—';
  return new Date(iso).toLocaleString();
}

function ModelCard({ info, title }: { info: ModelStage | null; title: string }) {
  if (!info) {
    return (
      <Card title={title}>
        <p className="text-sm text-gray-500">No {title.toLowerCase()} model loaded.</p>
      </Card>
    );
  }
  return (
    <Card title={title}>
      <div className="space-y-3">
        <div className="flex items-center gap-2">
          <span className="text-2xl font-bold text-gray-100">v{info.version}</span>
          <Badge label={info.stage} stage={info.stage} />
        </div>
        <dl className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
          <dt className="text-gray-400">Model</dt>
          <dd className="font-mono text-gray-200 truncate">{info.model_name}</dd>
          <dt className="text-gray-400">Validation MAE</dt>
          <dd className={`font-mono font-medium ${info.val_mae != null ? 'text-emerald-400' : 'text-gray-500'}`}>
            {fmtMae(info.val_mae)}
          </dd>
          <dt className="text-gray-400">Run ID</dt>
          <dd className="font-mono text-xs text-gray-400 truncate">{info.run_id}</dd>
          <dt className="text-gray-400">Loaded at</dt>
          <dd className="text-gray-300">{fmtTs(info.loaded_at)}</dd>
        </dl>
        {Object.keys(info.tags).length > 0 && (
          <div className="flex flex-wrap gap-1.5 pt-1">
            {Object.entries(info.tags).map(([k, v]) => (
              <span key={k} className="text-xs px-2 py-0.5 rounded bg-gray-800 text-gray-300 font-mono">
                {k}={v}
              </span>
            ))}
          </div>
        )}
      </div>
    </Card>
  );
}

export default function ModelView() {
  const queryClient = useQueryClient();
  const [promoteMsg, setPromoteMsg] = useState<string | null>(null);

  const { data: info, isError: infoError } = useQuery({
    queryKey: ['model-info'],
    queryFn: getModelInfo,
    refetchInterval: 30_000,
  });

  const { data: versions } = useQuery({
    queryKey: ['model-versions'],
    queryFn: getModelVersions,
    staleTime: 60_000,
  });

  const promote = useMutation({
    mutationFn: promoteStaging,
    onSuccess: (data) => {
      setPromoteMsg(`v${data.promoted_version} promoted to Production.`);
      void queryClient.invalidateQueries({ queryKey: ['model-info'] });
      void queryClient.invalidateQueries({ queryKey: ['model-versions'] });
      void queryClient.invalidateQueries({ queryKey: ['predict'] });
    },
    onError: (err: Error) => {
      setPromoteMsg(`Error: ${err.message}`);
    },
  });

  const reload = useMutation({
    mutationFn: reloadModels,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['model-info'] });
    },
  });

  const hasStaging = !!info?.staging;

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <h1 className="text-lg font-semibold text-gray-100">Model Registry</h1>
        <div className="flex gap-2">
          <button
            onClick={() => reload.mutate()}
            disabled={reload.isPending}
            className="px-3 py-1.5 text-sm rounded-lg bg-gray-800 hover:bg-gray-700
                       text-gray-300 border border-gray-700 disabled:opacity-50 transition-colors"
          >
            {reload.isPending ? 'Reloading…' : 'Reload models'}
          </button>
          <button
            onClick={() => {
              setPromoteMsg(null);
              promote.mutate();
            }}
            disabled={!hasStaging || promote.isPending}
            title={hasStaging ? 'Promote Staging → Production' : 'No Staging model available'}
            className="px-3 py-1.5 text-sm rounded-lg bg-blue-600 hover:bg-blue-500
                       text-white disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
          >
            {promote.isPending ? 'Promoting…' : 'Promote Staging →'}
          </button>
        </div>
      </div>

      {/* Promote feedback */}
      {promoteMsg && (
        <div className={[
          'px-4 py-2 rounded-lg text-sm border',
          promoteMsg.startsWith('Error')
            ? 'bg-red-900/30 border-red-800 text-red-300'
            : 'bg-emerald-900/30 border-emerald-800 text-emerald-300',
        ].join(' ')}>
          {promoteMsg}
        </div>
      )}

      {infoError && (
        <div className="bg-amber-900/30 border border-amber-800 rounded-lg px-4 py-3 text-amber-300 text-sm">
          Model info unavailable — Production model may not be loaded yet.
          Run <code className="font-mono text-amber-200">make train && make promote</code>.
        </div>
      )}

      {/* Production + Staging cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <ModelCard info={info?.production ?? null} title="Production" />
        <ModelCard info={info?.staging ?? null} title="Staging" />
      </div>

      {/* Feature store stats */}
      {info?.feature_store && (
        <Card title="Feature store">
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 text-sm">
            <div>
              <p className="text-gray-400 mb-1">Redis hits</p>
              <p className="text-gray-100 font-medium tabular-nums">
                {info.feature_store.redis_hits.toLocaleString()}
              </p>
            </div>
            <div>
              <p className="text-gray-400 mb-1">Redis misses</p>
              <p className="text-gray-100 font-medium tabular-nums">
                {info.feature_store.redis_misses.toLocaleString()}
              </p>
            </div>
            <div>
              <p className="text-gray-400 mb-1">Miss rate</p>
              <p className={`font-medium tabular-nums ${
                (info.feature_store.miss_rate_pct ?? 0) > 10 ? 'text-amber-400' : 'text-emerald-400'
              }`}>
                {info.feature_store.miss_rate_pct != null
                  ? `${info.feature_store.miss_rate_pct}%`
                  : '—'}
              </p>
            </div>
            <div>
              <p className="text-gray-400 mb-1">Top miss</p>
              <p className="text-gray-100 text-xs font-mono truncate">
                {Object.entries(info.feature_store.per_field_miss_counts).sort(
                  ([, a], [, b]) => b - a,
                )[0]?.[0] ?? '—'}
              </p>
            </div>
          </div>
        </Card>
      )}

      {/* Version history table */}
      {versions && versions.length > 0 && (
        <Card title="Version history">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-800">
                  <th className="pb-2 text-left text-xs font-semibold uppercase tracking-wider text-gray-400 pr-4">Version</th>
                  <th className="pb-2 text-left text-xs font-semibold uppercase tracking-wider text-gray-400 pr-4">Stage</th>
                  <th className="pb-2 text-left text-xs font-semibold uppercase tracking-wider text-gray-400 pr-4">Val MAE</th>
                  <th className="pb-2 text-left text-xs font-semibold uppercase tracking-wider text-gray-400 pr-4">Created</th>
                  <th className="pb-2 text-left text-xs font-semibold uppercase tracking-wider text-gray-400">Run ID</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-800">
                {versions.map((v) => (
                  <tr key={v.version} className="hover:bg-gray-800/50 transition-colors">
                    <td className="py-2.5 pr-4 font-mono text-gray-100">v{v.version}</td>
                    <td className="py-2.5 pr-4">
                      <Badge label={v.stage} stage={v.stage} />
                    </td>
                    <td className="py-2.5 pr-4 font-mono text-emerald-400">
                      {fmtMae(v.val_mae)}
                    </td>
                    <td className="py-2.5 pr-4 text-gray-400">
                      {v.created_at ? new Date(v.created_at).toLocaleString() : '—'}
                    </td>
                    <td className="py-2.5 font-mono text-xs text-gray-500 truncate max-w-[160px]">
                      {v.run_id}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}
    </div>
  );
}
