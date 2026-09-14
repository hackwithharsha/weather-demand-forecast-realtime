import { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getModelInfo, getModelVersions, promoteStaging, reloadModels } from '../api';
import Card from '../components/Card';
import Badge from '../components/Badge';
import type { ModelInfoResponse, ModelStage, ModelVersion } from '../types';

// ── Helpers ───────────────────────────────────────────────────────────────────

function fmtMae(v: number | null): string {
  return v != null ? v.toFixed(4) : '—';
}

function fmtTs(iso: string | null | undefined): string {
  if (!iso) return '—';
  return new Date(iso).toLocaleString();
}

// ── Confirmation dialog ───────────────────────────────────────────────────────

interface ConfirmDialogProps {
  staging: ModelStage;
  production: ModelStage | null;
  isPending: boolean;
  onConfirm: () => void;
  onCancel: () => void;
}

function ConfirmDialog({
  staging,
  production,
  isPending,
  onConfirm,
  onCancel,
}: ConfirmDialogProps) {
  // Close on Escape unless mutation is in-flight
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !isPending) onCancel();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [isPending, onCancel]);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm"
      onClick={(e) => {
        if (e.target === e.currentTarget && !isPending) onCancel();
      }}
    >
      <div
        className="bg-gray-900 border border-gray-700 rounded-2xl shadow-2xl w-full max-w-md p-6 space-y-5"
        role="dialog"
        aria-modal="true"
        aria-labelledby="promote-dialog-title"
      >
        <h2
          id="promote-dialog-title"
          className="text-base font-semibold text-gray-100"
        >
          Promote Staging to Production?
        </h2>

        {/* ── Change summary ────────────────────────────────────────── */}
        <div className="space-y-2 text-sm">
          {/* Staging → Production */}
          <div className="flex items-start gap-3 p-3 rounded-lg bg-gray-800 border border-gray-700">
            <span className="text-gray-500 text-xs uppercase tracking-wider mt-0.5 w-14 flex-shrink-0">
              Promote
            </span>
            <div className="min-w-0">
              <span className="text-white font-semibold">v{staging.version}</span>
              {staging.val_mae != null && (
                <span className="text-gray-400 ml-2 font-mono text-xs">
                  MAE {staging.val_mae.toFixed(4)}
                </span>
              )}
              <div className="mt-1.5 flex items-center gap-1.5 text-xs flex-wrap">
                <Badge label="Staging" stage="Staging" />
                <span className="text-gray-500">→</span>
                <Badge label="Production" stage="Production" />
              </div>
            </div>
          </div>

          {/* Current Production → Archived */}
          {production && (
            <div className="flex items-start gap-3 p-3 rounded-lg bg-gray-800/50 border border-gray-800">
              <span className="text-gray-500 text-xs uppercase tracking-wider mt-0.5 w-14 flex-shrink-0">
                Archive
              </span>
              <div className="min-w-0">
                <span className="text-gray-400">v{production.version}</span>
                {production.val_mae != null && (
                  <span className="text-gray-600 ml-2 font-mono text-xs">
                    MAE {production.val_mae.toFixed(4)}
                  </span>
                )}
                <div className="mt-1.5 flex items-center gap-1.5 text-xs flex-wrap">
                  <Badge label="Production" stage="Production" />
                  <span className="text-gray-500">→</span>
                  <Badge label="Archived" stage="Archived" />
                </div>
              </div>
            </div>
          )}
        </div>

        <p className="text-xs text-gray-500 leading-relaxed">
          The serving API will hot-swap to the new model immediately after the
          registry transition. In-flight predictions will finish with the current
          Production model.
        </p>

        {/* ── Actions ───────────────────────────────────────────────── */}
        <div className="flex justify-end gap-2 pt-1">
          <button
            onClick={onCancel}
            disabled={isPending}
            className="px-4 py-2 text-sm rounded-lg bg-gray-800 hover:bg-gray-700
                       text-gray-300 border border-gray-700 disabled:opacity-50
                       transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={onConfirm}
            disabled={isPending}
            className="px-4 py-2 text-sm rounded-lg bg-blue-600 hover:bg-blue-500
                       text-white disabled:opacity-50 transition-colors
                       min-w-[148px] text-center"
          >
            {isPending ? (
              <span className="inline-flex items-center justify-center gap-2">
                <span
                  className="w-3.5 h-3.5 border-2 border-white/30 border-t-white
                             rounded-full animate-spin"
                />
                Promoting…
              </span>
            ) : (
              'Confirm Promote'
            )}
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Model card ────────────────────────────────────────────────────────────────

function ModelCard({
  info,
  title,
  dim = false,
}: {
  info: ModelStage | null;
  title: string;
  dim?: boolean;
}) {
  if (!info) {
    return (
      <Card title={title}>
        <p className="text-sm text-gray-500">No {title.toLowerCase()} model loaded.</p>
      </Card>
    );
  }

  return (
    <Card
      title={title}
      className={dim ? 'opacity-60 transition-opacity' : 'transition-opacity'}
    >
      <div className="space-y-3">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-2xl font-bold text-gray-100">v{info.version}</span>
          <Badge label={info.stage} stage={info.stage} />
          {dim && <span className="text-xs text-gray-500 italic">updating…</span>}
        </div>

        <dl className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
          <dt className="text-gray-400">Model</dt>
          <dd className="font-mono text-gray-200 truncate">{info.model_name}</dd>

          <dt className="text-gray-400">Validation MAE</dt>
          <dd
            className={`font-mono font-medium ${
              info.val_mae != null ? 'text-emerald-400' : 'text-gray-500'
            }`}
          >
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
              <span
                key={k}
                className="text-xs px-2 py-0.5 rounded bg-gray-800 text-gray-300 font-mono"
              >
                {k}={v}
              </span>
            ))}
          </div>
        )}
      </div>
    </Card>
  );
}

// ── Main view ─────────────────────────────────────────────────────────────────

export default function ModelView() {
  const queryClient = useQueryClient();

  // Snapshot of the staging/production versions at the time the dialog opens.
  // Stored separately so the dialog stays accurate during the optimistic update
  // phase (when info.staging is already null in the cache).
  const [confirmTarget, setConfirmTarget] = useState<{
    staging: ModelStage;
    production: ModelStage | null;
  } | null>(null);

  const [promoteMsg, setPromoteMsg] = useState<{
    ok: boolean;
    text: string;
  } | null>(null);

  // ── Queries ────────────────────────────────────────────────────────────

  const { data: info, isError: infoError } = useQuery({
    queryKey: ['model-info'],
    queryFn:  getModelInfo,
    refetchInterval: 30_000,
  });

  const { data: versions } = useQuery({
    queryKey: ['model-versions'],
    queryFn:  getModelVersions,
    staleTime: 60_000,
  });

  // ── Promote mutation ───────────────────────────────────────────────────

  const promote = useMutation({
    mutationFn: promoteStaging,

    onMutate: async () => {
      // Prevent in-flight refetches from overwriting the optimistic state
      await queryClient.cancelQueries({ queryKey: ['model-info'] });
      await queryClient.cancelQueries({ queryKey: ['model-versions'] });

      const prevInfo     = queryClient.getQueryData<ModelInfoResponse>(['model-info']);
      const prevVersions = queryClient.getQueryData<ModelVersion[]>(['model-versions']);

      // Optimistic model-info: staging becomes the new production
      if (prevInfo?.staging) {
        queryClient.setQueryData<ModelInfoResponse>(['model-info'], (old) => {
          if (!old) return old;
          return {
            ...old,
            production: {
              ...old.staging!,
              stage: 'Production',
              loaded_at: new Date().toISOString(),
            },
            staging: null,
          };
        });
      }

      // Optimistic version list: flip Staging → Production, Production → Archived
      if (prevVersions) {
        queryClient.setQueryData<ModelVersion[]>(['model-versions'], (old) => {
          if (!old) return old;
          return old.map((v) => {
            if (v.stage === 'Production') return { ...v, stage: 'Archived' };
            if (v.stage === 'Staging')    return { ...v, stage: 'Production' };
            return v;
          });
        });
      }

      return { prevInfo, prevVersions };
    },

    onSuccess: (data) => {
      setPromoteMsg({
        ok:   true,
        text: `v${data.promoted_version} promoted to Production.`,
      });
      setConfirmTarget(null);
    },

    onError: (err: Error, _vars, ctx) => {
      // Roll back optimistic update
      if (ctx?.prevInfo !== undefined) {
        queryClient.setQueryData(['model-info'], ctx.prevInfo);
      }
      if (ctx?.prevVersions !== undefined) {
        queryClient.setQueryData(['model-versions'], ctx.prevVersions);
      }
      setPromoteMsg({ ok: false, text: err.message });
      setConfirmTarget(null);
    },

    onSettled: () => {
      // Always re-fetch real data after mutation settles (success or error)
      void queryClient.invalidateQueries({ queryKey: ['model-info'] });
      void queryClient.invalidateQueries({ queryKey: ['model-versions'] });
      void queryClient.invalidateQueries({ queryKey: ['predict'] });
    },
  });

  // ── Reload mutation ────────────────────────────────────────────────────

  const reload = useMutation({
    mutationFn: reloadModels,
    onSettled: () => {
      void queryClient.invalidateQueries({ queryKey: ['model-info'] });
    },
  });

  const hasStaging  = !!info?.staging;
  const isPromoting = promote.isPending;

  // ── Render ─────────────────────────────────────────────────────────────

  return (
    <div className="space-y-4">

      {/* ── Confirmation dialog ───────────────────────────────────────── */}
      {confirmTarget && (
        <ConfirmDialog
          staging={confirmTarget.staging}
          production={confirmTarget.production}
          isPending={isPromoting}
          onConfirm={() => promote.mutate()}
          onCancel={() => {
            if (!isPromoting) setConfirmTarget(null);
          }}
        />
      )}

      {/* ── Header ───────────────────────────────────────────────────── */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <h1 className="text-lg font-semibold text-gray-100">Model Registry</h1>

        <div className="flex gap-2">
          <button
            onClick={() => reload.mutate()}
            disabled={reload.isPending || isPromoting}
            className="px-3 py-1.5 text-sm rounded-lg bg-gray-800 hover:bg-gray-700
                       text-gray-300 border border-gray-700 disabled:opacity-50
                       transition-colors"
          >
            {reload.isPending ? 'Reloading…' : 'Reload models'}
          </button>

          <button
            onClick={() => {
              if (!info?.staging) return;
              setPromoteMsg(null);
              setConfirmTarget({
                staging:    info.staging,
                production: info.production ?? null,
              });
            }}
            disabled={!hasStaging || isPromoting}
            title={
              hasStaging
                ? 'Promote Staging → Production'
                : 'No Staging model available'
            }
            className="px-3 py-1.5 text-sm rounded-lg bg-blue-600 hover:bg-blue-500
                       text-white disabled:opacity-40 disabled:cursor-not-allowed
                       transition-colors"
          >
            Promote Staging →
          </button>
        </div>
      </div>

      {/* ── Feedback banner ───────────────────────────────────────────── */}
      {promoteMsg && (
        <div
          className={[
            'flex items-center justify-between gap-3 px-4 py-2.5 rounded-lg text-sm border',
            promoteMsg.ok
              ? 'bg-emerald-900/30 border-emerald-800 text-emerald-300'
              : 'bg-red-900/30 border-red-800 text-red-300',
          ].join(' ')}
        >
          <span>{promoteMsg.text}</span>
          <button
            onClick={() => setPromoteMsg(null)}
            className="opacity-60 hover:opacity-100 transition-opacity flex-shrink-0"
            aria-label="Dismiss"
          >
            ✕
          </button>
        </div>
      )}

      {infoError && (
        <div className="bg-amber-900/30 border border-amber-800 rounded-lg px-4 py-3 text-amber-300 text-sm">
          Model info unavailable — the Production model may not be loaded yet.
          Run <code className="font-mono text-amber-200">make train</code> then
          reload or promote a version.
        </div>
      )}

      {/* ── Production + Staging cards ────────────────────────────────── */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <ModelCard
          info={info?.production ?? null}
          title="Production"
          dim={isPromoting}
        />
        <ModelCard
          info={info?.staging ?? null}
          title="Staging"
          dim={isPromoting}
        />
      </div>

      {/* ── Feature store stats ───────────────────────────────────────── */}
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
              <p
                className={`font-medium tabular-nums ${
                  (info.feature_store.miss_rate_pct ?? 0) > 10
                    ? 'text-amber-400'
                    : 'text-emerald-400'
                }`}
              >
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

      {/* ── Version history table ─────────────────────────────────────── */}
      {versions && versions.length > 0 && (
        <Card title="Version history">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-800">
                  {['Version', 'Stage', 'Val MAE', 'Created', 'Run ID'].map(
                    (h) => (
                      <th
                        key={h}
                        className="pb-2 text-left text-xs font-semibold uppercase
                                   tracking-wider text-gray-400 pr-4 last:pr-0"
                      >
                        {h}
                      </th>
                    ),
                  )}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-800">
                {versions.map((v) => {
                  const isActive =
                    v.stage === 'Production' || v.stage === 'Staging';
                  return (
                    <tr
                      key={v.version}
                      className={[
                        'transition-colors',
                        isActive
                          ? 'bg-gray-800/30 hover:bg-gray-800/60'
                          : 'hover:bg-gray-800/30',
                        isPromoting && isActive ? 'opacity-50' : '',
                      ].join(' ')}
                    >
                      <td className="py-2.5 pr-4 font-mono text-gray-100">
                        v{v.version}
                      </td>
                      <td className="py-2.5 pr-4">
                        <Badge label={v.stage} stage={v.stage} />
                      </td>
                      <td
                        className={[
                          'py-2.5 pr-4 font-mono',
                          v.val_mae != null
                            ? 'text-emerald-400'
                            : 'text-gray-500',
                        ].join(' ')}
                      >
                        {fmtMae(v.val_mae)}
                      </td>
                      <td className="py-2.5 pr-4 text-gray-400 whitespace-nowrap">
                        {v.created_at
                          ? new Date(v.created_at).toLocaleString()
                          : '—'}
                      </td>
                      <td className="py-2.5 font-mono text-xs text-gray-500 truncate max-w-[160px]">
                        {v.run_id}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </Card>
      )}

    </div>
  );
}
