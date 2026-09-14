import type {
  HistoryResponse,
  ModelInfoResponse,
  ModelVersion,
  PipelineStats,
  DriftScores,
  DriftHistory,
  PromInstant,
  PromSeries,
  PredictResponse,
} from './types';

const API = '/api';
const PROM = '/prom/api/v1';

// ── Helper ──────────────────────────────────────────────────────────────────

async function get<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`${res.status} ${res.statusText}: ${body}`);
  }
  return res.json() as Promise<T>;
}

async function post<T>(url: string, body?: unknown): Promise<T> {
  const res = await fetch(url, {
    method: 'POST',
    headers: body !== undefined ? { 'Content-Type': 'application/json' } : {},
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({}));
    const detail = (data as { detail?: string }).detail ?? res.statusText;
    throw new Error(detail);
  }
  return res.json() as Promise<T>;
}

// ── API endpoints ────────────────────────────────────────────────────────────

export async function getCities(): Promise<string[]> {
  const data = await get<{ cities: string[] }>(`${API}/cities`);
  return data.cities;
}

export async function getHistory(city: string, hours = 24): Promise<HistoryResponse> {
  return get<HistoryResponse>(`${API}/history/${encodeURIComponent(city)}?hours=${hours}`);
}

export async function predict(city: string, horizonHours = 24): Promise<PredictResponse> {
  return post<PredictResponse>(`${API}/predict`, { city, horizon_hours: horizonHours });
}

export async function getModelInfo(): Promise<ModelInfoResponse> {
  return get<ModelInfoResponse>(`${API}/model/info`);
}

export async function getModelVersions(): Promise<ModelVersion[]> {
  const data = await get<{ versions: ModelVersion[] }>(`${API}/model/versions`);
  return data.versions;
}

export async function promoteStaging(): Promise<{ status: string; promoted_version: number }> {
  return post(`${API}/admin/promote`);
}

export async function reloadModels(): Promise<{ status: string }> {
  return post(`${API}/admin/reload`);
}

export async function getPipelineStats(): Promise<PipelineStats> {
  return get<PipelineStats>(`${API}/pipeline/stats`);
}

export async function getDriftScores(): Promise<DriftScores> {
  return get<DriftScores>(`${API}/drift/scores`);
}

export async function getDriftHistory(windowHours = 6, step = '5m'): Promise<DriftHistory> {
  return get<DriftHistory>(
    `${API}/drift/history?window_hours=${windowHours}&step=${encodeURIComponent(step)}`,
  );
}

// ── Prometheus helpers ────────────────────────────────────────────────────────

/** Instant vector query. Returns [] on error (Prometheus may not be running). */
export async function promQuery(query: string): Promise<PromInstant[]> {
  try {
    const data = await get<{ status: string; data: { result: PromInstant[] } }>(
      `${PROM}/query?query=${encodeURIComponent(query)}`,
    );
    return data.status === 'success' ? data.data.result : [];
  } catch {
    return [];
  }
}

/** Range vector query. Returns [] on error. */
export async function promQueryRange(
  query: string,
  startUnix: number,
  endUnix: number,
  step: string,
): Promise<PromSeries[]> {
  try {
    const params = new URLSearchParams({
      query,
      start: String(startUnix),
      end: String(endUnix),
      step,
    });
    const data = await get<{ status: string; data: { result: PromSeries[] } }>(
      `${PROM}/query_range?${params}`,
    );
    return data.status === 'success' ? data.data.result : [];
  } catch {
    return [];
  }
}

/** Extract a single numeric value from an instant query result. */
export function singleValue(results: PromInstant[]): number | null {
  if (results.length === 0) return null;
  const v = parseFloat(results[0].value[1]);
  return isFinite(v) ? v : null;
}
