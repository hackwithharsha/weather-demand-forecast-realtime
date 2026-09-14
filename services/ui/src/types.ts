// ── API response types ──────────────────────────────────────────────────────

export interface HistoryPoint {
  hour_ts: string;
  total_demand: number;
}

export interface HistoryResponse {
  city: string;
  count: number;
  history: HistoryPoint[];
}

export interface HourForecast {
  target_hour: string;
  predicted_demand: number;
}

export interface FeatureStatus {
  hash_present: boolean;
  age_s: number | null;
  missing: string[];
  degraded: string[];
}

export interface PredictResponse {
  city: string;
  as_of: string;
  horizon_hours: number;
  model_version: number | null;
  source: string;
  feature_status: FeatureStatus;
  forecasts: HourForecast[];
}

export interface ModelStage {
  model_name: string;
  version: number;
  stage: string;
  run_id: string;
  val_mae: number | null;
  tags: Record<string, string>;
  loaded_at: string;
}

export interface ModelInfoResponse {
  production: ModelStage | null;
  staging: ModelStage | null;
  feature_store: {
    redis_hits: number;
    redis_misses: number;
    miss_rate_pct: number | null;
    per_field_miss_counts: Record<string, number>;
  };
}

export interface ModelVersion {
  version: number;
  stage: string;
  run_id: string;
  val_mae: number | null;
  created_at: string | null;
  tags: Record<string, string>;
}

// ── Prometheus types ────────────────────────────────────────────────────────

export interface PromInstant {
  metric: Record<string, string>;
  value: [number, string]; // [unix_ts, value_str]
}

export interface PromSeries {
  metric: Record<string, string>;
  values: Array<[number, string]>; // [[unix_ts, value_str], ...]
}

// ── Chart data types ────────────────────────────────────────────────────────

export interface LiveChartPoint {
  ts: number;       // Unix ms — used as the numeric x-axis key
  actual?: number;
  predicted?: number;
}

export interface DriftChartPoint {
  t: string;       // formatted time
  ts: number;      // unix timestamp for sorting
  [feature: string]: number | string;
}

// ── WebSocket message ───────────────────────────────────────────────────────

export interface LiveEvent {
  event: 'predict' | 'forecast_refresh';
  data: PredictResponse;
}
