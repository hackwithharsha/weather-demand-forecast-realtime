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

// ── Pipeline stats ──────────────────────────────────────────────────────────

export interface TopicLag {
  topic: string;
  lag: number;
}

export interface PipelineStats {
  kafka_lag: number | null;
  rows_per_min: number | null;
  dlq_total: number | null;
  last_pipeline_run_ts: number | null;   // Unix seconds
  msgs_consumed_per_min: number | null;
  validation_failed_per_min: number | null;
  lag_by_topic: TopicLag[];
  prometheus_available: boolean;
}

// ── Drift types ──────────────────────────────────────────────────────────────

export interface DriftScore {
  feature: string;
  score: number;
  detected: boolean;
}

export interface DriftScores {
  scores: DriftScore[];
  last_run_ts: number | null;       // Unix seconds
  reference_count: number | null;
  current_count: number | null;
  prometheus_available: boolean;
}

export interface DriftHistoryPoint {
  ts: number;     // Unix seconds
  score: number;
}

export interface DriftSeries {
  feature: string;
  points: DriftHistoryPoint[];
}

export interface DriftHistory {
  series: DriftSeries[];
  step: string;
  window_hours: number;
  prometheus_available: boolean;
}

// ── WebSocket message ───────────────────────────────────────────────────────

export interface LiveEvent {
  event: 'predict' | 'forecast_refresh';
  data: PredictResponse;
}
