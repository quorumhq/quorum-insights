// API response types — mirrors Python models

export interface DateRange {
  start: string | null
  end: string | null
}

// /api/overview
export interface OverviewData {
  total_events: number
  total_users: number
  date_range: DateRange
  dau_series: { date: string; dau: number }[]
  anomaly_count: number
  avg_dau: number
}

// /api/insights
export interface InsightCard {
  title: string
  severity: 'critical' | 'high' | 'medium' | 'low' | 'info'
  finding: string
  evidence: string
  action: string
  confidence: number
  category: string
  estimated_impact?: string
  related_metrics?: string[]
}

export interface InsightsData {
  cards: InsightCard[]
  prompt_version?: string
  model?: string
  stats_summary?: Record<string, unknown>
  note?: string
}

// /api/retention
export interface CohortRetention {
  key: string
  dimension: string
  size: number
  retention: Record<string, number>
}

export interface RetentionData {
  metric: string
  date_range: DateRange
  total_users: number
  periods: number[]
  overall_retention: Record<string, number>
  cohorts: CohortRetention[]
  best_cohort?: { key: string; dimension: string } & Record<string, number>
  worst_cohort?: { key: string; dimension: string } & Record<string, number>
}

// /api/features
export interface FeatureEntry {
  name: string
  users: number
  events: number
  impact: Record<string, number>
  net_score: number
}

export interface FeaturesData {
  metric: string
  caveat: string
  total_users: number
  total_features: number
  positive_count: number
  negative_count: number
  top_features: FeatureEntry[]
  negative_features: FeatureEntry[]
}

// /api/activation
export interface ActivationMoment {
  events: string[]
  min_frequency: number
  label: string
  window_days: number
  adoption_rate: number
  retention_adopters: number
  retention_non_adopters: number
  lift: number
  correlation: number
  p_value: number
  impact_score: number
}

export interface ActivationData {
  metric: string
  caveat: string
  total_users: number
  baseline_retention: number
  activation_window_days: number
  retention_period_days: number
  moments_found: number
  top_moments: ActivationMoment[]
}

// /api/churn
export interface ChurnUser {
  user_id: string
  health_score: number
  decay_stage: string
  matched_signals: string[]
  signal_details: Record<string, string>
  days_inactive: number
  is_at_risk: boolean
  action_window: string
  components: { activity: number; feature_breadth: number; recency: number }
}

export interface ChurnCohort {
  signal: string
  user_count: number
  avg_health_score: number
  description: string
}

export interface ChurnData {
  metric: string
  total_users: number
  at_risk_count: number
  at_risk_pct: number
  stage_distribution: Record<string, number>
  cohorts: ChurnCohort[]
  top_at_risk: ChurnUser[]
}
