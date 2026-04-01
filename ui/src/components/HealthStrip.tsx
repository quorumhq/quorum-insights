import { TrendingUp, TrendingDown, AlertTriangle, Users } from 'lucide-react'
import type { OverviewData, ChurnData } from '../lib/types'

interface Props {
  overview?: OverviewData
  churn?: ChurnData
}

export function HealthStrip({ overview, churn }: Props) {
  return (
    <div className="flex items-center gap-6 px-5 py-3 rounded-lg bg-[var(--color-surface)] border border-[var(--color-border)] text-sm overflow-x-auto">
      <Metric
        icon={<Users className="w-4 h-4" />}
        label="DAU"
        value={overview?.avg_dau?.toLocaleString() ?? '—'}
      />
      <Sep />
      <Metric
        icon={<TrendingUp className="w-4 h-4" />}
        label="Users"
        value={overview?.total_users?.toLocaleString() ?? '—'}
      />
      <Sep />
      <Metric
        icon={<AlertTriangle className="w-4 h-4" />}
        label="Anomalies"
        value={String(overview?.anomaly_count ?? 0)}
        alert={overview?.anomaly_count ? overview.anomaly_count > 0 : false}
      />
      <Sep />
      <Metric
        icon={<TrendingDown className="w-4 h-4" />}
        label="At Risk"
        value={churn ? `${churn.at_risk_count} users` : '—'}
        alert={churn ? churn.at_risk_count > 0 : false}
      />
    </div>
  )
}

function Sep() {
  return <div className="w-px h-5 bg-[var(--color-border)] shrink-0" />
}

function Metric({ icon, label, value, alert }: {
  icon: React.ReactNode
  label: string
  value: string
  alert?: boolean
}) {
  return (
    <div className="flex items-center gap-2 shrink-0">
      <span className={alert ? 'text-[var(--color-severity-high)]' : 'text-[var(--color-text-muted)]'}>
        {icon}
      </span>
      <span className="text-[var(--color-text-muted)]">{label}:</span>
      <span className={`font-semibold ${alert ? 'text-[var(--color-severity-high)]' : 'text-[var(--color-text)]'}`}>
        {value}
      </span>
    </div>
  )
}
