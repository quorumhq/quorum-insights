import { useChurn } from '../hooks/useApi'
import { ChartSkeleton } from '../components/Skeleton'

const STAGE_COLORS: Record<string, string> = {
  thriving: 'bg-green-500',
  coasting: 'bg-yellow-400',
  fading: 'bg-orange-500',
  ghosting: 'bg-red-500',
  gone: 'bg-gray-600',
}

const STAGE_LABELS: Record<string, string> = {
  thriving: 'Thriving',
  coasting: 'Coasting',
  fading: 'Fading',
  ghosting: 'Ghosting',
  gone: 'Gone',
}

export function ChurnView() {
  const { data, isLoading } = useChurn()

  if (isLoading) return <ChartSkeleton />

  if (!data || data.total_users === 0) {
    return (
      <div className="rounded-lg border border-[var(--color-border)] bg-[var(--color-surface)] p-8 text-center">
        <p className="text-sm text-[var(--color-text-muted)]">No user data available for churn analysis.</p>
      </div>
    )
  }

  const stages = ['thriving', 'coasting', 'fading', 'ghosting', 'gone']
  const total = data.total_users

  return (
    <div className="space-y-5">
      {/* Summary cards */}
      <div className="flex gap-4">
        <StatCard label="Total Users" value={data.total_users} />
        <StatCard label="At Risk" value={data.at_risk_count} color="text-red-600" />
        <StatCard label="At Risk %" value={`${(data.at_risk_pct * 100).toFixed(1)}%`} color="text-red-600" />
      </div>

      {/* Decay stage distribution bar */}
      <div className="rounded-lg border border-[var(--color-border)] bg-[var(--color-surface)] p-4">
        <h3 className="text-sm font-semibold text-[var(--color-text)] mb-3">Behavioral Decay Stages</h3>
        <div className="flex h-8 rounded-md overflow-hidden">
          {stages.map((stage) => {
            const count = data.stage_distribution[stage] || 0
            const pct = total > 0 ? (count / total) * 100 : 0
            if (pct === 0) return null
            return (
              <div
                key={stage}
                className={`${STAGE_COLORS[stage]} flex items-center justify-center text-[10px] font-bold text-white`}
                style={{ width: `${pct}%` }}
                title={`${STAGE_LABELS[stage]}: ${count} users (${pct.toFixed(1)}%)`}
              >
                {pct > 8 ? `${STAGE_LABELS[stage]} ${count}` : ''}
              </div>
            )
          })}
        </div>
        <div className="flex gap-4 mt-2">
          {stages.map((stage) => (
            <div key={stage} className="flex items-center gap-1.5 text-xs text-[var(--color-text-muted)]">
              <div className={`w-2.5 h-2.5 rounded-sm ${STAGE_COLORS[stage]}`} />
              {STAGE_LABELS[stage]}: {data.stage_distribution[stage] || 0}
            </div>
          ))}
        </div>
      </div>

      {/* Cohort alerts */}
      {data.cohorts.length > 0 && (
        <div className="space-y-3">
          <h3 className="text-sm font-semibold text-[var(--color-text)]">Signal Cohorts</h3>
          {data.cohorts.map((cohort, i) => (
            <div
              key={i}
              className="rounded-lg border border-[var(--color-border)] bg-[var(--color-surface)] px-4 py-3 flex items-center justify-between"
            >
              <div>
                <p className="text-sm font-medium text-[var(--color-text)]">{cohort.description}</p>
                <p className="text-xs text-[var(--color-text-muted)] mt-0.5">
                  Signal: <span className="font-mono">{cohort.signal}</span>
                </p>
              </div>
              <div className="text-right shrink-0 ml-4">
                <p className="text-lg font-bold text-[var(--color-severity-high)]">{cohort.user_count}</p>
                <p className="text-xs text-[var(--color-text-muted)]">avg health: {cohort.avg_health_score.toFixed(0)}</p>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* At-risk user table */}
      {data.top_at_risk.length > 0 && (
        <div className="rounded-lg border border-[var(--color-border)] bg-[var(--color-surface)] overflow-x-auto">
          <div className="p-4 border-b border-[var(--color-border)]">
            <h3 className="text-sm font-semibold text-[var(--color-text)]">Most At-Risk Users</h3>
          </div>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[var(--color-border)]">
                <th className="text-left px-4 py-2 text-xs font-medium text-[var(--color-text-muted)]">User</th>
                <th className="text-center px-4 py-2 text-xs font-medium text-[var(--color-text-muted)]">Health</th>
                <th className="text-center px-4 py-2 text-xs font-medium text-[var(--color-text-muted)]">Stage</th>
                <th className="text-left px-4 py-2 text-xs font-medium text-[var(--color-text-muted)]">Signals</th>
                <th className="text-right px-4 py-2 text-xs font-medium text-[var(--color-text-muted)]">Inactive</th>
                <th className="text-right px-4 py-2 text-xs font-medium text-[var(--color-text-muted)]">Window</th>
              </tr>
            </thead>
            <tbody>
              {data.top_at_risk.map((user) => (
                <tr key={user.user_id} className="border-b border-[var(--color-border)] last:border-0">
                  <td className="px-4 py-2.5 font-mono text-xs text-[var(--color-text)]">{user.user_id}</td>
                  <td className="px-4 py-2.5 text-center">
                    <HealthBadge score={user.health_score} />
                  </td>
                  <td className="px-4 py-2.5 text-center">
                    <span className={`text-[10px] font-bold px-2 py-0.5 rounded ${STAGE_COLORS[user.decay_stage]} text-white`}>
                      {STAGE_LABELS[user.decay_stage] || user.decay_stage}
                    </span>
                  </td>
                  <td className="px-4 py-2.5 text-xs text-[var(--color-text-muted)]">
                    {user.matched_signals.join(', ')}
                  </td>
                  <td className="px-4 py-2.5 text-right text-xs text-[var(--color-text)]">{user.days_inactive}d</td>
                  <td className="px-4 py-2.5 text-right text-xs text-[var(--color-text-muted)]">{user.action_window}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function StatCard({ label, value, color }: { label: string; value: string | number; color?: string }) {
  return (
    <div className="rounded-lg border border-[var(--color-border)] bg-[var(--color-surface)] px-5 py-3">
      <div className={`text-2xl font-bold ${color || 'text-[var(--color-text)]'}`}>{value}</div>
      <div className="text-xs text-[var(--color-text-muted)] mt-1">{label}</div>
    </div>
  )
}

function HealthBadge({ score }: { score: number }) {
  const color = score >= 70 ? 'text-green-600' : score >= 40 ? 'text-yellow-600' : 'text-red-600'
  const bg = score >= 70 ? 'bg-green-50 dark:bg-green-950/30' : score >= 40 ? 'bg-yellow-50 dark:bg-yellow-950/30' : 'bg-red-50 dark:bg-red-950/30'
  return (
    <span className={`text-xs font-bold px-2 py-0.5 rounded ${color} ${bg}`}>
      {score.toFixed(0)}
    </span>
  )
}
