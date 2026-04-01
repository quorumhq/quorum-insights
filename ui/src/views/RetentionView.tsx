import { useRetention } from '../hooks/useApi'
import { ChartSkeleton } from '../components/Skeleton'

export function RetentionView() {
  const { data, isLoading } = useRetention()

  if (isLoading) return <ChartSkeleton />

  if (!data || !data.cohorts?.length) {
    return (
      <div className="rounded-lg border border-[var(--color-border)] bg-[var(--color-surface)] p-8 text-center">
        <p className="text-sm text-[var(--color-text-muted)]">No retention data available for this period.</p>
      </div>
    )
  }

  const periods = Object.keys(data.overall_retention)

  return (
    <div className="space-y-5">
      {/* Overall retention summary */}
      <div className="flex gap-4">
        {periods.map((p) => (
          <div key={p} className="rounded-lg border border-[var(--color-border)] bg-[var(--color-surface)] px-5 py-3 text-center">
            <div className="text-2xl font-bold text-[var(--color-text)]">
              {(data.overall_retention[p] * 100).toFixed(1)}%
            </div>
            <div className="text-xs text-[var(--color-text-muted)] mt-1">{p} Retention</div>
          </div>
        ))}
        <div className="rounded-lg border border-[var(--color-border)] bg-[var(--color-surface)] px-5 py-3 text-center">
          <div className="text-2xl font-bold text-[var(--color-text)]">{data.total_users.toLocaleString()}</div>
          <div className="text-xs text-[var(--color-text-muted)] mt-1">Total Users</div>
        </div>
      </div>

      {/* Cohort retention matrix */}
      <div className="rounded-lg border border-[var(--color-border)] bg-[var(--color-surface)] overflow-x-auto">
        <div className="p-4 border-b border-[var(--color-border)]">
          <h3 className="text-sm font-semibold text-[var(--color-text)]">Cohort Retention Matrix</h3>
        </div>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[var(--color-border)]">
              <th className="text-left px-4 py-2 text-xs font-medium text-[var(--color-text-muted)]">Cohort</th>
              <th className="text-right px-4 py-2 text-xs font-medium text-[var(--color-text-muted)]">Size</th>
              {periods.map((p) => (
                <th key={p} className="text-center px-4 py-2 text-xs font-medium text-[var(--color-text-muted)]">{p}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.cohorts.map((cohort) => (
              <tr key={cohort.key} className="border-b border-[var(--color-border)] last:border-0">
                <td className="px-4 py-2 text-xs text-[var(--color-text)]">{cohort.key}</td>
                <td className="px-4 py-2 text-xs text-right text-[var(--color-text-muted)]">{cohort.size}</td>
                {periods.map((p) => {
                  const val = cohort.retention[p]
                  if (val === undefined) return <td key={p} />
                  const pct = val * 100
                  // Blue intensity heatmap: 0% = transparent, 100% = full blue
                  const alpha = Math.min(val, 1)
                  return (
                    <td
                      key={p}
                      className="px-4 py-2 text-xs text-center font-medium"
                      style={{
                        backgroundColor: `rgba(59, 130, 246, ${alpha * 0.5})`,
                        color: alpha > 0.35 ? 'white' : 'var(--color-text)',
                      }}
                    >
                      {pct.toFixed(1)}%
                    </td>
                  )
                })}
              </tr>
            ))}
            {/* Mean row */}
            <tr className="bg-[var(--color-surface-hover)] font-semibold">
              <td className="px-4 py-2 text-xs text-[var(--color-text)]">Mean</td>
              <td className="px-4 py-2 text-xs text-right text-[var(--color-text-muted)]">—</td>
              {periods.map((p) => (
                <td key={p} className="px-4 py-2 text-xs text-center text-[var(--color-text)]">
                  {(data.overall_retention[p] * 100).toFixed(1)}%
                </td>
              ))}
            </tr>
          </tbody>
        </table>
      </div>

      {/* Best/worst cohort callout */}
      {(data.best_cohort || data.worst_cohort) && (
        <div className="grid grid-cols-2 gap-4">
          {data.best_cohort && (
            <div className="rounded-lg border border-green-200 bg-green-50 dark:bg-green-950/20 dark:border-green-900 p-4">
              <p className="text-xs font-medium text-green-700 dark:text-green-400 mb-1">Best Cohort</p>
              <p className="text-sm font-semibold text-[var(--color-text)]">{data.best_cohort.key}</p>
              <p className="text-xs text-[var(--color-text-muted)]">{data.best_cohort.dimension}</p>
            </div>
          )}
          {data.worst_cohort && (
            <div className="rounded-lg border border-red-200 bg-red-50 dark:bg-red-950/20 dark:border-red-900 p-4">
              <p className="text-xs font-medium text-red-700 dark:text-red-400 mb-1">Worst Cohort</p>
              <p className="text-sm font-semibold text-[var(--color-text)]">{data.worst_cohort.key}</p>
              <p className="text-xs text-[var(--color-text-muted)]">{data.worst_cohort.dimension}</p>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
