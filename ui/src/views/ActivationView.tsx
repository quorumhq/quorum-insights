import { useActivation } from '../hooks/useApi'
import { ChartSkeleton } from '../components/Skeleton'
import { AlertTriangle } from 'lucide-react'

export function ActivationView() {
  const { data, isLoading } = useActivation()

  if (isLoading) return <ChartSkeleton />

  if (!data || !data.top_moments?.length) {
    return (
      <div className="rounded-lg border border-[var(--color-border)] bg-[var(--color-surface)] p-8 text-center">
        <p className="text-sm text-[var(--color-text-muted)]">No activation moments discovered. Need more event diversity or users.</p>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {/* Caveat */}
      <div className="flex items-start gap-2 rounded-lg border border-yellow-300 bg-yellow-50 dark:bg-yellow-950/20 dark:border-yellow-800 px-4 py-3">
        <AlertTriangle className="w-4 h-4 text-yellow-600 shrink-0 mt-0.5" />
        <p className="text-xs text-yellow-800 dark:text-yellow-300">{data.caveat}</p>
      </div>

      {/* Summary */}
      <div className="flex gap-4">
        <Stat label="Moments discovered" value={data.moments_found} />
        <Stat label="Baseline retention" value={`${(data.baseline_retention * 100).toFixed(1)}%`} />
        <Stat label="Window" value={`${data.activation_window_days}d`} />
        <Stat label="Retention period" value={`D${data.retention_period_days}`} />
      </div>

      {/* Moments table */}
      <div className="rounded-lg border border-[var(--color-border)] bg-[var(--color-surface)] overflow-x-auto">
        <div className="p-4 border-b border-[var(--color-border)]">
          <h3 className="text-sm font-semibold text-[var(--color-text)]">Discovered Activation Moments</h3>
          <p className="text-xs text-[var(--color-text-muted)] mt-0.5">Ranked by Matthews Correlation Coefficient</p>
        </div>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[var(--color-border)]">
              <th className="text-left px-4 py-2 text-xs font-medium text-[var(--color-text-muted)]">#</th>
              <th className="text-left px-4 py-2 text-xs font-medium text-[var(--color-text-muted)]">Activation Pattern</th>
              <th className="text-right px-4 py-2 text-xs font-medium text-[var(--color-text-muted)]">Lift</th>
              <th className="text-right px-4 py-2 text-xs font-medium text-[var(--color-text-muted)]">Adoption</th>
              <th className="text-right px-4 py-2 text-xs font-medium text-[var(--color-text-muted)]">Ret (adopters)</th>
              <th className="text-right px-4 py-2 text-xs font-medium text-[var(--color-text-muted)]">Ret (others)</th>
              <th className="text-right px-4 py-2 text-xs font-medium text-[var(--color-text-muted)]">MCC</th>
              <th className="text-right px-4 py-2 text-xs font-medium text-[var(--color-text-muted)]">p-value</th>
            </tr>
          </thead>
          <tbody>
            {data.top_moments.map((m, i) => (
              <tr key={i} className="border-b border-[var(--color-border)] last:border-0">
                <td className="px-4 py-2.5 text-xs text-[var(--color-text-dim)]">{i + 1}</td>
                <td className="px-4 py-2.5">
                  <span className="font-mono text-xs font-semibold text-[var(--color-primary)]">{m.label}</span>
                </td>
                <td className="px-4 py-2.5 text-right font-bold text-green-600">{m.lift.toFixed(1)}x</td>
                <td className="px-4 py-2.5 text-right text-[var(--color-text)]">{(m.adoption_rate * 100).toFixed(0)}%</td>
                <td className="px-4 py-2.5 text-right text-green-600">{(m.retention_adopters * 100).toFixed(1)}%</td>
                <td className="px-4 py-2.5 text-right text-[var(--color-text-muted)]">{(m.retention_non_adopters * 100).toFixed(1)}%</td>
                <td className="px-4 py-2.5 text-right font-medium text-[var(--color-text)]">{m.correlation.toFixed(3)}</td>
                <td className="px-4 py-2.5 text-right text-xs text-[var(--color-text-dim)]">{m.p_value < 0.001 ? '<0.001' : m.p_value.toFixed(3)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function Stat({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="rounded-lg border border-[var(--color-border)] bg-[var(--color-surface)] px-4 py-3">
      <div className="text-xl font-bold text-[var(--color-text)]">{value}</div>
      <div className="text-xs text-[var(--color-text-muted)] mt-0.5">{label}</div>
    </div>
  )
}
