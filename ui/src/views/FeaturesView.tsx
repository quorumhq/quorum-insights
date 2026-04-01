import { useFeatures } from '../hooks/useApi'
import { ChartSkeleton } from '../components/Skeleton'
import { AlertTriangle } from 'lucide-react'

export function FeaturesView() {
  const { data, isLoading } = useFeatures()

  if (isLoading) return <ChartSkeleton />

  if (!data || !data.top_features?.length) {
    return (
      <div className="rounded-lg border border-[var(--color-border)] bg-[var(--color-surface)] p-8 text-center">
        <p className="text-sm text-[var(--color-text-muted)]">Not enough data to compute feature correlations.</p>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {/* Caveat banner */}
      <div className="flex items-start gap-2 rounded-lg border border-yellow-300 bg-yellow-50 dark:bg-yellow-950/20 dark:border-yellow-800 px-4 py-3">
        <AlertTriangle className="w-4 h-4 text-yellow-600 shrink-0 mt-0.5" />
        <p className="text-xs text-yellow-800 dark:text-yellow-300">
          {data.caveat || 'Correlations, not causal. Features associated with retention may not cause it. Use A/B tests to validate.'}
        </p>
      </div>

      {/* Summary */}
      <div className="flex gap-4">
        <Stat label="Features analyzed" value={data.total_features} />
        <Stat label="Positive correlation" value={data.positive_count} color="text-green-600" />
        <Stat label="Negative correlation" value={data.negative_count} color="text-red-600" />
      </div>

      {/* Feature ranking table */}
      <div className="rounded-lg border border-[var(--color-border)] bg-[var(--color-surface)] overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[var(--color-border)]">
              <th className="text-left px-4 py-3 text-xs font-medium text-[var(--color-text-muted)]">Feature</th>
              <th className="text-right px-4 py-3 text-xs font-medium text-[var(--color-text-muted)]">Users</th>
              {Object.keys(data.top_features[0]?.impact || {}).map((period) => (
                <th key={period} className="text-right px-4 py-3 text-xs font-medium text-[var(--color-text-muted)]">
                  {period} Impact
                </th>
              ))}
              <th className="text-right px-4 py-3 text-xs font-medium text-[var(--color-text-muted)]">Net Score</th>
            </tr>
          </thead>
          <tbody>
            {data.top_features.map((feat) => {
              const isNegative = feat.net_score < -0.01
              return (
                <tr
                  key={feat.name}
                  className={`border-b border-[var(--color-border)] last:border-0 ${
                    isNegative ? 'bg-red-50 dark:bg-red-950/10' : ''
                  }`}
                >
                  <td className="px-4 py-2.5 font-medium text-[var(--color-text)]">
                    {feat.name}
                    {isNegative && (
                      <span className="ml-2 text-[10px] font-bold text-red-600 bg-red-100 dark:bg-red-900 px-1.5 py-0.5 rounded">
                        NEGATIVE
                      </span>
                    )}
                  </td>
                  <td className="px-4 py-2.5 text-right text-[var(--color-text-muted)]">{feat.users}</td>
                  {Object.entries(feat.impact).map(([period, val]) => (
                    <td
                      key={period}
                      className={`px-4 py-2.5 text-right font-medium ${
                        val > 0 ? 'text-green-600' : val < 0 ? 'text-red-600' : 'text-[var(--color-text-muted)]'
                      }`}
                    >
                      {val > 0 ? '+' : ''}{(val * 100).toFixed(1)}%
                    </td>
                  ))}
                  <td className={`px-4 py-2.5 text-right font-bold ${
                    feat.net_score > 0 ? 'text-green-600' : feat.net_score < 0 ? 'text-red-600' : 'text-[var(--color-text-muted)]'
                  }`}>
                    {feat.net_score > 0 ? '+' : ''}{feat.net_score.toFixed(3)}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function Stat({ label, value, color }: { label: string; value: number; color?: string }) {
  return (
    <div className="rounded-lg border border-[var(--color-border)] bg-[var(--color-surface)] px-4 py-3">
      <div className={`text-xl font-bold ${color || 'text-[var(--color-text)]'}`}>{value}</div>
      <div className="text-xs text-[var(--color-text-muted)] mt-0.5">{label}</div>
    </div>
  )
}
