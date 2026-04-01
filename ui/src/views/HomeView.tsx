import { useOverview, useInsights, useActivation, useChurn } from '../hooks/useApi'
import { HealthStrip } from '../components/HealthStrip'
import { InsightCard } from '../components/InsightCard'
import { ActivationSpotlight } from '../components/ActivationSpotlight'
import { CardSkeleton } from '../components/Skeleton'

interface Props {
  onNavigate: (view: string) => void
}

export function HomeView({ onNavigate }: Props) {
  const overview = useOverview()
  const insights = useInsights()
  const activation = useActivation()
  const churn = useChurn()

  const cards = insights.data?.cards ?? []
  const topMoment = activation.data?.top_moments?.[0]

  return (
    <div className="space-y-5">
      {/* Health strip */}
      <HealthStrip overview={overview.data} churn={churn.data} />

      {/* AI Insight Cards — primary content */}
      <div>
        <h2 className="text-lg font-semibold text-[var(--color-text)] mb-3">
          AI Insights
          {cards.length > 0 && (
            <span className="text-sm font-normal text-[var(--color-text-muted)] ml-2">
              {cards.length} finding{cards.length !== 1 ? 's' : ''}
            </span>
          )}
        </h2>

        {insights.isLoading ? (
          <div className="space-y-4">
            <CardSkeleton />
            <CardSkeleton />
            <CardSkeleton />
          </div>
        ) : cards.length > 0 ? (
          <div className="space-y-4">
            {cards.map((card, i) => (
              <InsightCard
                key={i}
                card={card}
                rank={i + 1}
                onViewEvidence={() => {
                  const viewMap: Record<string, string> = {
                    retention: 'retention',
                    anomaly: 'home',
                    feature_correlation: 'features',
                    overview: 'home',
                  }
                  onNavigate(viewMap[card.category] ?? 'home')
                }}
                onFeedback={(vote) => {
                  fetch(`/api/feedback?insight_rank=${i + 1}&vote=${vote}`, { method: 'POST' })
                }}
              />
            ))}
          </div>
        ) : (
          <div className="rounded-lg border border-[var(--color-border)] bg-[var(--color-surface)] p-8 text-center">
            <p className="text-3xl mb-2">✅</p>
            <h3 className="text-base font-semibold text-[var(--color-text)] mb-1">
              No significant findings this period
            </h3>
            <p className="text-sm text-[var(--color-text-muted)]">
              All metrics are within normal ranges. Retention is stable, no anomalies detected.
            </p>
            {insights.data?.note && (
              <p className="text-xs text-[var(--color-text-dim)] mt-3 italic">
                {insights.data.note}
              </p>
            )}
          </div>
        )}
      </div>

      {/* Activation spotlight */}
      {topMoment && activation.data && (
        <ActivationSpotlight
          moment={topMoment}
          baselineRetention={activation.data.baseline_retention}
        />
      )}
    </div>
  )
}
