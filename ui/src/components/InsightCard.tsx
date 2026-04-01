import { ThumbsUp, ThumbsDown, ArrowRight } from 'lucide-react'
import type { InsightCard as InsightCardType } from '../lib/types'
import { cn } from '../lib/cn'

const SEVERITY_STYLES: Record<string, { border: string; badge: string; bg: string; label: string }> = {
  critical: { border: 'border-l-red-600', badge: 'bg-red-600 text-white', bg: 'bg-red-50 dark:bg-red-950/30', label: 'CRITICAL' },
  high:     { border: 'border-l-orange-500', badge: 'bg-orange-500 text-white', bg: 'bg-orange-50 dark:bg-orange-950/30', label: 'HIGH' },
  medium:   { border: 'border-l-yellow-500', badge: 'bg-yellow-500 text-white', bg: 'bg-yellow-50 dark:bg-yellow-950/20', label: 'MEDIUM' },
  low:      { border: 'border-l-green-500', badge: 'bg-green-600 text-white', bg: 'bg-green-50 dark:bg-green-950/20', label: 'LOW' },
  info:     { border: 'border-l-gray-400', badge: 'bg-gray-500 text-white', bg: 'bg-gray-50 dark:bg-gray-800/50', label: 'INFO' },
}

interface Props {
  card: InsightCardType
  rank: number
  onViewEvidence?: () => void
  onFeedback?: (vote: 'useful' | 'not_useful') => void
}

export function InsightCard({ card, onViewEvidence, onFeedback }: Props) {
  const style = SEVERITY_STYLES[card.severity] || SEVERITY_STYLES.info

  return (
    <div className={cn(
      'rounded-lg border-l-4 border border-[var(--color-border)]',
      style.border, style.bg,
      'p-5 transition-shadow hover:shadow-md',
    )}>
      {/* Header: badge + title */}
      <div className="flex items-start gap-3 mb-3">
        <span className={cn('text-[11px] font-bold px-2 py-0.5 rounded tracking-wider shrink-0', style.badge)}>
          {style.label}
        </span>
        <h3 className="text-[15px] font-semibold text-[var(--color-text)] leading-snug">
          {card.title}
        </h3>
      </div>

      {/* Finding */}
      <p className="text-sm text-[var(--color-text)] leading-relaxed mb-2">
        {card.finding}
      </p>

      {/* Evidence */}
      <p className="text-xs text-[var(--color-text-muted)] mb-3">
        <span className="font-medium">Evidence:</span> {card.evidence}
      </p>

      {/* Action */}
      <p className="text-sm font-medium text-[var(--color-primary)] mb-3">
        <ArrowRight className="inline w-3.5 h-3.5 mr-1" />
        {card.action}
      </p>

      {/* Estimated impact */}
      {card.estimated_impact && (
        <p className="text-xs text-[var(--color-text-muted)] mb-3 italic">
          Impact: {card.estimated_impact}
        </p>
      )}

      {/* Footer: confidence + actions */}
      <div className="flex items-center justify-between pt-3 border-t border-[var(--color-border)]">
        <span className="text-xs text-[var(--color-text-dim)]">
          Confidence: {Math.round(card.confidence * 100)}% · {card.category}
        </span>
        <div className="flex items-center gap-2">
          {onViewEvidence && (
            <button
              onClick={onViewEvidence}
              className="text-xs text-[var(--color-primary)] hover:underline"
            >
              View evidence
            </button>
          )}
          {onFeedback && (
            <div className="flex gap-1 ml-2">
              <button
                onClick={() => onFeedback('useful')}
                className="p-1 rounded hover:bg-[var(--color-surface-hover)] text-[var(--color-text-muted)]"
                title="Useful"
              >
                <ThumbsUp className="w-3.5 h-3.5" />
              </button>
              <button
                onClick={() => onFeedback('not_useful')}
                className="p-1 rounded hover:bg-[var(--color-surface-hover)] text-[var(--color-text-muted)]"
                title="Not useful"
              >
                <ThumbsDown className="w-3.5 h-3.5" />
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
