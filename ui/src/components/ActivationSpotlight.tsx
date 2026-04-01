import { Sparkles } from 'lucide-react'
import type { ActivationMoment } from '../lib/types'

interface Props {
  moment: ActivationMoment
  baselineRetention: number
}

export function ActivationSpotlight({ moment, baselineRetention }: Props) {
  return (
    <div className="rounded-lg border border-[var(--color-border)] bg-[var(--color-surface)] p-5">
      <div className="flex items-center gap-2 mb-2">
        <Sparkles className="w-4 h-4 text-yellow-500" />
        <h3 className="text-sm font-semibold text-[var(--color-text)]">Activation Moment Discovered</h3>
      </div>
      <p className="text-sm text-[var(--color-text)] mb-2">
        Users who do{' '}
        <span className="font-mono font-semibold bg-[var(--color-surface-hover)] px-1.5 py-0.5 rounded text-[var(--color-primary)]">
          {moment.label}
        </span>
        {' '}within {moment.window_days} days retain at{' '}
        <span className="font-bold text-green-600">{moment.lift.toFixed(1)}x</span>
        {' '}the base rate.
      </p>
      <div className="flex items-center gap-4 text-xs text-[var(--color-text-muted)]">
        <span>Adoption: {(moment.adoption_rate * 100).toFixed(0)}%</span>
        <span>·</span>
        <span>Headroom: {((1 - moment.adoption_rate) * 100).toFixed(0)}%</span>
        <span>·</span>
        <span>Baseline retention: {(baselineRetention * 100).toFixed(0)}%</span>
        <span>·</span>
        <span>Correlation: {moment.correlation.toFixed(3)}</span>
      </div>
    </div>
  )
}
