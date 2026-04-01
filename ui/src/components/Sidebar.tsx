import { Home, BarChart3, Star, Target, AlertTriangle, Moon, Sun } from 'lucide-react'
import { cn } from '../lib/cn'

interface Props {
  current: string
  onNavigate: (view: string) => void
  dark: boolean
  onToggleDark: () => void
}

const NAV = [
  { id: 'home', label: 'Home', icon: Home },
  { id: 'retention', label: 'Retention', icon: BarChart3 },
  { id: 'features', label: 'Features', icon: Star },
  { id: 'activation', label: 'Activation', icon: Target },
  { id: 'churn', label: 'Churn', icon: AlertTriangle },
]

export function Sidebar({ current, onNavigate, dark, onToggleDark }: Props) {
  return (
    <aside className="w-52 shrink-0 h-screen sticky top-0 flex flex-col border-r border-[var(--color-border)] bg-[var(--color-surface)]">
      {/* Logo */}
      <div className="px-4 py-5 border-b border-[var(--color-border)]">
        <h1 className="text-base font-bold text-[var(--color-text)]">
          Quorum <span className="text-[var(--color-primary)]">Insights</span>
        </h1>
        <p className="text-[11px] text-[var(--color-text-dim)] mt-0.5">AI-Powered Analytics</p>
      </div>

      {/* Nav */}
      <nav className="flex-1 py-3 px-2">
        {NAV.map(({ id, label, icon: Icon }) => (
          <button
            key={id}
            onClick={() => onNavigate(id)}
            className={cn(
              'w-full flex items-center gap-2.5 px-3 py-2 rounded-md text-sm transition-colors mb-0.5',
              current === id
                ? 'bg-[var(--color-primary)] text-white font-medium'
                : 'text-[var(--color-text-muted)] hover:bg-[var(--color-surface-hover)] hover:text-[var(--color-text)]',
            )}
          >
            <Icon className="w-4 h-4" />
            {label}
          </button>
        ))}
      </nav>

      {/* Footer */}
      <div className="px-3 py-3 border-t border-[var(--color-border)]">
        <button
          onClick={onToggleDark}
          className="flex items-center gap-2 text-xs text-[var(--color-text-muted)] hover:text-[var(--color-text)] transition-colors"
        >
          {dark ? <Sun className="w-3.5 h-3.5" /> : <Moon className="w-3.5 h-3.5" />}
          {dark ? 'Light mode' : 'Dark mode'}
        </button>
      </div>
    </aside>
  )
}
