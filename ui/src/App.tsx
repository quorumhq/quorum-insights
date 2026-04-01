import { useState, useEffect } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { Sidebar } from './components/Sidebar'
import { HomeView } from './views/HomeView'
import { RetentionView } from './views/RetentionView'
import { FeaturesView } from './views/FeaturesView'
import { ActivationView } from './views/ActivationView'
import { ChurnView } from './views/ChurnView'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
})

const VIEW_TITLES: Record<string, string> = {
  home: 'Home',
  retention: 'Retention Analysis',
  features: 'Feature Correlations',
  activation: 'Activation Discovery',
  churn: 'Churn Detection',
}

function AppContent() {
  const [view, setView] = useState('home')
  const [dark, setDark] = useState(() => {
    if (typeof window !== 'undefined') {
      return localStorage.getItem('theme') === 'dark' ||
        (!localStorage.getItem('theme') && window.matchMedia('(prefers-color-scheme: dark)').matches)
    }
    return false
  })

  useEffect(() => {
    document.documentElement.classList.toggle('dark', dark)
    localStorage.setItem('theme', dark ? 'dark' : 'light')
  }, [dark])

  return (
    <div className="flex min-h-screen">
      <Sidebar
        current={view}
        onNavigate={setView}
        dark={dark}
        onToggleDark={() => setDark(!dark)}
      />
      <main className="flex-1 p-6 max-w-5xl">
        {view !== 'home' && (
          <h2 className="text-xl font-bold text-[var(--color-text)] mb-5">
            {VIEW_TITLES[view] || view}
          </h2>
        )}
        {view === 'home' && <HomeView onNavigate={setView} />}
        {view === 'retention' && <RetentionView />}
        {view === 'features' && <FeaturesView />}
        {view === 'activation' && <ActivationView />}
        {view === 'churn' && <ChurnView />}
      </main>
    </div>
  )
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <AppContent />
    </QueryClientProvider>
  )
}
