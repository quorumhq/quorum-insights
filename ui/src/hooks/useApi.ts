import { useQuery } from '@tanstack/react-query'
import type {
  OverviewData, InsightsData, RetentionData,
  FeaturesData, ActivationData, ChurnData,
} from '../lib/types'

const API = '/api'

async function fetchJson<T>(path: string): Promise<T> {
  const res = await fetch(`${API}${path}`)
  if (!res.ok) throw new Error(`API error: ${res.status}`)
  return res.json()
}

export function useOverview() {
  return useQuery<OverviewData>({
    queryKey: ['overview'],
    queryFn: () => fetchJson('/overview'),
    staleTime: 5 * 60_000,
  })
}

export function useInsights() {
  return useQuery<InsightsData>({
    queryKey: ['insights'],
    queryFn: () => fetchJson('/insights'),
    staleTime: 10 * 60_000,
  })
}

export function useRetention(periods = '1,7,30') {
  return useQuery<RetentionData>({
    queryKey: ['retention', periods],
    queryFn: () => fetchJson(`/retention?periods=${periods}`),
    staleTime: 10 * 60_000,
  })
}

export function useFeatures() {
  return useQuery<FeaturesData>({
    queryKey: ['features'],
    queryFn: () => fetchJson('/features'),
    staleTime: 10 * 60_000,
  })
}

export function useActivation() {
  return useQuery<ActivationData>({
    queryKey: ['activation'],
    queryFn: () => fetchJson('/activation'),
    staleTime: 10 * 60_000,
  })
}

export function useChurn() {
  return useQuery<ChurnData>({
    queryKey: ['churn'],
    queryFn: () => fetchJson('/churn'),
    staleTime: 10 * 60_000,
  })
}
