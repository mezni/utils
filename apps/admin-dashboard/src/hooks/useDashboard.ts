import { useQuery } from '@tanstack/react-query';
import { fetchKpis } from '../api/dashboard';
import type { KpiData } from '../types/common';

export function useKpis() {
  return useQuery<KpiData>({
    queryKey: ['dashboard', 'kpis'],
    queryFn: fetchKpis,
    refetchInterval: 30_000,
  });
}
