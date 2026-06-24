import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listChargers, createCharger, updateCharger, patchCharger, deleteCharger } from '../api/chargers';
import type { CreateChargerRequest } from '../types/charger';

export function useChargers(page = 1, limit = 50, stationId?: string) {
  return useQuery({
    queryKey: ['chargers', { page, limit, stationId }],
    queryFn: () => listChargers(page, limit, stationId),
  });
}

export function useCreateCharger() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: CreateChargerRequest) => createCharger(req),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['chargers'] }),
  });
}

export function useUpdateCharger(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: Partial<CreateChargerRequest>) => updateCharger(id, req),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['chargers'] }); qc.invalidateQueries({ queryKey: ['charger', id] }); },
  });
}

export function usePatchCharger() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, powerRating }: { id: string; powerRating: number }) => patchCharger(id, powerRating),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['chargers'] }); },
  });
}

export function useDeleteCharger() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => deleteCharger(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['chargers'] }),
  });
}
