import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listStations, createStation, updateStation, patchStation, deleteStation } from '../api/stations';
import type { CreateStationRequest } from '../types/station';

export function useStations(page = 1, limit = 50, partnerId?: string) {
  return useQuery({
    queryKey: ['stations', { page, limit, partnerId }],
    queryFn: () => listStations(page, limit, partnerId),
  });
}

export function useCreateStation() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: CreateStationRequest) => createStation(req),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['stations'] }),
  });
}

export function useUpdateStation(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: Partial<CreateStationRequest>) => updateStation(id, req),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['stations'] }); qc.invalidateQueries({ queryKey: ['station', id] }); },
  });
}

export function usePatchStation() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, name, location }: { id: string; name: string; location?: string }) => patchStation(id, name, location),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['stations'] }); },
  });
}

export function useDeleteStation() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => deleteStation(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['stations'] }),
  });
}
