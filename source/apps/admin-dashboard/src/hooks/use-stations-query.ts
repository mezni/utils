import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  listStations,
  getStation,
  createStation,
  updateStation,
  deleteStation,
} from "@bornemap/client-core";
import type { CreateStationRequest, UpdateStationRequest } from "@bornemap/domain-types";

const QUERY_KEY = "stations";

export function useStations(params?: { page?: number; partner_id?: string }) {
  return useQuery({
    queryKey: [QUERY_KEY, params],
    queryFn: () => listStations(params),
  });
}

export function useStation(id: string) {
  return useQuery({
    queryKey: [QUERY_KEY, id],
    queryFn: () => getStation(id),
    enabled: !!id,
  });
}

export function useCreateStation() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: CreateStationRequest) => createStation(data),
    onSuccess: () => qc.invalidateQueries({ queryKey: [QUERY_KEY] }),
  });
}

export function useUpdateStation(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: UpdateStationRequest) => updateStation(id, data),
    onSuccess: () => qc.invalidateQueries({ queryKey: [QUERY_KEY] }),
  });
}

export function useDeleteStation() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => deleteStation(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: [QUERY_KEY] }),
  });
}
