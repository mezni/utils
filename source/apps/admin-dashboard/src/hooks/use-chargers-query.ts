import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  listChargers,
  getCharger,
  createCharger,
  updateCharger,
  deleteCharger,
} from "@bornemap/client-core";
import type { CreateChargerRequest, UpdateChargerRequest } from "@bornemap/domain-types";

const QUERY_KEY = "chargers";

export function useChargers(params?: { page?: number; station_id?: string }) {
  return useQuery({
    queryKey: [QUERY_KEY, params],
    queryFn: () => listChargers(params),
  });
}

export function useCharger(id: string) {
  return useQuery({
    queryKey: [QUERY_KEY, id],
    queryFn: () => getCharger(id),
    enabled: !!id,
  });
}

export function useCreateCharger() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: CreateChargerRequest) => createCharger(data),
    onSuccess: () => qc.invalidateQueries({ queryKey: [QUERY_KEY] }),
  });
}

export function useUpdateCharger(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: UpdateChargerRequest) => updateCharger(id, data),
    onSuccess: () => qc.invalidateQueries({ queryKey: [QUERY_KEY] }),
  });
}

export function useDeleteCharger() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => deleteCharger(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: [QUERY_KEY] }),
  });
}
