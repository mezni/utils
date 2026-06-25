import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  listPartners,
  getPartner,
  createPartner,
  updatePartner,
  deletePartner,
} from "@bornemap/client-core";
import type { CreatePartnerRequest, UpdatePartnerRequest } from "@bornemap/domain-types";

const QUERY_KEY = "partners";

export function usePartners(params?: { page?: number; search?: string }) {
  return useQuery({
    queryKey: [QUERY_KEY, params],
    queryFn: () => listPartners(params),
  });
}

export function usePartner(id: string) {
  return useQuery({
    queryKey: [QUERY_KEY, id],
    queryFn: () => getPartner(id),
    enabled: !!id,
  });
}

export function useCreatePartner() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: CreatePartnerRequest) => createPartner(data),
    onSuccess: () => qc.invalidateQueries({ queryKey: [QUERY_KEY] }),
  });
}

export function useUpdatePartner(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: UpdatePartnerRequest) => updatePartner(id, data),
    onSuccess: () => qc.invalidateQueries({ queryKey: [QUERY_KEY] }),
  });
}

export function useDeletePartner() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => deletePartner(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: [QUERY_KEY] }),
  });
}
