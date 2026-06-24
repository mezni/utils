import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listPartners, createPartner, updatePartner, patchPartner, deletePartner } from '../api/partners';
import type { CreatePartnerRequest } from '../types/partner';

export function usePartners(page = 1, limit = 50) {
  return useQuery({
    queryKey: ['partners', { page, limit }],
    queryFn: () => listPartners(page, limit),
  });
}

export function useCreatePartner() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: CreatePartnerRequest) => createPartner(req),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['partners'] }),
  });
}

export function useUpdatePartner(id: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: Partial<CreatePartnerRequest>) => updatePartner(id, req),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['partners'] }); qc.invalidateQueries({ queryKey: ['partner', id] }); },
  });
}

export function usePatchPartner() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, name }: { id: string; name: string }) => patchPartner(id, name),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['partners'] }); },
  });
}

export function useDeletePartner() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => deletePartner(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['partners'] }),
  });
}
