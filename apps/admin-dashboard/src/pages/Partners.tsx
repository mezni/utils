import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { usePartners, useCreatePartner } from '../hooks/usePartners';
import { useKpis } from '../hooks/useDashboard';
import { DataTable } from '../components/ui/DataTable';
import { Button } from '../components/ui/Button';
import { StatusBadge } from '../components/ui/StatusBadge';
import { Input } from '../components/ui/Input';
import type { Column } from '../components/ui/DataTable';
import type { Partner } from '../types/partner';

const columns: Column<Partner>[] = [
  { key: 'id', header: 'ID', render: (p) => <span className="font-mono text-xs">{p.id}</span> },
  { key: 'name', header: 'Name', render: (p) => <span className="font-medium">{p.name}</span> },
  { key: 'status', header: 'Status', render: (p) => <StatusBadge status={p.status} /> },
  { key: 'is_valid', header: 'Valid', render: (p) => p.is_valid ? <span className="text-green-400 text-xs">✓</span> : <span className="text-red-400 text-xs">✗</span> },
  { key: 'created_at', header: 'Created', render: (p) => <span className="text-gray-400 text-xs">{new Date(p.created_at).toLocaleDateString()}</span> },
];

export function PartnersPage() {
  const navigate = useNavigate();
  const [page, setPage] = useState(1);
  const [showCreate, setShowCreate] = useState(false);
  const [name, setName] = useState('');
  const { data, isLoading, isError, error, refetch } = usePartners(page);
  const create = useCreatePartner();
  const { refetch: refetchKpis } = useKpis();

  const handleCreate = async () => {
    if (!name.trim()) return;
    await create.mutateAsync({ name: name.trim(), status: 'ACTIVE', is_valid: true });
    setName('');
    setShowCreate(false);
    refetchKpis();
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white font-mono">Partners</h1>
          <p className="text-sm text-gray-400 mt-1">EV network operator organizations</p>
        </div>
        <Button onClick={() => setShowCreate(true)}>+ New Partner</Button>
      </div>

      {showCreate && (
        <div className="bg-surface border border-gray-800 rounded-xl p-6 space-y-4">
          <h2 className="text-lg font-semibold text-white">Create Partner</h2>
          <div className="max-w-md">
            <Input label="Organization Name" value={name} onChange={(e) => setName(e.target.value)} placeholder="e.g. GreenCharge Networks" helperText="Alphanumeric, spaces, and hyphens only" />
          </div>
          <div className="flex gap-3">
            <Button onClick={handleCreate} loading={create.isPending}>Create</Button>
            <Button variant="secondary" onClick={() => setShowCreate(false)}>Cancel</Button>
          </div>
          {create.isError && <p className="text-sm text-red-400">{create.error?.message}</p>}
        </div>
      )}

      <DataTable
        columns={columns}
        data={data?.items}
        isLoading={isLoading}
        isError={isError}
        error={error}
        page={page}
        pages={data?.pagination?.pages ?? 1}
        onPageChange={setPage}
        onRowClick={(p) => navigate(`/partners/${p.id}`)}
        emptyTitle="No partners yet"
        emptyMessage="Create your first partner organization to get started."
        onCreate={() => setShowCreate(true)}
        onRefresh={refetch}
      />
    </div>
  );
}
