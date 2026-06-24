import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useStations, useCreateStation } from '../hooks/useStations';
import { usePartners } from '../hooks/usePartners';
import { useKpis } from '../hooks/useDashboard';
import { DataTable } from '../components/ui/DataTable';
import { Button } from '../components/ui/Button';
import { StatusBadge } from '../components/ui/StatusBadge';
import { Input } from '../components/ui/Input';
import type { Column } from '../components/ui/DataTable';
import type { Station } from '../types/station';

const columns: Column<Station>[] = [
  { key: 'id', header: 'ID', render: (s) => <span className="font-mono text-xs">{s.id}</span> },
  { key: 'name', header: 'Name', render: (s) => <span className="font-medium">{s.name}</span> },
  { key: 'partner_id', header: 'Partner', render: (s) => <span className="font-mono text-xs text-gray-400">{s.partner_id}</span> },
  { key: 'location', header: 'Location', render: (s) => s.location ? <span className="text-gray-400 text-xs">{s.location}</span> : <span className="text-gray-600 text-xs">—</span> },
  { key: 'status', header: 'Status', render: (s) => <StatusBadge status={s.status} /> },
  { key: 'created_at', header: 'Created', render: (s) => <span className="text-gray-400 text-xs">{new Date(s.created_at).toLocaleDateString()}</span> },
];

export function StationsPage() {
  const navigate = useNavigate();
  const [page, setPage] = useState(1);
  const [showCreate, setShowCreate] = useState(false);
  const [name, setName] = useState('');
  const [location, setLocation] = useState('');
  const [partnerId, setPartnerId] = useState('');
  const { data, isLoading, isError, error, refetch } = useStations(page);
  const { data: partners } = usePartners(1, 100);
  const create = useCreateStation();
  const { refetch: refetchKpis } = useKpis();

  const handleCreate = async () => {
    if (!name.trim() || !partnerId.trim()) return;
    await create.mutateAsync({ name: name.trim(), location: location.trim() || undefined, partner_id: partnerId, status: 'ACTIVE' });
    setName(''); setLocation(''); setPartnerId('');
    setShowCreate(false);
    refetchKpis();
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white font-mono">Stations</h1>
          <p className="text-sm text-gray-400 mt-1">Charging station locations</p>
        </div>
        <Button onClick={() => setShowCreate(true)}>+ New Station</Button>
      </div>

      {showCreate && (
        <div className="bg-surface border border-gray-800 rounded-xl p-6 space-y-4">
          <h2 className="text-lg font-semibold text-white">Create Station</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <Input label="Station Name" value={name} onChange={(e) => setName(e.target.value)} placeholder="e.g. Downtown Hub" />
            <Input label="Partner ID" value={partnerId} onChange={(e) => setPartnerId(e.target.value)} placeholder="PRT-..." />
            <div className="md:col-span-2">
              <Input label="Location (optional)" value={location} onChange={(e) => setLocation(e.target.value)} placeholder="e.g. 123 Main St, City" />
            </div>
          </div>
          {partners && partners.items.length > 0 && (
            <div className="flex flex-wrap gap-2">
              <span className="text-xs text-gray-500 mr-1">Quick select:</span>
              {partners.items.slice(0, 5).map((p) => (
                <button key={p.id} onClick={() => setPartnerId(p.id)} className={`px-2 py-1 text-xs rounded border ${partnerId === p.id ? 'border-orange-500 bg-orange-500/10 text-orange-400' : 'border-gray-600 text-gray-400 hover:border-gray-500'}`}>{p.name}</button>
              ))}
            </div>
          )}
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
        onRowClick={(s) => navigate(`/stations/${s.id}`)}
        emptyTitle="No stations yet"
        emptyMessage="Create a station to start tracking charging locations."
        onCreate={() => setShowCreate(true)}
        onRefresh={refetch}
      />
    </div>
  );
}
