import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useChargers, useCreateCharger } from '../hooks/useChargers';
import { useStations } from '../hooks/useStations';
import { useKpis } from '../hooks/useDashboard';
import { DataTable } from '../components/ui/DataTable';
import { Button } from '../components/ui/Button';
import { StatusBadge } from '../components/ui/StatusBadge';
import { Input } from '../components/ui/Input';
import type { Column } from '../components/ui/DataTable';
import type { Charger } from '../types/charger';

const columns: Column<Charger>[] = [
  { key: 'id', header: 'ID', render: (c) => <span className="font-mono text-xs">{c.id}</span> },
  { key: 'station_id', header: 'Station', render: (c) => <span className="font-mono text-xs text-gray-400">{c.station_id}</span> },
  { key: 'status', header: 'Status', render: (c) => <StatusBadge status={c.status} /> },
  { key: 'power_rating', header: 'Power', render: (c) => <span className="font-mono text-sm tabular-nums">{c.power_rating} kW</span>, className: 'text-right' },
  { key: 'created_at', header: 'Created', render: (c) => <span className="text-gray-400 text-xs">{new Date(c.created_at).toLocaleDateString()}</span> },
];

export function ChargersPage() {
  const navigate = useNavigate();
  const [page, setPage] = useState(1);
  const [showCreate, setShowCreate] = useState(false);
  const [stationId, setStationId] = useState('');
  const [powerRating, setPowerRating] = useState('');
  const { data, isLoading, isError, error, refetch } = useChargers(page);
  const { data: stations } = useStations(1, 100);
  const create = useCreateCharger();
  const { refetch: refetchKpis } = useKpis();

  const handleCreate = async () => {
    if (!stationId.trim() || !powerRating.trim()) return;
    const rating = parseInt(powerRating, 10);
    if (isNaN(rating) || rating < 1 || rating > 1000) return;
    await create.mutateAsync({ station_id: stationId, status: 'ACTIVE', power_rating: rating });
    setStationId(''); setPowerRating('');
    setShowCreate(false);
    refetchKpis();
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white font-mono">Chargers</h1>
          <p className="text-sm text-gray-400 mt-1">Charging units</p>
        </div>
        <Button onClick={() => setShowCreate(true)}>+ New Charger</Button>
      </div>

      {showCreate && (
        <div className="bg-surface border border-gray-800 rounded-xl p-6 space-y-4">
          <h2 className="text-lg font-semibold text-white">Create Charger</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <Input label="Station ID" value={stationId} onChange={(e) => setStationId(e.target.value)} placeholder="STA-..." />
            <Input label="Power Rating (kW)" type="number" value={powerRating} onChange={(e) => setPowerRating(e.target.value)} placeholder="e.g. 150" helperText="1–1000 kW" />
          </div>
          {stations && stations.items.length > 0 && (
            <div className="flex flex-wrap gap-2">
              <span className="text-xs text-gray-500 mr-1">Quick select:</span>
              {stations.items.slice(0, 5).map((s) => (
                <button key={s.id} onClick={() => setStationId(s.id)} className={`px-2 py-1 text-xs rounded border ${stationId === s.id ? 'border-orange-500 bg-orange-500/10 text-orange-400' : 'border-gray-600 text-gray-400 hover:border-gray-500'}`}>{s.name}</button>
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
        onRowClick={(c) => navigate(`/chargers/${c.id}`)}
        emptyTitle="No chargers yet"
        emptyMessage="Create a charger to start tracking charging units."
        onCreate={() => setShowCreate(true)}
        onRefresh={refetch}
      />
    </div>
  );
}
