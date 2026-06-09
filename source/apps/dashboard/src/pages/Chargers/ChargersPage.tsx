import { useEffect, useState } from 'react';
import { DataTable, type Column } from '../../components/shared/DataTable';
import { StatusBadge } from '../../components/shared/StatusBadge';
import { Button } from '../../components/shared/Button';
import { Modal } from '../../components/shared/Modal';
import { Input } from '../../components/shared/Input';
import { EmptyState } from '../../components/shared/EmptyState';
import { ErrorState } from '../../components/shared/ErrorState';
import { Skeleton } from '../../components/shared/Skeleton';
import { list, create, update, remove } from '../../api/client';

interface Station { id: string; name: string; }
interface Charger { id: string; station_id: string; connector_type: string; power_kw: number; status: string; }

interface ChargerRow extends Charger { stationName: string; }

const connectorTypes = ['type2', 'ccs', 'chademo', 'type1'] as const;
const statuses = ['available', 'in_use', 'maintenance', 'offline'] as const;

export function ChargersPage() {
  const [data, setData] = useState<ChargerRow[]>([]);
  const [stations, setStations] = useState<Station[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState('');
  const [modalOpen, setModalOpen] = useState(false);
  const [editItem, setEditItem] = useState<Charger | null>(null);
  const [deleteItem, setDeleteItem] = useState<Charger | null>(null);
  const [form, setForm] = useState({ station_id: '', connector_type: 'type2', power_kw: '', status: 'available' });
  const [formErrors, setFormErrors] = useState<Record<string, string>>({});

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const [chargers, allStations] = await Promise.all([
        list<Charger>('chargers'),
        list<Station>('stations'),
      ]);
      setStations(allStations);
      const stationMap = Object.fromEntries(allStations.map((s) => [s.id, s.name]));
      setData(chargers.map((c) => ({ ...c, stationName: stationMap[c.station_id] || 'Unknown' })));
    } catch {
      setError('Failed to load chargers');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchData(); }, []);

  const filtered = filter ? data.filter((c) => c.station_id === filter) : data;

  const openAdd = () => {
    setEditItem(null);
    setForm({ station_id: '', connector_type: 'type2', power_kw: '', status: 'available' });
    setFormErrors({});
    setModalOpen(true);
  };

  const openEdit = (item: Charger) => {
    setEditItem(item);
    setForm({ station_id: item.station_id, connector_type: item.connector_type, power_kw: String(item.power_kw), status: item.status });
    setFormErrors({});
    setModalOpen(true);
  };

  const validate = () => {
    const errs: Record<string, string> = {};
    if (!form.station_id) errs.station_id = 'Station is required';
    if (!form.power_kw || isNaN(Number(form.power_kw)) || Number(form.power_kw) <= 0) errs.power_kw = 'Must be a positive number';
    setFormErrors(errs);
    return Object.keys(errs).length === 0;
  };

  const handleSubmit = async () => {
    if (!validate()) return;
    const body = { station_id: form.station_id, connector_type: form.connector_type, power_kw: Number(form.power_kw), status: form.status };
    try {
      if (editItem) await update('chargers', editItem.id, body);
      else await create('chargers', body);
      setModalOpen(false);
      fetchData();
    } catch {
      setFormErrors({ power_kw: 'Failed to save. Try again.' });
    }
  };

  const handleDelete = async () => {
    if (!deleteItem) return;
    await remove('chargers', deleteItem.id);
    setDeleteItem(null);
    fetchData();
  };

  if (loading) return <Skeleton className="h-64" />;
  if (error) return <ErrorState message={error} onRetry={fetchData} />;

  const columns: Column<ChargerRow>[] = [
    { key: 'stationName', label: 'Station' },
    { key: 'connector_type', label: 'Connector', render: (c) => <span className="capitalize">{c.connector_type.replace('type', 'Type ')}</span> },
    { key: 'power_kw', label: 'Power (kW)' },
    { key: 'status', label: 'Status', render: (c) => <StatusBadge status={c.status} /> },
  ];

  return (
    <div>
      <div className="mb-4 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h2 className="text-base font-semibold text-main">All Chargers</h2>
          <select value={filter} onChange={(e) => setFilter(e.target.value)} className="rounded border border-default px-2 py-1 text-sm">
            <option value="">All stations</option>
            {stations.map((s) => <option key={s.id} value={s.id}>{s.name}</option>)}
          </select>
        </div>
        <Button onClick={openAdd}>Add Charger</Button>
      </div>

      {filtered.length === 0 ? (
        <EmptyState message="No chargers found" actionLabel="Add your first charger" onAction={openAdd} />
      ) : (
        <DataTable
          columns={columns}
          data={filtered}
          keyExtractor={(c) => c.id}
          actions={(c) => (
            <div className="flex gap-2">
              <Button variant="ghost" onClick={() => openEdit(c)}>Edit</Button>
              <Button variant="danger" onClick={() => setDeleteItem(c)}>Delete</Button>
            </div>
          )}
        />
      )}

      <Modal isOpen={modalOpen} onClose={() => setModalOpen(false)} title={editItem ? 'Edit Charger' : 'Add Charger'}>
        <div className="space-y-4">
          <div className="flex flex-col gap-1">
            <label className="text-sm font-medium text-main">Station</label>
            <select value={form.station_id} onChange={(e) => setForm({ ...form, station_id: e.target.value })} className="rounded-lg border border-default px-3 py-2 text-sm outline-none">
              <option value="">Select station...</option>
              {stations.map((s) => <option key={s.id} value={s.id}>{s.name}</option>)}
            </select>
            {formErrors.station_id && <span className="text-xs text-status-maintenance">{formErrors.station_id}</span>}
          </div>
          <div className="flex flex-col gap-1">
            <label className="text-sm font-medium text-main">Connector Type</label>
            <select value={form.connector_type} onChange={(e) => setForm({ ...form, connector_type: e.target.value })} className="rounded-lg border border-default px-3 py-2 text-sm outline-none">
              {connectorTypes.map((t) => <option key={t} value={t}>{t.replace('type', 'Type ').toUpperCase()}</option>)}
            </select>
          </div>
          <Input label="Power (kW)" type="number" value={form.power_kw} onChange={(e) => setForm({ ...form, power_kw: e.target.value })} error={formErrors.power_kw} placeholder="e.g. 22" />
          <div className="flex flex-col gap-1">
            <label className="text-sm font-medium text-main">Status</label>
            <select value={form.status} onChange={(e) => setForm({ ...form, status: e.target.value })} className="rounded-lg border border-default px-3 py-2 text-sm outline-none">
              {statuses.map((s) => <option key={s} value={s}>{s.replace(/_/g, ' ')}</option>)}
            </select>
          </div>
          <div className="flex justify-end gap-2">
            <Button variant="secondary" onClick={() => setModalOpen(false)}>Cancel</Button>
            <Button onClick={handleSubmit}>{editItem ? 'Save' : 'Create'}</Button>
          </div>
        </div>
      </Modal>

      <Modal isOpen={!!deleteItem} onClose={() => setDeleteItem(null)} title="Delete Charger">
        <p className="mb-4 text-sm text-muted">Are you sure you want to delete this charger?</p>
        <div className="flex justify-end gap-2">
          <Button variant="secondary" onClick={() => setDeleteItem(null)}>Cancel</Button>
          <Button variant="danger" onClick={handleDelete}>Delete</Button>
        </div>
      </Modal>
    </div>
  );
}
