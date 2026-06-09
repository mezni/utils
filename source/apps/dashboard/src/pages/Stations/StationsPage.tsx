import { useEffect, useState } from 'react';
import { DataTable, type Column } from '../../components/shared/DataTable';
import { Button } from '../../components/shared/Button';
import { Modal } from '../../components/shared/Modal';
import { Input } from '../../components/shared/Input';
import { EmptyState } from '../../components/shared/EmptyState';
import { ErrorState } from '../../components/shared/ErrorState';
import { Skeleton } from '../../components/shared/Skeleton';
import { list, create, update, remove } from '../../api/client';

interface Partner { id: string; name: string; }
interface Station { id: string; partner_id: string; name: string; address: string; latitude: number; longitude: number; }
interface Charger { id: string; station_id: string; }

interface StationRow extends Station { partnerName: string; chargerCount: number; }

export function StationsPage() {
  const [data, setData] = useState<StationRow[]>([]);
  const [partners, setPartners] = useState<Partner[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState('');
  const [modalOpen, setModalOpen] = useState(false);
  const [editItem, setEditItem] = useState<Station | null>(null);
  const [deleteItem, setDeleteItem] = useState<Station | null>(null);
  const [form, setForm] = useState({ name: '', address: '', latitude: '', longitude: '', partner_id: '' });
  const [formErrors, setFormErrors] = useState<Record<string, string>>({});

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const [stations, chargers, allPartners] = await Promise.all([
        list<Station>('stations'),
        list<Charger>('chargers'),
        list<Partner>('partners'),
      ]);
      setPartners(allPartners);
      const partnerMap = Object.fromEntries(allPartners.map((p) => [p.id, p.name]));
      const chargerCountMap: Record<string, number> = {};
      for (const ch of chargers) chargerCountMap[ch.station_id] = (chargerCountMap[ch.station_id] || 0) + 1;
      setData(stations.map((s) => ({ ...s, partnerName: partnerMap[s.partner_id] || 'Unknown', chargerCount: chargerCountMap[s.id] || 0 })));
    } catch {
      setError('Failed to load stations');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchData(); }, []);

  const filtered = filter ? data.filter((s) => s.partner_id === filter) : data;

  const openAdd = () => {
    setEditItem(null);
    setForm({ name: '', address: '', latitude: '', longitude: '', partner_id: '' });
    setFormErrors({});
    setModalOpen(true);
  };

  const openEdit = (item: Station) => {
    setEditItem(item);
    setForm({ name: item.name, address: item.address || '', latitude: String(item.latitude), longitude: String(item.longitude), partner_id: item.partner_id });
    setFormErrors({});
    setModalOpen(true);
  };

  const validate = () => {
    const errs: Record<string, string> = {};
    if (!form.name.trim()) errs.name = 'Name is required';
    if (!form.partner_id) errs.partner_id = 'Partner is required';
    const lat = Number(form.latitude);
    const lng = Number(form.longitude);
    if (isNaN(lat) || lat < -90 || lat > 90) errs.latitude = 'Latitude must be between -90 and 90';
    if (isNaN(lng) || lng < -180 || lng > 180) errs.longitude = 'Longitude must be between -180 and 180';
    setFormErrors(errs);
    return Object.keys(errs).length === 0;
  };

  const handleSubmit = async () => {
    if (!validate()) return;
    const body = { name: form.name, address: form.address, latitude: Number(form.latitude), longitude: Number(form.longitude), partner_id: form.partner_id };
    try {
      if (editItem) await update('stations', editItem.id, body);
      else await create('stations', body);
      setModalOpen(false);
      fetchData();
    } catch {
      setFormErrors({ name: 'Failed to save. Try again.' });
    }
  };

  const handleDelete = async () => {
    if (!deleteItem) return;
    await remove('stations', deleteItem.id);
    setDeleteItem(null);
    fetchData();
  };

  if (loading) return <Skeleton className="h-64" />;
  if (error) return <ErrorState message={error} onRetry={fetchData} />;

  const columns: Column<StationRow>[] = [
    { key: 'name', label: 'Name' },
    { key: 'address', label: 'Address' },
    { key: 'partnerName', label: 'Partner' },
    { key: 'chargerCount', label: 'Chargers' },
  ];

  return (
    <div>
      <div className="mb-4 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h2 className="text-base font-semibold text-main">All Stations</h2>
          <select value={filter} onChange={(e) => setFilter(e.target.value)} className="rounded border border-default px-2 py-1 text-sm">
            <option value="">All partners</option>
            {partners.map((p) => <option key={p.id} value={p.id}>{p.name}</option>)}
          </select>
        </div>
        <Button onClick={openAdd}>Add Station</Button>
      </div>

      {filtered.length === 0 ? (
        <EmptyState message="No stations found" actionLabel="Add your first station" onAction={openAdd} />
      ) : (
        <DataTable
          columns={columns}
          data={filtered}
          keyExtractor={(s) => s.id}
          actions={(s) => (
            <div className="flex gap-2">
              <Button variant="ghost" onClick={() => openEdit(s)}>Edit</Button>
              <Button variant="danger" onClick={() => setDeleteItem(s)}>Delete</Button>
            </div>
          )}
        />
      )}

      <Modal isOpen={modalOpen} onClose={() => setModalOpen(false)} title={editItem ? 'Edit Station' : 'Add Station'}>
        <div className="space-y-4">
          <Input label="Name" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} error={formErrors.name} placeholder="Station name" />
          <Input label="Address" value={form.address} onChange={(e) => setForm({ ...form, address: e.target.value })} placeholder="Street address" />
          <div className="grid grid-cols-2 gap-3">
            <Input label="Latitude" value={form.latitude} onChange={(e) => setForm({ ...form, latitude: e.target.value })} error={formErrors.latitude} placeholder="-90 to 90" />
            <Input label="Longitude" value={form.longitude} onChange={(e) => setForm({ ...form, longitude: e.target.value })} error={formErrors.longitude} placeholder="-180 to 180" />
          </div>
          <div className="flex flex-col gap-1">
            <label className="text-sm font-medium text-main">Partner</label>
            <select value={form.partner_id} onChange={(e) => setForm({ ...form, partner_id: e.target.value })} className="rounded-lg border border-default px-3 py-2 text-sm outline-none">
              <option value="">Select partner...</option>
              {partners.map((p) => <option key={p.id} value={p.id}>{p.name}</option>)}
            </select>
            {formErrors.partner_id && <span className="text-xs text-status-maintenance">{formErrors.partner_id}</span>}
          </div>
          <div className="flex justify-end gap-2">
            <Button variant="secondary" onClick={() => setModalOpen(false)}>Cancel</Button>
            <Button onClick={handleSubmit}>{editItem ? 'Save' : 'Create'}</Button>
          </div>
        </div>
      </Modal>

      <Modal isOpen={!!deleteItem} onClose={() => setDeleteItem(null)} title="Delete Station">
        <p className="mb-4 text-sm text-muted">Are you sure you want to delete <strong>{deleteItem?.name}</strong>?</p>
        <div className="flex justify-end gap-2">
          <Button variant="secondary" onClick={() => setDeleteItem(null)}>Cancel</Button>
          <Button variant="danger" onClick={handleDelete}>Delete</Button>
        </div>
      </Modal>
    </div>
  );
}
