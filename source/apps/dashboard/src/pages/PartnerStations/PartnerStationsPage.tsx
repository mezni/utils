import { useEffect, useState } from 'react';
import { useRole } from '../../context/RoleContext';
import { DataTable, type Column } from '../../components/shared/DataTable';
import { Button } from '../../components/shared/Button';
import { Modal } from '../../components/shared/Modal';
import { Input } from '../../components/shared/Input';
import { EmptyState } from '../../components/shared/EmptyState';
import { ErrorState } from '../../components/shared/ErrorState';
import { Skeleton } from '../../components/shared/Skeleton';
import { list, create, update, remove } from '../../api/client';

interface Station {
  id: string;
  partner_id: string;
  name: string;
  address: string;
  latitude: number;
  longitude: number;
}

export function PartnerStationsPage() {
  const { selectedPartnerId } = useRole();
  const [data, setData] = useState<Station[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [modalOpen, setModalOpen] = useState(false);
  const [editItem, setEditItem] = useState<Station | null>(null);
  const [deleteItem, setDeleteItem] = useState<Station | null>(null);
  const [form, setForm] = useState({ name: '', address: '', latitude: '', longitude: '' });
  const [formErrors, setFormErrors] = useState<Record<string, string>>({});

  const fetchData = async () => {
    if (!selectedPartnerId) { setLoading(false); return; }
    setLoading(true);
    setError(null);
    try {
      setData(await list<Station>('stations', { partner_id: selectedPartnerId }));
    } catch {
      setError('Failed to load stations');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchData(); }, [selectedPartnerId]);

  if (!selectedPartnerId) {
    return (
      <div className="flex items-center justify-center py-16">
        <p className="text-muted">Please select a partner from the sidebar dropdown.</p>
      </div>
    );
  }

  const openAdd = () => {
    setEditItem(null);
    setForm({ name: '', address: '', latitude: '', longitude: '' });
    setFormErrors({});
    setModalOpen(true);
  };

  const openEdit = (item: Station) => {
    setEditItem(item);
    setForm({ name: item.name, address: item.address || '', latitude: String(item.latitude), longitude: String(item.longitude) });
    setFormErrors({});
    setModalOpen(true);
  };

  const validate = () => {
    const errs: Record<string, string> = {};
    if (!form.name.trim()) errs.name = 'Name is required';
    const lat = Number(form.latitude);
    const lng = Number(form.longitude);
    if (isNaN(lat) || lat < -90 || lat > 90) errs.latitude = 'Latitude must be between -90 and 90';
    if (isNaN(lng) || lng < -180 || lng > 180) errs.longitude = 'Longitude must be between -180 and 180';
    setFormErrors(errs);
    return Object.keys(errs).length === 0;
  };

  const handleSubmit = async () => {
    if (!validate()) return;
    const body = { name: form.name, address: form.address, latitude: Number(form.latitude), longitude: Number(form.longitude), partner_id: selectedPartnerId };
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

  const columns: Column<Station>[] = [
    { key: 'name', label: 'Name' },
    { key: 'address', label: 'Address' },
  ];

  return (
    <div>
      <div className="mb-4 flex items-center justify-between">
        <h2 className="text-base font-semibold text-main">My Stations</h2>
        <Button onClick={openAdd}>Add Station</Button>
      </div>

      {data.length === 0 ? (
        <EmptyState message="No stations yet" actionLabel="Add your first station" onAction={openAdd} />
      ) : (
        <DataTable
          columns={columns}
          data={data}
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
            <label className="text-sm font-medium text-main">Partner ID</label>
            <input
              value={selectedPartnerId}
              disabled
              className="rounded-lg border border-default bg-neutral-50 px-3 py-2 text-sm text-muted outline-none"
            />
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
