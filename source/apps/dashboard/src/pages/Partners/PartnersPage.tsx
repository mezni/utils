import { useEffect, useState } from 'react';
import { DataTable, type Column } from '../../components/shared/DataTable';
import { Button } from '../../components/shared/Button';
import { Modal } from '../../components/shared/Modal';
import { Input } from '../../components/shared/Input';
import { EmptyState } from '../../components/shared/EmptyState';
import { ErrorState } from '../../components/shared/ErrorState';
import { Skeleton } from '../../components/shared/Skeleton';
import { list, create, update, remove } from '../../api/client';

interface Partner {
  id: string;
  name: string;
  type: string;
  is_verified: boolean;
  is_live: boolean;
  is_active: boolean;
}

const partnerTypes = ['business', 'personal'] as const;

export function PartnersPage() {
  const [data, setData] = useState<Partner[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [modalOpen, setModalOpen] = useState(false);
  const [editItem, setEditItem] = useState<Partner | null>(null);
  const [deleteItem, setDeleteItem] = useState<Partner | null>(null);
  const [form, setForm] = useState({ name: '', type: 'business' });
  const [formErrors, setFormErrors] = useState<Record<string, string>>({});

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      setData(await list<Partner>('partners'));
    } catch {
      setError('Failed to load partners');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchData(); }, []);

  const openAdd = () => {
    setEditItem(null);
    setForm({ name: '', type: 'business' });
    setFormErrors({});
    setModalOpen(true);
  };

  const openEdit = (item: Partner) => {
    setEditItem(item);
    setForm({ name: item.name, type: item.type });
    setFormErrors({});
    setModalOpen(true);
  };

  const validate = () => {
    const errs: Record<string, string> = {};
    if (!form.name.trim()) errs.name = 'Name is required';
    setFormErrors(errs);
    return Object.keys(errs).length === 0;
  };

  const handleSubmit = async () => {
    if (!validate()) return;
    try {
      if (editItem) {
        await update<Partner>('partners', editItem.id, form);
      } else {
        await create<Partner>('partners', form);
      }
      setModalOpen(false);
      fetchData();
    } catch {
      setFormErrors({ name: 'Failed to save. Try again.' });
    }
  };

  const handleVerify = async (item: Partner) => {
    await update<Partner>('partners', item.id, { is_verified: true } as Partial<Partner>);
    fetchData();
  };

  const handleToggleActive = async (item: Partner) => {
    await update<Partner>('partners', item.id, { is_active: !item.is_active } as Partial<Partner>);
    fetchData();
  };

  const handleDelete = async () => {
    if (!deleteItem) return;
    await remove('partners', deleteItem.id);
    setDeleteItem(null);
    fetchData();
  };

  if (loading) return <div className="space-y-3">{Array.from({ length: 3 }).map((_, i) => <Skeleton key={i} className="h-12" />)}</div>;
  if (error) return <ErrorState message={error} onRetry={fetchData} />;

  const columns: Column<Partner>[] = [
    { key: 'name', label: 'Name' },
    {
      key: 'type', label: 'Type',
      render: (p) => (
        <span className={`rounded-full px-2 py-0.5 text-xs font-medium ${p.type === 'business' ? 'bg-blue-100 text-blue-700' : 'bg-purple-100 text-purple-700'}`}>
          {p.type}
        </span>
      ),
    },
    {
      key: 'is_verified', label: 'Verified',
      render: (p) => (p.is_verified ? <span className="text-status-available">✓ Verified</span> : <span className="text-muted">✗ Not verified</span>),
    },
    {
      key: 'is_live', label: 'Live',
      render: (p) => (p.is_live ? <span className="text-status-available">✓ Live</span> : <span className="text-muted">—</span>),
    },
    {
      key: 'is_active', label: 'Active',
      render: (p) => (
        <button
          onClick={() => handleToggleActive(p)}
          className={`rounded px-2 py-0.5 text-xs font-medium ${p.is_active ? 'bg-status-available-bg text-status-available' : 'bg-status-maintenance-bg text-status-maintenance'}`}
        >
          {p.is_active ? 'Active' : 'Inactive'}
        </button>
      ),
    },
  ];

  return (
    <div>
      <div className="mb-4 flex items-center justify-between">
        <h2 className="text-base font-semibold text-main">All Partners</h2>
        <Button onClick={openAdd}>Add Partner</Button>
      </div>

      {data.length === 0 ? (
        <EmptyState message="No partners yet" actionLabel="Add your first partner" onAction={openAdd} />
      ) : (
        <DataTable
          columns={columns}
          data={data}
          keyExtractor={(p) => p.id}
          actions={(p) => (
            <div className="flex gap-2">
              {!p.is_verified && <Button variant="secondary" onClick={() => handleVerify(p)}>Verify</Button>}
              <Button variant="ghost" onClick={() => openEdit(p)}>Edit</Button>
              <Button variant="danger" onClick={() => setDeleteItem(p)}>Delete</Button>
            </div>
          )}
        />
      )}

      <Modal isOpen={modalOpen} onClose={() => setModalOpen(false)} title={editItem ? 'Edit Partner' : 'Add Partner'}>
        <div className="space-y-4">
          <Input label="Name" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} error={formErrors.name} placeholder="Partner name" />
          <div className="flex flex-col gap-1">
            <label className="text-sm font-medium text-main">Type</label>
            <select value={form.type} onChange={(e) => setForm({ ...form, type: e.target.value })} className="rounded-lg border border-default px-3 py-2 text-sm outline-none">
              {partnerTypes.map((t) => <option key={t} value={t}>{t.charAt(0).toUpperCase() + t.slice(1)}</option>)}
            </select>
          </div>
          <div className="flex justify-end gap-2">
            <Button variant="secondary" onClick={() => setModalOpen(false)}>Cancel</Button>
            <Button onClick={handleSubmit}>{editItem ? 'Save' : 'Create'}</Button>
          </div>
        </div>
      </Modal>

      <Modal isOpen={!!deleteItem} onClose={() => setDeleteItem(null)} title="Delete Partner">
        <p className="mb-4 text-sm text-muted">Are you sure you want to delete <strong>{deleteItem?.name}</strong>? This action cannot be undone.</p>
        <div className="flex justify-end gap-2">
          <Button variant="secondary" onClick={() => setDeleteItem(null)}>Cancel</Button>
          <Button variant="danger" onClick={handleDelete}>Delete</Button>
        </div>
      </Modal>
    </div>
  );
}
