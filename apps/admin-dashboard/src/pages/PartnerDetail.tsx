import { useParams, useNavigate } from 'react-router-dom';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { getPartner, deletePartner } from '../api/partners';
import { usePatchPartner } from '../hooks/usePartners';
import { Button } from '../components/ui/Button';
import { StatusBadge } from '../components/ui/StatusBadge';
import { LoadingState } from '../components/ui/LoadingState';
import { ErrorState } from '../components/ui/ErrorState';
import { Input } from '../components/ui/Input';
import { useState } from 'react';

export function PartnerDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const qc = useQueryClient();
  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['partner', id],
    queryFn: () => getPartner(id!),
    enabled: !!id,
  });

  const [editing, setEditing] = useState(false);
  const [name, setName] = useState('');
  const [deleteConfirm, setDeleteConfirm] = useState(false);
  const patch = usePatchPartner();
  const [delError, setDelError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);

  if (isLoading) return <LoadingState />;
  if (isError) return <ErrorState message={error?.message} onRetry={refetch} />;
  if (!data) return <ErrorState message="Partner not found" />;

  const handleSave = async () => {
    if (!name.trim()) return;
    try {
      await patch.mutateAsync({ id: data.id, name: name.trim() });
      qc.invalidateQueries({ queryKey: ['partners'] });
      setEditing(false);
    } catch { /* error shown via patch.isError */ }
  };

  const handleDelete = async () => {
    setDeleting(true);
    setDelError(null);
    try {
      await deletePartner(data.id);
      qc.invalidateQueries({ queryKey: ['partners'] });
      navigate('/partners');
    } catch (e) {
      setDelError((e as Error).message);
      setDeleting(false);
    }
  };

  return (
    <div className="max-w-2xl space-y-6">
      <button onClick={() => navigate('/partners')} className="text-sm text-gray-400 hover:text-white transition-colors flex items-center gap-1">
        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" /></svg>
        Back to Partners
      </button>

      <div className="bg-surface border border-gray-800 rounded-xl p-6 space-y-6 border-l-2 border-l-orange-500">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-white font-mono">{data.id}</h1>
            <p className="text-sm text-gray-400 mt-1">{data.name}</p>
          </div>
          <StatusBadge status={data.status} />
        </div>

        <div className="space-y-4">
          <div className="grid grid-cols-2 gap-4 text-sm">
            <div><span className="text-gray-500">Name</span><p className="text-gray-300 mt-0.5">{data.name}</p></div>
            <div><span className="text-gray-500">Valid</span><p className="text-gray-300 mt-0.5">{data.is_valid ? 'Yes' : 'No'}</p></div>
            <div><span className="text-gray-500">Created</span><p className="text-gray-300 mt-0.5">{new Date(data.created_at).toLocaleString()}</p></div>
            <div><span className="text-gray-500">Updated</span><p className="text-gray-300 mt-0.5">{new Date(data.updated_at).toLocaleString()}</p></div>
          </div>
        </div>

        <div className="flex gap-3 pt-4 border-t border-gray-800">
          <Button variant="secondary" onClick={() => { setEditing(true); setName(data.name); }}>Edit</Button>
          <Button variant="danger" onClick={() => setDeleteConfirm(true)} disabled={deleting}>Delete</Button>
        </div>
      </div>

      {editing && (
        <div className="bg-surface border border-gray-800 rounded-xl p-6 space-y-4">
          <h2 className="text-lg font-semibold text-white">Edit Partner</h2>
          <Input label="Name" value={name} onChange={(e) => setName(e.target.value)} />
          {patch.isError && <p className="text-sm text-red-400">{patch.error?.message}</p>}
          <div className="flex gap-3">
            <Button onClick={handleSave} loading={patch.isPending}>Save</Button>
            <Button variant="secondary" onClick={() => setEditing(false)}>Cancel</Button>
          </div>
        </div>
      )}

      {deleteConfirm && (
        <div className="bg-red-500/5 border border-red-500/15 rounded-xl p-6 space-y-4 border-l-2 border-l-red-500">
          <p className="text-red-400 text-sm">Are you sure you want to delete {data.id}? This will also delete all associated stations and chargers.</p>
          {delError && <p className="text-sm text-red-400">{delError}</p>}
          <div className="flex gap-3">
            <Button variant="danger" onClick={handleDelete} loading={deleting}>Confirm Delete</Button>
            <Button variant="secondary" onClick={() => setDeleteConfirm(false)}>Cancel</Button>
          </div>
        </div>
      )}
    </div>
  );
}
