import { useParams, useNavigate } from 'react-router-dom';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { getCharger, deleteCharger } from '../api/chargers';
import { usePatchCharger } from '../hooks/useChargers';
import { Button } from '../components/ui/Button';
import { StatusBadge } from '../components/ui/StatusBadge';
import { LoadingState } from '../components/ui/LoadingState';
import { ErrorState } from '../components/ui/ErrorState';
import { Input } from '../components/ui/Input';
import { useState } from 'react';

export function ChargerDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const qc = useQueryClient();
  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['charger', id],
    queryFn: () => getCharger(id!),
    enabled: !!id,
  });

  const [editing, setEditing] = useState(false);
  const [powerRating, setPowerRating] = useState('');
  const [deleteConfirm, setDeleteConfirm] = useState(false);
  const patch = usePatchCharger();
  const [delError, setDelError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);

  if (isLoading) return <LoadingState />;
  if (isError) return <ErrorState message={error?.message} onRetry={refetch} />;
  if (!data) return <ErrorState message="Charger not found" />;

  const handleSave = async () => {
    const rating = parseInt(powerRating, 10);
    if (isNaN(rating) || rating < 1 || rating > 1000) return;
    try {
      await patch.mutateAsync({ id: data.id, powerRating: rating });
      qc.invalidateQueries({ queryKey: ['chargers'] });
      setEditing(false);
    } catch { /* error shown via patch.isError */ }
  };

  const handleDelete = async () => {
    setDeleting(true);
    setDelError(null);
    try {
      await deleteCharger(data.id);
      qc.invalidateQueries({ queryKey: ['chargers'] });
      navigate('/chargers');
    } catch (e) {
      setDelError((e as Error).message);
      setDeleting(false);
    }
  };

  return (
    <div className="max-w-2xl space-y-6">
      <button onClick={() => navigate('/chargers')} className="text-sm text-gray-400 hover:text-white transition-colors flex items-center gap-1">
        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" /></svg>
        Back to Chargers
      </button>

      <div className="bg-surface border border-gray-800 rounded-xl p-6 space-y-6 border-l-2 border-l-blue-500">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-white font-mono">{data.id}</h1>
            <p className="text-sm text-gray-400 mt-1">{data.power_rating} kW Charger</p>
          </div>
          <StatusBadge status={data.status} />
        </div>

        <div className="space-y-4">
          <div className="grid grid-cols-2 gap-4 text-sm">
            <div><span className="text-gray-500">Station</span><p className="font-mono text-gray-300 mt-0.5 text-xs">{data.station_id}</p></div>
            <div><span className="text-gray-500">Power Rating</span><p className="font-mono text-gray-300 mt-0.5">{data.power_rating} kW</p></div>
            <div><span className="text-gray-500">Status</span><p className="text-gray-300 mt-0.5">{data.status}</p></div>
            <div><span className="text-gray-500">Created</span><p className="text-gray-300 mt-0.5">{new Date(data.created_at).toLocaleString()}</p></div>
          </div>
        </div>

        <div className="flex gap-3 pt-4 border-t border-gray-800">
          <Button variant="secondary" onClick={() => { setEditing(true); setPowerRating(String(data.power_rating)); }}>Edit</Button>
          <Button variant="danger" onClick={() => setDeleteConfirm(true)} disabled={deleting}>Delete</Button>
        </div>
      </div>

      {editing && (
        <div className="bg-surface border border-gray-800 rounded-xl p-6 space-y-4">
          <h2 className="text-lg font-semibold text-white">Edit Charger</h2>
          <Input label="Power Rating (kW)" type="number" value={powerRating} onChange={(e) => setPowerRating(e.target.value)} />
          {patch.isError && <p className="text-sm text-red-400">{patch.error?.message}</p>}
          <div className="flex gap-3">
            <Button onClick={handleSave} loading={patch.isPending}>Save</Button>
            <Button variant="secondary" onClick={() => setEditing(false)}>Cancel</Button>
          </div>
        </div>
      )}

      {deleteConfirm && (
        <div className="bg-red-500/5 border border-red-500/15 rounded-xl p-6 space-y-4 border-l-2 border-l-red-500">
          <p className="text-red-400 text-sm">Are you sure you want to delete charger {data.id}?</p>
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
