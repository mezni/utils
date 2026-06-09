import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { get, list, type Station, type Charger } from '../api/client';
import { ChargerRow } from '../components/ChargerRow';

export function StationDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [station, setStation] = useState<Station | null>(null);
  const [chargers, setChargers] = useState<Charger[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchData = async () => {
    if (!id) return;
    setLoading(true);
    setError(null);
    try {
      const [stationData, chargersData] = await Promise.all([
        get<Station>('stations', id),
        list<Charger>('chargers', { station_id: id }),
      ]);
      setStation(stationData);
      setChargers(chargersData);
    } catch {
      setError('Failed to load station details.');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchData(); }, [id]);

  const goBack = () => {
    navigate(-1);
  };

  if (loading) {
    return (
      <div className="flex h-full items-center justify-center bg-surface-background">
        <p className="text-sm text-muted">Loading station details...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-3 bg-surface-background p-6">
        <p className="text-sm text-status-maintenance">{error}</p>
        <button
          onClick={fetchData}
          className="rounded-lg bg-brand-primary px-4 py-2 text-sm font-medium text-white hover:bg-brand-primaryDark"
        >
          Retry
        </button>
      </div>
    );
  }

  if (!station) return null;

  return (
    <div className="flex min-h-full flex-col bg-surface-background">
      <div className="flex items-center gap-3 border-b border-default bg-white px-4 py-3">
        <button
          onClick={goBack}
          className="text-sm font-medium text-muted hover:text-main"
          aria-label="Back to map"
        >
          &larr; Back
        </button>
      </div>

      <div className="flex-1 p-4">
        <h1 className="text-lg font-bold text-main">{station.name}</h1>
        <p className="mt-1 text-sm text-muted">{station.address}</p>

        <h2 className="mb-3 mt-6 text-sm font-semibold text-main">Chargers</h2>
        {chargers.length === 0 ? (
          <p className="text-sm text-muted">No chargers at this station.</p>
        ) : (
          <div className="space-y-2">
            {chargers.map(ch => (
              <ChargerRow key={ch.id} charger={ch} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
