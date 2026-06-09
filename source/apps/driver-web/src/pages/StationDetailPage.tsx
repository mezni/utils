import { useEffect, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { Button } from '@/components/ui/button';
import { get, list, type Charger, type Station } from '../api/client';
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
      <div className="flex h-full items-center justify-center">
        <p className="text-sm text-muted-foreground">Loading station details...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-4 p-6">
        <p className="text-sm text-destructive">{error}</p>
        <Button variant="destructive" onClick={fetchData}>Retry</Button>
      </div>
    );
  }

  if (!station) return null;

  return (
    <div className="flex min-h-full flex-col">
      <div className="flex items-center gap-3 border-b bg-card px-4 py-3">
        <Button variant="ghost" onClick={goBack} aria-label="Back to map">
          &larr; Back
        </Button>
      </div>

      <div className="flex-1 p-4">
        <h1 className="text-lg font-bold text-foreground">{station.name}</h1>
        <p className="mt-1 text-sm text-muted-foreground">{station.address}</p>

        <h2 className="mb-3 mt-6 text-sm font-semibold text-foreground">Chargers</h2>
        {chargers.length === 0 ? (
          <p className="text-sm text-muted-foreground">No chargers at this station.</p>
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
