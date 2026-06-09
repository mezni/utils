import { useEffect, useState } from 'react';
import { StatCard } from '../../components/shared/StatCard';
import { DataTable, type Column } from '../../components/shared/DataTable';
import { Skeleton } from '../../components/shared/Skeleton';
import { ErrorState } from '../../components/shared/ErrorState';
import { EmptyState } from '../../components/shared/EmptyState';
import { list } from '../../api/client';

interface Partner { id: string; name: string; }
interface Station { id: string; partner_id: string; name: string; address: string; }
interface Charger { id: string; station_id: string; }

interface StationRow extends Station { partnerName: string; chargerCount: number; }

export function OverviewPage() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [partnerCount, setPartnerCount] = useState(0);
  const [stationCount, setStationCount] = useState(0);
  const [chargerCount, setChargerCount] = useState(0);
  const [stations, setStations] = useState<StationRow[]>([]);

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const [partners, allStations, chargers] = await Promise.all([
        list<Partner>('partners'),
        list<Station>('stations'),
        list<Charger>('chargers'),
      ]);
      const partnerMap = Object.fromEntries(partners.map((p) => [p.id, p.name]));
      const chargerCountMap: Record<string, number> = {};
      for (const ch of chargers) {
        chargerCountMap[ch.station_id] = (chargerCountMap[ch.station_id] || 0) + 1;
      }
      setPartnerCount(partners.length);
      setStationCount(allStations.length);
      setChargerCount(chargers.length);
      setStations(
        allStations.slice(0, 10).map((s) => ({
          ...s,
          partnerName: partnerMap[s.partner_id] || 'Unknown',
          chargerCount: chargerCountMap[s.id] || 0,
        }))
      );
    } catch {
      setError('Failed to load dashboard data');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchData(); }, []);

  if (loading) {
    return (
      <div className="space-y-6">
        <div className="grid grid-cols-3 gap-4">
          {[1, 2, 3].map((i) => <Skeleton key={i} className="h-24" />)}
        </div>
        <Skeleton className="h-64" />
      </div>
    );
  }

  if (error) return <ErrorState message={error} onRetry={fetchData} />;

  const columns: Column<StationRow>[] = [
    { key: 'name', label: 'Name' },
    { key: 'partnerName', label: 'Partner' },
    { key: 'chargerCount', label: 'Chargers' },
  ];

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-3 gap-4">
        <StatCard label="Total Partners" value={partnerCount} />
        <StatCard label="Total Stations" value={stationCount} />
        <StatCard label="Total Chargers" value={chargerCount} />
      </div>

      <div>
        <h2 className="mb-3 text-base font-semibold text-main">Recent Stations</h2>
        {stations.length === 0 ? (
          <EmptyState message="No stations yet" actionLabel="Add your first station" />
        ) : (
          <DataTable columns={columns} data={stations} keyExtractor={(s) => s.id} />
        )}
      </div>
    </div>
  );
}
