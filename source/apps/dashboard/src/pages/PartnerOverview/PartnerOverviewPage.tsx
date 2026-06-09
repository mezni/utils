import { useEffect, useState } from 'react';
import { useRole } from '../../context/RoleContext';
import { StatCard } from '../../components/shared/StatCard';
import { DataTable, type Column } from '../../components/shared/DataTable';
import { Skeleton } from '../../components/shared/Skeleton';
import { ErrorState } from '../../components/shared/ErrorState';
import { EmptyState } from '../../components/shared/EmptyState';
import { get, list } from '../../api/client';

interface Partner {
  id: string;
  name: string;
  is_verified: boolean;
  is_live: boolean;
  is_active: boolean;
}

interface Station {
  id: string;
  partner_id: string;
  name: string;
  address: string;
}

interface Charger {
  id: string;
  station_id: string;
  status: string;
}

interface StationAvailability {
  id: string;
  station_id: string;
  status: string;
  updated_at: string;
}

interface StationRow extends Station {
  chargerCount: number;
  availability: string;
}

export function PartnerOverviewPage() {
  const { selectedPartnerId } = useRole();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [partner, setPartner] = useState<Partner | null>(null);
  const [stations, setStations] = useState<StationRow[]>([]);
  const [chargerCount, setChargerCount] = useState(0);
  const [availableChargerCount, setAvailableChargerCount] = useState(0);

  const fetchData = async () => {
    if (!selectedPartnerId) { setLoading(false); return; }
    setLoading(true);
    setError(null);
    try {
      const [partnerData, stationsData] = await Promise.all([
        get<Partner>('partners', selectedPartnerId),
        list<Station>('stations', { partner_id: selectedPartnerId }),
      ]);
      setPartner(partnerData);

      const stationIds = stationsData.map(s => s.id);
      let chargersData: Charger[] = [];
      let availabilityRecords: StationAvailability[] = [];

      if (stationIds.length > 0) {
        const qs = stationIds.map(id => 'station_id=' + id).join('&');
        [chargersData, availabilityRecords] = await Promise.all([
          list<Charger>('chargers?' + qs),
          list<StationAvailability>('station_availability?' + qs),
        ]);
      }

      setChargerCount(chargersData.length);
      setAvailableChargerCount(chargersData.filter(c => c.status === 'available').length);

      const latestPerStation: Record<string, string> = {};
      for (const rec of availabilityRecords) {
        const existing = latestPerStation[rec.station_id];
        if (!existing || rec.updated_at > existing) {
          latestPerStation[rec.station_id] = rec.status;
        }
      }

      const chargerCountMap: Record<string, number> = {};
      for (const ch of chargersData) {
        chargerCountMap[ch.station_id] = (chargerCountMap[ch.station_id] || 0) + 1;
      }

      setStations(stationsData.map(s => ({
        ...s,
        chargerCount: chargerCountMap[s.id] || 0,
        availability: latestPerStation[s.id] || 'unknown',
      })));
    } catch {
      setError('Failed to load overview data');
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

  if (loading) {
    return (
      <div className="space-y-6">
        <div className="grid grid-cols-3 gap-4">
          {[1, 2, 3].map(i => <Skeleton key={i} className="h-24" />)}
        </div>
        <Skeleton className="h-12 w-96" />
        <Skeleton className="h-64" />
      </div>
    );
  }

  if (error) return <ErrorState message={error} onRetry={fetchData} />;

  const Badge = ({ label, color }: { label: string; color: 'green' | 'gray' | 'red' }) => {
    const styles = {
      green: 'bg-status-available-bg text-status-available',
      gray: 'bg-neutral-100 text-neutral-500',
      red: 'bg-status-maintenance-bg text-status-maintenance',
    };
    return (
      <span className={`inline-block rounded-full px-2.5 py-0.5 text-xs font-medium ${styles[color]}`}>
        {label}
      </span>
    );
  };

  const columns: Column<StationRow>[] = [
    { key: 'name', label: 'Name' },
    { key: 'chargerCount', label: 'Chargers' },
    {
      key: 'availability',
      label: 'Availability',
      render: (s) => {
        const styles: Record<string, string> = {
          available: 'text-status-available',
          partial: 'text-status-in-use',
          unavailable: 'text-status-maintenance',
          unknown: 'text-neutral-400',
        };
        return <span className={`text-xs font-medium capitalize ${styles[s.availability] || 'text-neutral-400'}`}>{s.availability}</span>;
      },
    },
  ];

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-3 gap-4">
        <StatCard label="Own Stations" value={stations.length} />
        <StatCard label="Own Chargers" value={chargerCount} />
        <StatCard label="Available Chargers" value={availableChargerCount} />
      </div>

      <div className="flex items-center gap-4">
        <span className="text-sm font-medium text-muted">Status:</span>
        <Badge label={partner?.is_verified ? 'Verified' : 'Awaiting Verification'} color={partner?.is_verified ? 'green' : 'gray'} />
        <Badge label={partner?.is_live ? 'Live' : 'Not Live'} color={partner?.is_live ? 'green' : 'gray'} />
        <Badge label={partner?.is_active ? 'Active' : 'Suspended'} color={partner?.is_active ? 'green' : 'red'} />
      </div>

      <div>
        <h2 className="mb-3 text-base font-semibold text-main">My Stations</h2>
        {stations.length === 0 ? (
          <EmptyState message="No stations yet" />
        ) : (
          <DataTable columns={columns} data={stations} keyExtractor={(s) => s.id} />
        )}
      </div>
    </div>
  );
}
