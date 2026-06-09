import { useEffect, useState, useCallback } from 'react';
import { useRole } from '../../context/RoleContext';
import { DataTable, type Column } from '../../components/shared/DataTable';
import { Button } from '../../components/shared/Button';
import { EmptyState } from '../../components/shared/EmptyState';
import { ErrorState } from '../../components/shared/ErrorState';
import { Skeleton } from '../../components/shared/Skeleton';
import { list, create } from '../../api/client';

interface Station {
  id: string;
  name: string;
}

interface StationAvailability {
  id: string;
  station_id: string;
  status: string;
  updated_at: string;
  updated_by: string;
}

interface StationRow extends Station {
  currentStatus: string;
  toggling: boolean;
}

export function PartnerAvailabilityPage() {
  const { selectedPartnerId } = useRole();
  const [stations, setStations] = useState<StationRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [toggleError, setToggleError] = useState<string | null>(null);

  const getLatestPerStation = useCallback((records: StationAvailability[]): Record<string, string> => {
    const latest: Record<string, { status: string; updated_at: string }> = {};
    for (const rec of records) {
      const existing = latest[rec.station_id];
      if (!existing || rec.updated_at > existing.updated_at) {
        latest[rec.station_id] = { status: rec.status, updated_at: rec.updated_at };
      }
    }
    return Object.fromEntries(Object.entries(latest).map(([k, v]) => [k, v.status]));
  }, []);

  const fetchData = useCallback(async () => {
    if (!selectedPartnerId) { setLoading(false); return; }
    setLoading(true);
    setError(null);
    try {
      const ownStations = await list<Station>('stations', { partner_id: selectedPartnerId });
      const stationIds = ownStations.map(s => s.id);
      let availabilityMap: Record<string, string> = {};
      if (stationIds.length > 0) {
        const qs = stationIds.map(id => 'station_id=' + id).join('&');
        const records = await list<StationAvailability>('station_availability?' + qs);
        availabilityMap = getLatestPerStation(records);
      }
      setStations(ownStations.map(s => ({
        ...s,
        currentStatus: availabilityMap[s.id] || 'unknown',
        toggling: false,
      })));
    } catch {
      setError('Failed to load availability data');
    } finally {
      setLoading(false);
    }
  }, [selectedPartnerId, getLatestPerStation]);

  useEffect(() => { fetchData(); }, [fetchData]);

  const handleToggle = async (stationId: string, newStatus: string, currentStatus: string) => {
    if (newStatus === currentStatus) return;
    setToggleError(null);
    setStations(prev => prev.map(s => s.id === stationId ? { ...s, toggling: true } : s));
    try {
      await create('station_availability', {
        station_id: stationId,
        status: newStatus,
        updated_by: selectedPartnerId ? 'USR-' + selectedPartnerId : '',
        updated_at: new Date().toISOString(),
      });
      await fetchData();
    } catch {
      await fetchData();
      setToggleError('Failed to update availability. Please try again.');
    }
  };

  if (!selectedPartnerId) {
    return (
      <div className="flex items-center justify-center py-16">
        <p className="text-muted">Please select a partner from the sidebar dropdown.</p>
      </div>
    );
  }

  if (loading) return <Skeleton className="h-64" />;
  if (error) return <ErrorState message={error} onRetry={fetchData} />;

  const statusLabel = (status: string) => {
    const labels: Record<string, string> = { available: 'Available', partial: 'Partial', unavailable: 'Unavailable', unknown: 'Unknown' };
    return labels[status] || status;
  };

  const statusColor = (status: string) => {
    const colors: Record<string, string> = {
      available: 'text-status-available',
      partial: 'text-status-in-use',
      unavailable: 'text-status-maintenance',
      unknown: 'text-neutral-400',
    };
    return colors[status] || 'text-neutral-400';
  };

  const columns: Column<StationRow>[] = [
    { key: 'name', label: 'Station' },
    {
      key: 'currentStatus',
      label: 'Current Status',
      render: (s) => (
        <span className={`text-sm font-medium capitalize ${statusColor(s.currentStatus)}`}>
          {statusLabel(s.currentStatus)}
        </span>
      ),
    },
    {
      key: 'actions',
      label: 'Set Status',
      render: (s) => (
        <div className="flex gap-1">
          {(['available', 'partial', 'unavailable'] as const).map(status => (
            <Button
              key={status}
              variant={s.currentStatus === status ? 'primary' : 'secondary'}
              disabled={s.toggling}
              onClick={() => handleToggle(s.id, status, s.currentStatus)}
              className="px-2 py-1 text-xs capitalize"
            >
              {status === 'available' ? 'Available' : status === 'partial' ? 'Partial' : 'Unavailable'}
            </Button>
          ))}
        </div>
      ),
    },
  ];

  return (
    <div>
      <h2 className="mb-4 text-base font-semibold text-main">Station Availability</h2>

      {toggleError && (
        <div className="mb-3 rounded-lg border border-status-maintenance bg-status-maintenance-bg px-3 py-2 text-sm text-status-maintenance">
          {toggleError}
        </div>
      )}

      {stations.length === 0 ? (
        <EmptyState message="No stations yet" />
      ) : (
        <DataTable columns={columns} data={stations} keyExtractor={(s) => s.id} />
      )}
    </div>
  );
}
