import type { Station } from '../api/client';

interface StationCardProps {
  station: Station;
  availableCount: number;
  totalChargers: number;
}

export function StationCard({ station, availableCount, totalChargers }: StationCardProps) {
  return (
    <div className="rounded-lg border border-default bg-card p-4 shadow-card">
      <h3 className="text-sm font-semibold text-main">{station.name}</h3>
      <p className="mt-1 text-xs text-muted">{station.address}</p>
      <p className="mt-2 text-xs font-medium">
        <span className="text-status-available">{availableCount}</span>
        <span className="text-muted">/{totalChargers} available</span>
      </p>
    </div>
  );
}
