import { Card, CardContent } from '@/components/ui/card';
import type { Station } from '../api/client';

interface StationCardProps {
  station: Station;
  availableCount: number;
  totalChargers: number;
}

export function StationCard({ station, availableCount, totalChargers }: StationCardProps) {
  return (
    <Card>
      <CardContent className="p-4">
        <h3 className="text-sm font-semibold text-foreground">{station.name}</h3>
        <p className="mt-1 text-xs text-muted-foreground">{station.address}</p>
        <p className="mt-2 text-xs font-medium">
          <span className="text-green-600">{availableCount}</span>
          <span className="text-muted-foreground">/{totalChargers} available</span>
        </p>
      </CardContent>
    </Card>
  );
}
