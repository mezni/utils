import { Badge } from '@/components/ui/badge';
import type { Charger } from '../api/client';

interface ChargerRowProps {
  charger: Charger;
}

const variantMap: Record<string, 'default' | 'secondary' | 'destructive' | 'outline'> = {
  available: 'default',
  in_use: 'secondary',
  maintenance: 'destructive',
  offline: 'outline',
};

export function ChargerRow({ charger }: ChargerRowProps) {
  const variant = variantMap[charger.status] || 'outline';

  return (
    <div className="flex items-center justify-between rounded-lg border bg-card px-4 py-3">
      <div className="flex items-center gap-3">
        <span className="text-sm font-medium capitalize text-foreground">
          {charger.connector_type.replace('type', 'Type ')}
        </span>
        <span className="text-xs text-muted-foreground">{charger.power_kw} kW</span>
      </div>
      <Badge variant={variant} className="capitalize">
        {charger.status.replace(/_/g, ' ')}
      </Badge>
    </div>
  );
}
