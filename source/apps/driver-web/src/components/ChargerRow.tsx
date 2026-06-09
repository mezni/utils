import type { Charger } from '../api/client';

interface ChargerRowProps {
  charger: Charger;
}

const statusStyles: Record<string, string> = {
  available: 'text-status-available bg-status-available-bg',
  in_use: 'text-status-in-use bg-status-in-use-bg',
  maintenance: 'text-status-maintenance bg-status-maintenance-bg',
  offline: 'text-neutral-500 bg-neutral-100',
};

export function ChargerRow({ charger }: ChargerRowProps) {
  const style = statusStyles[charger.status] || 'text-neutral-500 bg-neutral-100';

  return (
    <div className="flex items-center justify-between rounded-lg border border-subtle px-4 py-3">
      <div className="flex items-center gap-3">
        <span className="text-sm font-medium capitalize text-main">
          {charger.connector_type.replace('type', 'Type ')}
        </span>
        <span className="text-xs text-muted">{charger.power_kw} kW</span>
      </div>
      <span className={`inline-block rounded-full px-2.5 py-0.5 text-xs font-medium capitalize ${style}`}>
        {charger.status.replace(/_/g, ' ')}
      </span>
    </div>
  );
}
