const statusStyles: Record<string, string> = {
  available: 'bg-status-available-bg text-status-available',
  in_use: 'bg-status-in-use-bg text-status-in-use',
  maintenance: 'bg-status-maintenance-bg text-status-maintenance',
  offline: 'bg-neutral-100 text-neutral-500',
};

interface StatusBadgeProps {
  status: string;
}

export function StatusBadge({ status }: StatusBadgeProps) {
  const style = statusStyles[status] || 'bg-neutral-100 text-neutral-500';
  return (
    <span className={`inline-block rounded-full px-2.5 py-0.5 text-xs font-medium capitalize ${style}`}>
      {status.replace(/_/g, ' ')}
    </span>
  );
}
