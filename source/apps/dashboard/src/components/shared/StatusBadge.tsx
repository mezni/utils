import { Badge } from '@/components/ui/badge';

const variantMap: Record<string, 'default' | 'secondary' | 'destructive' | 'outline'> = {
  available: 'default',
  in_use: 'secondary',
  maintenance: 'destructive',
  offline: 'outline',
};

interface StatusBadgeProps {
  status: string;
}

export function StatusBadge({ status }: StatusBadgeProps) {
  const variant = variantMap[status] || 'outline';
  return (
    <Badge variant={variant} className="capitalize">
      {status.replace(/_/g, ' ')}
    </Badge>
  );
}
