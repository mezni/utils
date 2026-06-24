import type { EntityStatus } from '../../types/common';

const statusColors: Record<EntityStatus, { bg: string; text: string; dot: string }> = {
  ACTIVE: { bg: 'bg-green-500/10', text: 'text-green-400', dot: 'bg-green-400' },
  INACTIVE: { bg: 'bg-gray-500/10', text: 'text-gray-400', dot: 'bg-gray-400' },
  MAINTENANCE: { bg: 'bg-yellow-500/10', text: 'text-yellow-400', dot: 'bg-yellow-400' },
  DISABLED: { bg: 'bg-red-500/10', text: 'text-red-400', dot: 'bg-red-400' },
};

export function StatusBadge({ status }: { status: EntityStatus }) {
  const c = statusColors[status] || statusColors.INACTIVE;
  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium ${c.bg} ${c.text} border border-current/10`}>
      <span className={`w-1.5 h-1.5 rounded-full ${c.dot}`} />
      {status}
    </span>
  );
}
