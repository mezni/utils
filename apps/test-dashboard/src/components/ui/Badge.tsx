import type { EntityStatus } from '../../types/common';

const stateConfig: Record<EntityStatus, { bg: string; text: string; dot: string }> = {
  ACTIVE: { bg: 'bg-green-500/10 border-green-500/20', text: 'text-green-400', dot: 'bg-green-400' },
  FAULTED: { bg: 'bg-red-500/10 border-red-500/20', text: 'text-red-400', dot: 'bg-red-400' },
  THROTTLED: { bg: 'bg-yellow-500/10 border-yellow-500/20', text: 'text-yellow-400', dot: 'bg-yellow-400' },
  CHARGING: { bg: 'bg-blue-500/10 border-blue-500/20', text: 'text-blue-400', dot: 'bg-blue-400' },
  OFFLINE: { bg: 'bg-gray-500/10 border-gray-500/20', text: 'text-gray-400', dot: 'bg-gray-400' },
  MAINTENANCE: { bg: 'bg-purple-500/10 border-purple-500/20', text: 'text-purple-400', dot: 'bg-purple-400' },
  DISABLED: { bg: 'bg-red-800/20 border-red-800/30', text: 'text-red-500', dot: 'bg-red-500' },
};

export function Badge({ status, pulse }: { status: EntityStatus; pulse?: boolean }) {
  const c = stateConfig[status] || stateConfig.OFFLINE;
  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium border ${c.bg} ${c.text}`}>
      <span className={`w-1.5 h-1.5 rounded-full ${c.dot} ${pulse ? 'animate-pulse-dot' : ''}`} />
      {status}
    </span>
  );
}
