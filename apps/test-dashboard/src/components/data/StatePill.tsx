import type { EntityStatus } from '../../types/common';

interface StatePillProps {
  status: EntityStatus;
  pulse?: boolean;
  size?: 'sm' | 'md';
}

const config: Record<EntityStatus, { bg: string; text: string; dot: string }> = {
  ACTIVE: { bg: 'bg-green-500/10', text: 'text-green-400', dot: 'bg-green-400' },
  FAULTED: { bg: 'bg-red-500/10', text: 'text-red-400', dot: 'bg-red-400' },
  THROTTLED: { bg: 'bg-yellow-500/10', text: 'text-yellow-400', dot: 'bg-yellow-400' },
  CHARGING: { bg: 'bg-blue-500/10', text: 'text-blue-400', dot: 'bg-blue-400' },
  OFFLINE: { bg: 'bg-gray-500/10', text: 'text-gray-400', dot: 'bg-gray-400' },
  MAINTENANCE: { bg: 'bg-purple-500/10', text: 'text-purple-400', dot: 'bg-purple-400' },
  DISABLED: { bg: 'bg-red-800/20', text: 'text-red-500', dot: 'bg-red-500' },
};

export function StatePill({ status, pulse, size = 'md' }: StatePillProps) {
  const c = config[status] || config.OFFLINE;
  const dotSize = size === 'sm' ? 'w-1 h-1' : 'w-1.5 h-1.5';
  const textSize = size === 'sm' ? 'text-[10px]' : 'text-xs';
  return (
    <span className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full font-medium ${c.bg} ${c.text} ${textSize} border border-current/10`}>
      <span className={`rounded-full ${dotSize} ${c.dot} ${pulse ? 'animate-pulse-dot' : ''}`} />
      {status}
    </span>
  );
}
