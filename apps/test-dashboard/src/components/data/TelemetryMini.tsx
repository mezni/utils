import type { TelemetrySnapshot } from '../../types/common';

interface TelemetryMiniProps {
  telemetry: TelemetrySnapshot;
  compact?: boolean;
}

export function TelemetryMini({ telemetry, compact }: TelemetryMiniProps) {
  if (compact) {
    return (
      <span className="inline-flex items-center gap-2 text-xs font-mono tabular-nums">
        <span className="text-orange-400">{telemetry.power_kw.toFixed(1)}<span className="text-gray-600">kW</span></span>
        <span className="text-gray-600">·</span>
        <span className={telemetry.uptime_pct > 99 ? 'text-green-400' : telemetry.uptime_pct > 95 ? 'text-yellow-400' : 'text-red-400'}>
          {telemetry.uptime_pct.toFixed(1)}<span className="text-gray-600">%</span>
        </span>
      </span>
    );
  }

  return (
    <div className="flex items-center gap-4 text-xs font-mono tabular-nums">
      <Metric label="kW" value={telemetry.power_kw.toFixed(1)} color="text-orange-400" />
      <Metric label="V" value={telemetry.voltage_v.toString()} color="text-blue-400" />
      <Metric label="A" value={telemetry.current_a.toFixed(1)} color="text-yellow-400" />
      <Metric
        label="UP"
        value={`${telemetry.uptime_pct.toFixed(1)}%`}
        color={telemetry.uptime_pct > 99 ? 'text-green-400' : telemetry.uptime_pct > 95 ? 'text-yellow-400' : 'text-red-400'}
      />
    </div>
  );
}

function Metric({ label, value, color }: { label: string; value: string; color: string }) {
  return (
    <span className="flex items-baseline gap-1">
      <span className="text-gray-600 text-[10px]">{label}</span>
      <span className={`${color} font-semibold`}>{value}</span>
    </span>
  );
}

export function TelemetrySparkline({ value, max, label, color = 'text-orange-400' }: {
  value: number; max: number; label: string; color?: string;
}) {
  const pct = Math.min((value / max) * 100, 100);
  return (
    <div className="flex items-center gap-2 text-xs">
      <span className={`font-mono tabular-nums ${color} font-semibold`}>{value.toFixed(1)}</span>
      <span className="text-gray-600 text-[10px]">{label}</span>
      <div className="w-16 h-1.5 bg-gray-800 rounded-full overflow-hidden">
        <div className={`h-full rounded-full transition-all duration-500 ${color.replace('text-', 'bg-')}`}
          style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}
