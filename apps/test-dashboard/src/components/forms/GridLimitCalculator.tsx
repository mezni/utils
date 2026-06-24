import { Input } from '../ui/Input';

interface GridLimitCalculatorProps {
  gridLimitKw: number;
  totalChargerPowerKw: number;
  onChange: (v: number) => void;
  error?: string;
}

export function GridLimitCalculator({
  gridLimitKw, totalChargerPowerKw, onChange, error,
}: GridLimitCalculatorProps) {
  const overheadPct = totalChargerPowerKw > 0
    ? ((gridLimitKw / totalChargerPowerKw) * 100)
    : 100;
  const isOverLimit = gridLimitKw < totalChargerPowerKw;
  const safe = gridLimitKw >= totalChargerPowerKw * 1.2;

  return (
    <div className="space-y-3">
      <h3 className="text-sm font-semibold text-gray-300 uppercase tracking-wider flex items-center gap-2">
        <svg className="w-4 h-4 text-orange-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
        </svg>
        Grid Limit Calculation
      </h3>
      <div className="grid grid-cols-2 gap-3">
        <Input
          label="Site Grid Capacity"
          type="number"
          min={0}
          step={10}
          value={gridLimitKw}
          onChange={(e) => onChange(parseInt(e.target.value) || 0)}
          error={error}
          suffix="kW"
          helperText="Maximum draw from grid connection"
        />
        <div className="space-y-1.5">
          <label className="block text-sm font-medium text-gray-300">Total Charger Load</label>
          <div className="h-10 px-3 py-2.5 bg-surfaceAlt border border-gray-700 rounded-lg text-sm font-mono text-gray-400 flex items-center">
            {totalChargerPowerKw} kW
          </div>
          <p className="text-[11px] text-gray-600">Sum of all charger power ratings at this station</p>
        </div>
      </div>

      {/* Gauge */}
      <div className="space-y-1.5">
        <div className="flex justify-between text-xs">
          <span className="text-gray-500">Utilization</span>
          <span className={`font-mono tabular-nums ${isOverLimit ? 'text-red-400' : safe ? 'text-green-400' : 'text-yellow-400'}`}>
            {overheadPct.toFixed(0)}%
          </span>
        </div>
        <div className="h-2 bg-gray-800 rounded-full overflow-hidden">
          <div
            className={`h-full rounded-full transition-all duration-500 ${
              isOverLimit ? 'bg-red-500' : safe ? 'bg-green-500' : 'bg-yellow-500'
            }`}
            style={{ width: `${Math.min(overheadPct, 100)}%` }}
          />
        </div>
        <div className="flex justify-between text-[10px] text-gray-600">
          <span>0 kW</span>
          <span>{totalChargerPowerKw} kW (rated)</span>
          <span>{(totalChargerPowerKw * 1.5).toFixed(0)} kW</span>
        </div>
      </div>

      {isOverLimit && (
        <div className="p-3 bg-red-500/5 border border-red-500/20 rounded-xl flex items-start gap-2.5">
          <svg className="w-4 h-4 text-red-400 mt-0.5 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          <div>
            <p className="text-xs font-medium text-red-400">Grid limit exceeded</p>
            <p className="text-xs text-gray-500 mt-0.5">Total charger load ({totalChargerPowerKw} kW) exceeds grid capacity ({gridLimitKw} kW). Increase grid limit or reduce charger count.</p>
          </div>
        </div>
      )}
      {!isOverLimit && safe && (
        <div className="p-3 bg-green-500/5 border border-green-500/20 rounded-xl flex items-start gap-2.5">
          <svg className="w-4 h-4 text-green-400 mt-0.5 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          <p className="text-xs text-green-400">Adequate headroom — {((gridLimitKw / totalChargerPowerKw - 1) * 100).toFixed(0)}% overhead capacity</p>
        </div>
      )}
    </div>
  );
}
