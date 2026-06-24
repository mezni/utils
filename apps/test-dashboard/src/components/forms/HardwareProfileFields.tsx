import { Input } from '../ui/Input';
import type { ConnectorType } from '../../types/common';

interface HardwareProfileFieldsProps {
  manufacturer: string;
  model: string;
  powerRatingKw: number;
  maxConnectors: number;
  connectorTypes: ConnectorType[];
  onChange: (field: string, value: unknown) => void;
  errors?: Record<string, string>;
}

const connectorOptions: ConnectorType[] = ['CCS2', 'CHADEMO', 'TYPE2', 'GBT', 'NACS'];

export function HardwareProfileFields({
  manufacturer, model, powerRatingKw, maxConnectors, connectorTypes,
  onChange, errors = {},
}: HardwareProfileFieldsProps) {
  return (
    <div className="space-y-4">
      <h3 className="text-sm font-semibold text-gray-300 uppercase tracking-wider flex items-center gap-2">
        <svg className="w-4 h-4 text-orange-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
        </svg>
        Hardware Profile
      </h3>
      <div className="grid grid-cols-2 gap-3">
        <Input
          label="Manufacturer"
          value={manufacturer}
          onChange={(e) => onChange('manufacturer', e.target.value)}
          placeholder="ABB, Siemens, Delta, etc."
          error={errors.manufacturer}
        />
        <Input
          label="Model"
          value={model}
          onChange={(e) => onChange('model', e.target.value)}
          placeholder="Terra 350"
          error={errors.model}
        />
      </div>
      <Input
        label="Max Power Rating"
        type="number"
        min={0}
        max={1000}
        step={1}
        value={powerRatingKw}
        onChange={(e) => onChange('powerRatingKw', parseInt(e.target.value) || 0)}
        error={errors.powerRatingKw}
        suffix="kW"
        helperText="Per-charger maximum power output (50–1000 kW)"
      />
      <div className="grid grid-cols-2 gap-3">
        <Input
          label="Max Connectors"
          type="number"
          min={1}
          max={6}
          value={maxConnectors}
          onChange={(e) => onChange('maxConnectors', parseInt(e.target.value) || 1)}
          error={errors.maxConnectors}
        />
        <div className="space-y-1.5">
          <label className="block text-sm font-medium text-gray-300">Connector Types</label>
          <div className="flex flex-wrap gap-1.5">
            {connectorOptions.map((ct) => (
              <button
                key={ct}
                type="button"
                onClick={() => {
                  const next = connectorTypes.includes(ct)
                    ? connectorTypes.filter(c => c !== ct)
                    : [...connectorTypes, ct];
                  onChange('connectorTypes', next);
                }}
                className={`px-2.5 py-1 rounded-lg text-xs font-medium border transition-all
                  ${connectorTypes.includes(ct)
                    ? 'bg-orange-500/15 border-orange-500/40 text-orange-400'
                    : 'bg-surfaceAlt border-gray-700 text-gray-500 hover:border-gray-600'}`}
              >
                {ct}
              </button>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
