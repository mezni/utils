import { Input } from '../ui/Input';

interface OcppConfigFieldsProps {
  chargeBoxId: string;
  ocppVersion: '1.6' | '2.0.1';
  serialNumber: string;
  onChange: (field: string, value: string) => void;
  errors?: Record<string, string>;
}

export function OcppConfigFields({
  chargeBoxId, ocppVersion, serialNumber, onChange, errors = {},
}: OcppConfigFieldsProps) {
  return (
    <div className="space-y-4">
      <h3 className="text-sm font-semibold text-gray-300 uppercase tracking-wider flex items-center gap-2">
        <svg className="w-4 h-4 text-orange-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z" />
        </svg>
        OCPP Configuration
      </h3>
      <div className="grid grid-cols-2 gap-3">
        <Input
          label="ChargeBox ID"
          value={chargeBoxId}
          onChange={(e) => onChange('chargeBoxId', e.target.value)}
          placeholder="CP-ABC123-001"
          error={errors.chargeBoxId}
          helperText="Unique OCPP identifier. Must match CSMS registry."
          className="font-mono text-xs"
        />
        <Input
          label="OCPP Version"
          value={ocppVersion}
          onChange={(e) => onChange('ocppVersion', e.target.value)}
          placeholder="2.0.1"
          error={errors.ocppVersion}
        />
      </div>
      <Input
        label="Serial Number"
        value={serialNumber}
        onChange={(e) => onChange('serialNumber', e.target.value)}
        placeholder="SN-2A3B4C5D"
        error={errors.serialNumber}
        className="font-mono text-xs"
      />
    </div>
  );
}
