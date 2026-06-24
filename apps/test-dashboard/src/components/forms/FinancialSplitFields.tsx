import { Input } from '../ui/Input';

interface FinancialSplitFieldsProps {
  revenueSharePct: number;
  payoutAddress: string;
  tariffId: string;
  energyRatePerKwh: number;
  onChange: (field: string, value: unknown) => void;
  errors?: Record<string, string>;
}

export function FinancialSplitFields({
  revenueSharePct, payoutAddress, tariffId, energyRatePerKwh,
  onChange, errors = {},
}: FinancialSplitFieldsProps) {
  return (
    <div className="space-y-4">
      <h3 className="text-sm font-semibold text-gray-300 uppercase tracking-wider flex items-center gap-2">
        <svg className="w-4 h-4 text-orange-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
        Financial Configuration
      </h3>
      <div className="grid grid-cols-2 gap-3">
        <Input
          label="Revenue Share"
          type="number"
          min={0}
          max={100}
          step={0.5}
          value={revenueSharePct}
          onChange={(e) => onChange('revenueSharePct', parseFloat(e.target.value) || 0)}
          error={errors.revenueSharePct}
          suffix="%"
          helperText="Partner payout percentage per session"
        />
        <Input
          label="Energy Rate"
          type="number"
          min={0}
          step={0.001}
          value={energyRatePerKwh}
          onChange={(e) => onChange('energyRatePerKwh', parseFloat(e.target.value) || 0)}
          error={errors.energyRatePerKwh}
          suffix="€/kWh"
        />
      </div>
      <Input
        label="Payout Address (Wallet / IBAN)"
        value={payoutAddress}
        onChange={(e) => onChange('payoutAddress', e.target.value)}
        placeholder="0x... / IBAN..."
        error={errors.payoutAddress}
        className="font-mono text-xs"
        helperText="Blockchain wallet address or SEPA IBAN for automated settlements"
      />
      <Input
        label="Tariff ID"
        value={tariffId}
        onChange={(e) => onChange('tariffId', e.target.value)}
        placeholder="TARIFF-STANDARD"
        error={errors.tariffId}
        className="font-mono text-xs"
      />
    </div>
  );
}
