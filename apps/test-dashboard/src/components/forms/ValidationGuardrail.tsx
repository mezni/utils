import { type ReactNode } from 'react';

interface ValidationGuardrailProps {
  children: ReactNode;
  show: boolean;
  type?: 'error' | 'warning' | 'success' | 'info';
  message: string;
}

const styles: Record<string, string> = {
  error: 'bg-red-500/5 border-red-500/20 text-red-400',
  warning: 'bg-yellow-500/5 border-yellow-500/20 text-yellow-400',
  success: 'bg-green-500/5 border-green-500/20 text-green-400',
  info: 'bg-blue-500/5 border-blue-500/20 text-blue-400',
};

const icons: Record<string, ReactNode> = {
  error: <svg className="w-4 h-4 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>,
  warning: <svg className="w-4 h-4 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" /></svg>,
  success: <svg className="w-4 h-4 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>,
  info: <svg className="w-4 h-4 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>,
};

export function ValidationGuardrail({
  children, show, type = 'info', message,
}: ValidationGuardrailProps) {
  return (
    <div className="space-y-2">
      {children}
      {show && (
        <div className={`flex items-start gap-2.5 p-3 rounded-xl border text-xs ${styles[type]} animate-slide-up`}>
          {icons[type]}
          <span>{message}</span>
        </div>
      )}
    </div>
  );
}

/* ─── Real-time Validation Rules ─── */

export const ocppIdPattern = /^CP-[A-Z0-9]+-\d{3}$/;
export const ibanPattern = /^[A-Z]{2}\d{2}[A-Z0-9]{1,30}$/;
export const ethAddressPattern = /^0x[a-fA-F0-9]{40}$/;
export const taxIdPatterns: Record<string, RegExp> = {
  DE: /^DE\d{9}$/,
  FR: /^FR[A-Z0-9]{11}$/,
  GB: /^GB\d{9}$/,
  NL: /^NL[A-Z0-9]{12}B\d{2}$/,
  US: /^\d{2}-\d{7}$/,
};

export function validateOcppId(id: string): string | null {
  if (!id) return 'ChargeBox ID is required';
  if (!ocppIdPattern.test(id)) return 'Format must be CP-XXXX-001';
  return null;
}

export function validatePayoutAddress(addr: string): string | null {
  if (!addr) return 'Payout address is required';
  if (!ethAddressPattern.test(addr) && !ibanPattern.test(addr.replace(/\s/g, ''))) {
    return 'Must be a valid ETH address (0x...) or IBAN';
  }
  return null;
}

export function validatePowerRating(kw: number): string | null {
  if (kw < 50) return 'Minimum 50 kW for DC fast charging';
  if (kw > 1000) return 'Maximum 1000 kW per charger';
  if (kw % 1 !== 0) return 'Must be a whole number';
  return null;
}

export function validateGridLimit(limit: number, totalChargerPower: number): string | null {
  if (limit < totalChargerPower) {
    return `Grid limit (${limit} kW) is below total charger capacity (${totalChargerPower} kW)`;
  }
  if (limit < totalChargerPower * 1.1) {
    return `Recommended minimum headroom is 20% over total charger load (${(totalChargerPower * 1.2).toFixed(0)} kW)`;
  }
  return null;
}
