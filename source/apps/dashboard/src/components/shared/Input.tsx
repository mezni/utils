import type { InputHTMLAttributes } from 'react';

interface InputProps extends InputHTMLAttributes<HTMLInputElement> {
  label: string;
  error?: string;
}

export function Input({ label, error, className = '', ...props }: InputProps) {
  return (
    <div className="flex flex-col gap-1">
      <label className="text-sm font-medium text-main">{label}</label>
      <input
        className={`rounded-lg border px-3 py-2 text-sm outline-none transition-colors focus:ring-2 focus:ring-brand-primary ${
          error ? 'border-status-maintenance' : 'border-default'
        } ${className}`}
        {...props}
      />
      {error && <span className="text-xs text-status-maintenance">{error}</span>}
    </div>
  );
}
