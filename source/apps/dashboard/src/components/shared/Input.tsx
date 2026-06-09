import type { InputHTMLAttributes } from 'react';
import { Input as ShadcnInput } from '@/components/ui/input';

interface InputProps extends InputHTMLAttributes<HTMLInputElement> {
  label: string;
  error?: string;
}

export function Input({ label, error, className, ...props }: InputProps) {
  return (
    <div className="flex flex-col gap-1.5">
      <label className="text-sm font-medium text-foreground">{label}</label>
      <ShadcnInput className={error ? 'border-destructive focus-visible:ring-destructive' : className} {...props} />
      {error && <span className="text-xs text-destructive">{error}</span>}
    </div>
  );
}
