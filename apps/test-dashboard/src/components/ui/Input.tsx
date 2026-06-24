import { useId } from 'react';

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  label: string;
  error?: string;
  helperText?: string;
  suffix?: string;
}

export function Input({
  label, error, helperText, suffix, id, className = '', ...props
}: InputProps) {
  const autoId = useId();
  const inputId = id || autoId;
  return (
    <div className="space-y-1.5">
      <label htmlFor={inputId} className="block text-sm font-medium text-gray-300">{label}</label>
      <div className="relative">
        <input
          id={inputId}
          className={`w-full px-3 py-2.5 bg-surface border rounded-lg text-foreground placeholder-gray-600 focus:outline-none focus:ring-2 transition-all duration-150
            ${error ? 'border-red-500/60 focus:ring-red-500/30 focus:border-red-500' : 'border-gray-700 hover:border-gray-600 focus:ring-orange-500/30 focus:border-orange-500/60'}
            ${suffix ? 'pr-12' : ''} ${className}`}
          {...props}
        />
        {suffix && (
          <span className="absolute right-3 top-1/2 -translate-y-1/2 text-xs text-gray-500 font-medium">{suffix}</span>
        )}
      </div>
      {error && <p className="text-xs text-red-400 flex items-center gap-1"><span>⚠</span> {error}</p>}
      {helperText && !error && <p className="text-xs text-gray-500">{helperText}</p>}
    </div>
  );
}
