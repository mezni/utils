import { type ReactNode } from 'react';

interface ContextBarProps {
  title: string;
  subtitle?: string;
  children: ReactNode;
  onClose?: () => void;
}

export function ContextBar({ title, subtitle, children, onClose }: ContextBarProps) {
  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800 shrink-0">
        <div className="min-w-0">
          <p className="text-sm font-semibold text-white truncate font-mono">{title}</p>
          {subtitle && <p className="text-xs text-gray-500 truncate">{subtitle}</p>}
        </div>
        {onClose && (
          <button onClick={onClose} className="p-1 text-gray-500 hover:text-gray-300 hover:bg-gray-800 rounded shrink-0 ml-2">
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" /></svg>
          </button>
        )}
      </div>
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {children}
      </div>
    </div>
  );
}

/* ─── Context snippet ─── */

export function ContextSnippet({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="space-y-0.5">
      <span className="text-[10px] font-medium text-gray-600 uppercase tracking-wider">{label}</span>
      <p className={`text-sm text-gray-300 ${mono ? 'font-mono text-xs' : ''}`}>{value}</p>
    </div>
  );
}
