interface EmptyStateProps {
  title?: string;
  message?: string;
  action?: { label: string; onClick: () => void };
}

export function EmptyState({ title, message, action }: EmptyStateProps) {
  return (
    <div className="flex flex-col items-center justify-center py-20 gap-5">
      <div className="p-4 bg-muted rounded-2xl text-gray-600">
        <svg className="w-8 h-8" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4" />
        </svg>
      </div>
      <div className="text-center max-w-xs">
        <p className="text-sm font-medium text-gray-300">{title || 'No data found'}</p>
        {message && <p className="text-xs text-gray-500 mt-1.5 leading-relaxed">{message}</p>}
      </div>
      {action && (
        <button onClick={action.onClick} className="inline-flex items-center gap-2 px-4 py-2 bg-orange-500 text-slate-900 text-sm font-medium rounded-lg hover:bg-orange-400 transition-colors shadow-lg shadow-orange-500/20">
          {action.label}
        </button>
      )}
    </div>
  );
}
