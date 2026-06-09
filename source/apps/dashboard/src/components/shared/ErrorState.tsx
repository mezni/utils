interface ErrorStateProps {
  message?: string;
  onRetry?: () => void;
}

export function ErrorState({ message, onRetry }: ErrorStateProps) {
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center">
      <p className="mb-2 text-status-maintenance">{message || 'Failed to load data'}</p>
      {onRetry && (
        <button onClick={onRetry} className="rounded-lg bg-brand-primary px-4 py-2 text-sm font-medium text-white hover:bg-brand-primaryDark">
          Retry
        </button>
      )}
    </div>
  );
}
