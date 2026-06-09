import { Button } from '@/components/shared/Button';

interface ErrorStateProps {
  message?: string;
  onRetry?: () => void;
}

export function ErrorState({ message, onRetry }: ErrorStateProps) {
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center">
      <p className="mb-4 text-destructive">{message || 'Failed to load data'}</p>
      {onRetry && (
        <Button variant="danger" onClick={onRetry}>Retry</Button>
      )}
    </div>
  );
}
