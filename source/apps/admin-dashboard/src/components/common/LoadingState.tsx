import { Skeleton } from "@/components/ui/skeleton";

interface LoadingStateProps {
  message?: string;
  count?: number;
}

export function LoadingState({ message = "Loading...", count = 5 }: LoadingStateProps) {
  return (
    <div className="space-y-4" role="status" aria-label={message}>
      <p className="text-sm text-muted-foreground">{message}</p>
      {Array.from({ length: count }).map((_, i) => (
        <div key={i} className="flex items-center space-x-4">
          <Skeleton className="h-12 flex-1" />
        </div>
      ))}
    </div>
  );
}
