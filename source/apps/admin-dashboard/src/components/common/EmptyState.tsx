import { Inbox } from "lucide-react";

interface EmptyStateProps {
  message?: string;
  action?: React.ReactNode;
}

export function EmptyState({
  message = "No data found.",
  action,
}: EmptyStateProps) {
  return (
    <div
      className="flex flex-col items-center justify-center py-12 text-center"
      role="status"
    >
      <Inbox className="h-12 w-12 text-muted-foreground/40 mb-4" />
      <p className="text-sm text-muted-foreground mb-4">{message}</p>
      {action}
    </div>
  );
}
