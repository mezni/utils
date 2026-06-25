import { usePartners } from "@/hooks/use-partners-query";
import { useStations } from "@/hooks/use-stations-query";
import { useChargers } from "@/hooks/use-chargers-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { ErrorState } from "@/components/common/ErrorState";
import { Building2, MapPin, Zap } from "lucide-react";

function SummaryCard({
  title,
  value,
  icon: Icon,
  isLoading,
  isError,
  onRetry,
}: {
  title: string;
  value?: number;
  icon: React.ElementType;
  isLoading: boolean;
  isError: boolean;
  onRetry?: () => void;
}) {
  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">{title}</CardTitle>
        <Icon className="h-4 w-4 text-muted-foreground" />
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <Skeleton className="h-8 w-20" />
        ) : isError ? (
          <div className="flex items-center gap-2">
            <span className="text-sm text-destructive">Error</span>
            {onRetry && (
              <button onClick={onRetry} className="text-xs text-primary hover:underline">
                Retry
              </button>
            )}
          </div>
        ) : (
          <p className="text-2xl font-bold">{value ?? 0}</p>
        )}
      </CardContent>
    </Card>
  );
}

export function DashboardPage() {
  const partners = usePartners({ page: 1 });
  const stations = useStations({ page: 1 });
  const chargers = useChargers({ page: 1 });

  const anyError = partners.isError || stations.isError || chargers.isError;

  if (anyError) {
    return (
      <ErrorState
        message="Failed to load dashboard data."
        onRetry={() => {
          partners.refetch();
          stations.refetch();
          chargers.refetch();
        }}
      />
    );
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Dashboard</h1>
        <p className="text-sm text-muted-foreground mt-1">
          Overview of your EV charging infrastructure.
        </p>
      </div>
      <div className="grid gap-4 md:grid-cols-3">
        <SummaryCard
          title="Partners"
          value={partners.data?.pagination.total}
          icon={Building2}
          isLoading={partners.isLoading}
          isError={partners.isError}
          onRetry={() => partners.refetch()}
        />
        <SummaryCard
          title="Stations"
          value={stations.data?.pagination.total}
          icon={MapPin}
          isLoading={stations.isLoading}
          isError={stations.isError}
          onRetry={() => stations.refetch()}
        />
        <SummaryCard
          title="Chargers"
          value={chargers.data?.pagination.total}
          icon={Zap}
          isLoading={chargers.isLoading}
          isError={chargers.isError}
          onRetry={() => chargers.refetch()}
        />
      </div>
    </div>
  );
}
