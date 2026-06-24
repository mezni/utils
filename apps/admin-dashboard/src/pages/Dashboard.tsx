import { useKpis } from '../hooks/useDashboard';
import { Card } from '../components/ui/Card';
import { useState } from 'react';

export function DashboardPage() {
  const { data, isLoading, isError, error, refetch, isRefetching } = useKpis();
  const [spinning, setSpinning] = useState(false);

  const handleRefresh = async () => {
    setSpinning(true);
    await refetch();
    setTimeout(() => setSpinning(false), 400);
  };

  return (
    <div className="space-y-8 animate-fade-in">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-white font-mono tracking-tight">Dashboard</h1>
          <p className="text-sm text-gray-500 mt-1.5">EV Infrastructure Overview</p>
        </div>
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2 px-3 py-1.5 bg-green-500/5 border border-green-500/10 rounded-full">
            <span className="w-1.5 h-1.5 rounded-full bg-green-400 animate-pulse-dot" />
            <span className="text-xs text-green-400 font-medium">{isRefetching ? 'Updating…' : 'All Systems'}</span>
          </div>
          <button
            onClick={handleRefresh}
            disabled={isRefetching}
            className="p-2 text-gray-500 hover:text-orange-400 transition-colors disabled:opacity-50 disabled:cursor-not-allowed rounded-lg hover:bg-orange-500/5"
            title="Refresh"
          >
            <svg className={`w-5 h-5 ${spinning || isRefetching ? 'animate-spin' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
            </svg>
          </button>
        </div>
      </div>

      {isError && (
        <div className="p-4 bg-red-500/5 border border-red-500/15 rounded-xl text-red-400 text-sm flex items-center justify-between">
          <span>{error?.message || 'Failed to load KPIs'}</span>
          <button onClick={() => refetch()} className="underline hover:text-red-300 transition-colors ml-2 font-medium">Retry</button>
        </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-3 gap-5 lg:gap-6">
        <Card
          title="Partners"
          value={isLoading ? '—' : (data?.partners_count ?? 0)}
          subtitle="Network operators"
          accent="orange"
          icon={<svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z" /></svg>}
        />
        <Card
          title="Stations"
          value={isLoading ? '—' : (data?.stations_count ?? 0)}
          subtitle="Charging locations"
          accent="green"
          icon={<svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4" /></svg>}
        />
        <Card
          title="Chargers"
          value={isLoading ? '—' : (data?.chargers_count ?? 0)}
          subtitle="Charging units"
          accent="blue"
          icon={<svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M13 10V3L4 14h7v7l9-11h-7z" /></svg>}
        />
      </div>
    </div>
  );
}
