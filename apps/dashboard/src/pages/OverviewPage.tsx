import { useState, useEffect } from 'react';

interface OverviewData {
  totalPartners: number;
  totalStations: number;
  totalChargers: number;
}

interface AdminApiItem {
  id: string;
}

async function fetchCount(url: string): Promise<number> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch ${url}`);
  const data: AdminApiItem[] = await res.json();
  return data.length;
}

export default function OverviewPage() {
  const [data, setData] = useState<OverviewData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        setLoading(true);
        const [totalPartners, totalStations, totalChargers] = await Promise.all(
          [
            fetchCount('/api/v1/partners'),
            fetchCount('/api/v1/stations'),
            fetchCount('/api/v1/chargers'),
          ]
        );
        if (!cancelled) {
          setData({ totalPartners, totalStations, totalChargers });
        }
      } catch (err) {
        if (!cancelled) {
          setError(
            err instanceof Error ? err.message : 'Failed to load overview'
          );
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    load();

    return () => {
      cancelled = true;
    };
  }, []);

  const cards = data
    ? [
        { label: 'Total Partners', value: data.totalPartners },
        { label: 'Total Stations', value: data.totalStations },
        { label: 'Total Chargers', value: data.totalChargers },
      ]
    : [];

  return (
    <div>
      <h2 className="text-2xl font-bold text-gray-800 mb-6">Overview</h2>

      {loading && (
        <div className="text-sm text-gray-500">Loading overview data...</div>
      )}

      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-2 rounded text-sm">
          {error}
        </div>
      )}

      {data && (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {cards.map((card) => (
            <div
              key={card.label}
              className="bg-white rounded-lg border border-gray-200 p-6 shadow-sm"
            >
              <p className="text-sm text-gray-500 mb-1">{card.label}</p>
              <p className="text-3xl font-bold text-gray-800">{card.value}</p>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
