import { MapView } from "../components/MapView";
import { useNearbyStations } from "../hooks/useNearbyStations";

const TUNISIA_CENTER: [number, number] = [34.0, 9.5];
const DEFAULT_RADIUS = 100_000;

export function Home() {
  const { stations, loading, error, retry } = useNearbyStations(
    TUNISIA_CENTER[0],
    TUNISIA_CENTER[1],
    DEFAULT_RADIUS
  );

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white shadow-sm border-b px-6 py-4">
        <h1 className="text-2xl font-bold text-gray-900">BorneMap</h1>
        <p className="text-sm text-gray-500">EV Charging Stations — Tunisia</p>
      </header>

      <main className="max-w-5xl mx-auto px-4 py-6 space-y-4">
        {loading && (
          <div className="flex items-center justify-center h-40">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
            <span className="ml-3 text-gray-600">Loading stations...</span>
          </div>
        )}

        {error && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-center">
            <p className="text-red-700">Failed to load stations: {error}</p>
            <button
              onClick={retry}
              className="mt-2 px-4 py-2 bg-red-600 text-white rounded hover:bg-red-700"
            >
              Retry
            </button>
          </div>
        )}

        {!loading && !error && stations.length === 0 && (
          <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4 text-center">
            <p className="text-yellow-700">
              No charging stations found within {DEFAULT_RADIUS / 1000} km.
            </p>
          </div>
        )}

        {!loading && !error && stations.length > 0 && (
          <div className="text-sm text-gray-600">
            Found {stations.length} station{stations.length !== 1 ? "s" : ""}
          </div>
        )}

        <MapView stations={stations} center={TUNISIA_CENTER} />
      </main>
    </div>
  );
}
