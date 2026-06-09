import { useCallback, useEffect, useState } from 'react';
import { MapContainer, CircleMarker, Popup, TileLayer, useMapEvents } from 'react-leaflet';
import { useLocation, useNavigate } from 'react-router-dom';
import { Button } from '@/components/ui/button';
import { list, type Charger, type Partner, type Station, type VisibleStation } from '../api/client';
import { ZoomControls } from '../components/ZoomControls';

interface MapPosition {
  center: [number, number];
  zoom: number;
}

const TUNISIA_CENTER: [number, number] = [33.8869, 9.5375];
const DEFAULT_ZOOM = 7;

function MapEventHandler({ onMoveEnd, onZoomIn, onZoomOut }: {
  onMoveEnd: (pos: MapPosition) => void;
  onZoomIn: () => void;
  onZoomOut: () => void;
}) {
  useMapEvents({
    moveend: (e) => {
      const map = e.target;
      const c = map.getCenter();
      onMoveEnd({ center: [c.lat, c.lng], zoom: map.getZoom() });
    },
  });

  return (
    <div className="absolute bottom-4 right-4 z-[1000]">
      <ZoomControls onZoomIn={onZoomIn} onZoomOut={onZoomOut} />
    </div>
  );
}

export function MapPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const restoredPosition = (location.state as { mapPosition?: MapPosition } | null)?.mapPosition;

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [stations, setStations] = useState<VisibleStation[]>([]);
  const [mapPosition, setMapPosition] = useState<MapPosition>(() => restoredPosition || {
    center: TUNISIA_CENTER,
    zoom: DEFAULT_ZOOM,
  });

  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [partners, allStations, chargers] = await Promise.all([
        list<Partner>('partners'),
        list<Station>('stations'),
        list<Charger>('chargers'),
      ]);

      const visiblePartnerIds = new Set(
        partners.filter(p => p.is_verified && p.is_live && p.is_active).map(p => p.id)
      );

      const chargerCountMap: Record<string, { total: number; available: number }> = {};
      for (const ch of chargers) {
        if (!chargerCountMap[ch.station_id]) {
          chargerCountMap[ch.station_id] = { total: 0, available: 0 };
        }
        chargerCountMap[ch.station_id].total++;
        if (ch.status === 'available') {
          chargerCountMap[ch.station_id].available++;
        }
      }

      setStations(
        allStations
          .filter(s => visiblePartnerIds.has(s.partner_id))
          .map(s => ({
            ...s,
            availableCount: chargerCountMap[s.id]?.available || 0,
            totalChargers: chargerCountMap[s.id]?.total || 0,
          }))
      );
    } catch {
      setError('Failed to load stations. Please try again.');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchData(); }, [fetchData]);
  useEffect(() => { window.scrollTo(0, 0); }, []);

  const handleMoveEnd = useCallback((pos: MapPosition) => {
    setMapPosition(pos);
  }, []);

  const handleZoomIn = () => {
    setMapPosition(prev => ({ ...prev, zoom: Math.min(prev.zoom + 1, 18) }));
  };

  const handleZoomOut = () => {
    setMapPosition(prev => ({ ...prev, zoom: Math.max(prev.zoom - 1, 1) }));
  };

  const handleNavigate = useCallback((stationId: string) => {
    navigate(`/stations/${stationId}`, { state: { mapPosition } });
  }, [navigate, mapPosition]);

  if (error) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-4 p-6">
        <p className="text-sm text-destructive">{error}</p>
        <Button variant="destructive" onClick={fetchData}>Retry</Button>
      </div>
    );
  }

  return (
    <div className="relative h-full w-full">
      <div className="fixed left-0 right-0 top-0 z-[2000] flex h-14 items-center bg-card px-4 shadow-sm">
        <span className="text-lg font-bold text-primary">BorneMap</span>
        {loading && <span className="ml-3 text-xs text-muted-foreground">Loading...</span>}
      </div>

      <div className="h-full w-full" style={{ paddingTop: '56px' }}>
        <MapContainer
          center={mapPosition.center}
          zoom={mapPosition.zoom}
          className="z-0 h-full w-full"
          scrollWheelZoom={true}
          zoomControl={false}
        >
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          <MapEventHandler onMoveEnd={handleMoveEnd} onZoomIn={handleZoomIn} onZoomOut={handleZoomOut} />

          {stations.map(station => (
            <CircleMarker
              key={station.id}
              center={[station.latitude, station.longitude]}
              radius={8}
              pathOptions={{
                fillColor: station.availableCount > 0 ? '#00E676' : '#EF4444',
                fillOpacity: 0.9,
                color: '#FFFFFF',
                weight: 2,
              }}
            >
              <Popup>
                <div className="min-w-[180px]">
                  <p className="text-sm font-semibold text-foreground">{station.name}</p>
                  <p className="mt-0.5 text-xs text-muted-foreground">{station.address}</p>
                  <p className="mt-1 text-xs">
                    <span className="font-medium">{station.availableCount}/{station.totalChargers}</span>{' '}
                    <span className="text-muted-foreground">available</span>
                  </p>
                  <button
                    onClick={() => handleNavigate(station.id)}
                    className="mt-2 text-xs font-medium text-primary hover:text-primary/80"
                  >
                    View Details &rarr;
                  </button>
                </div>
              </Popup>
            </CircleMarker>
          ))}
        </MapContainer>
      </div>
    </div>
  );
}
