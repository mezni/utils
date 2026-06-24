import { MapProvider, StationMarkerLayer, LoadingSpinner, ErrorBanner, EmptyState } from "@bornemap/ui-kit";
import { useStationsNearViewport } from "../hooks/useStationsNearViewport";
import styles from "./MapPage.module.css";

export function MapPage() {
  const { center, zoom, stations, isLoading, error, onViewportChange, refetch } =
    useStationsNearViewport();

  return (
    <div className={styles.page}>
      <header className={styles.header}>
        <div className={styles.brand}>
          <div className={styles.logo}>B</div>
          <div className={styles.titleGroup}>
            <h1 className={styles.title}>BorneMap</h1>
            <span className={styles.subtitle}>EV Charging Stations · Tunisia</span>
          </div>
        </div>
        <div className={styles.stats}>
          {!isLoading && !error && (
            <div className={styles.statBadge}>
              <div className={styles.statDot} />
              <span>{stations.length} station{stations.length !== 1 ? "s" : ""}</span>
            </div>
          )}
        </div>
      </header>

      <main className={styles.main}>
        <div className={styles.mapContainer}>
          <MapProvider center={center} zoom={zoom} onViewportChange={onViewportChange}>
            {!error && <StationMarkerLayer stations={stations} />}
          </MapProvider>

          {isLoading && (
            <div className={styles.overlay}>
              <LoadingSpinner message="Loading stations..." />
            </div>
          )}

          {error && (
            <div className={styles.errorOverlay}>
              <ErrorBanner message={error.message} onRetry={refetch} />
            </div>
          )}

          {!isLoading && !error && stations.length === 0 && (
            <div className={styles.emptyOverlay}>
              <EmptyState />
            </div>
          )}
        </div>
      </main>
    </div>
  );
}
