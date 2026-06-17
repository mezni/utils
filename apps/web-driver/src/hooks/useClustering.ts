import { useMemo } from 'react';
import type { Station } from '@bornemap/shared-types';

export interface ClusteredStations {
  clusters: Array<{
    id: string;
    stations: Station[];
    count: number;
  }>;
  total: number;
}

const CLUSTER_THRESHOLD_DEG = 0.05; // ~5km at equator, scales with latitude

export function useClustering(
  stations: Station[],
  zoom: number,
): ClusteredStations {
  return useMemo(() => {
    const isClustered = zoom < 13;

    if (!isClustered || stations.length === 0) {
      return {
        clusters: stations.map((station) => ({
          id: station.id,
          stations: [station],
          count: 1,
        })),
        total: stations.length,
      };
    }

    const adjustedThreshold = CLUSTER_THRESHOLD_DEG / Math.pow(2, 13 - zoom);

    const clusterCenters = new Map<string, Station[]>();

    stations.forEach((station) => {
      const lat = station.location.lat;
      const lon = station.location.lon;
      const key = `${Math.round(lat / adjustedThreshold)}_${Math.round(lon / adjustedThreshold)}`;
      if (!clusterCenters.has(key)) {
        clusterCenters.set(key, []);
      }
      clusterCenters.get(key)!.push(station);
    });

    const clusters = Array.from(clusterCenters.entries()).map(([id, clusterStations]) => ({
      id,
      stations: clusterStations,
      count: clusterStations.length,
    }));

    return { clusters, total: stations.length };
  }, [stations, zoom]);
}
