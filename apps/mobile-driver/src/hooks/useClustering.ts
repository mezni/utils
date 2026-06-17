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

const CLUSTER_THRESHOLD = 50; // meters

export function useClustering(
  stations: Station[],
  zoom: number,
): ClusteredStations {
  const isClustered = useMemo(() => zoom < 13, [zoom]);

  if (!isClustered) {
    return {
      clusters: stations.map((station) => ({
        id: station.id,
        stations: [station],
        count: 1,
      })),
      total: stations.length,
    };
  }

  const clustered = useMemo(() => {
    if (stations.length === 0) return [];

    const clusterCenters = new Map<string, Station[]>();

    stations.forEach((station) => {
      const key = `${Math.round(station.location['lat'] / CLUSTER_THRESHOLD)}_${Math.round(station.location['lon'] / CLUSTER_THRESHOLD)}`;
      if (!clusterCenters.has(key)) {
        clusterCenters.set(key, []);
      }
      clusterCenters.get(key)?.push(station);
    });

    return Array.from(clusterCenters.entries()).map(([id, clusterStations]) => ({
      id,
      stations: clusterStations,
      count: clusterStations.length,
    }));
  }, [stations]);

  return {
    clusters: clustered,
    total: stations.length,
  };
}
