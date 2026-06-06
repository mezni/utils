import { useTypedNavigation } from '../hooks/useTypedNavigation'
import StationCard from './StationCard'
import type { Station } from '../types'

interface StationListProps {
  stations: Station[]
}

/**
 * Example of how to use type-safe navigation with StationCard
 */
export function StationListWithNavigation({ stations }: StationListProps) {
  const { toStation } = useTypedNavigation()

  return (
    <div className="space-y-3">
      {stations.map(station => (
        <StationCard
          key={station.id}
          station={station}
          onClick={(id) => toStation(id)}
        />
      ))}
    </div>
  )
}
