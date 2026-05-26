import { useParams } from "react-router-dom"
import { ChargersTable } from "../components/chargers/chargers-table"

export function Chargers() {
  const { id } = useParams<{ id: string }>()

  return (
    <div>
      {id ? (
        <div>
          <h2 className="mb-4 text-lg font-semibold text-gray-900">Station: {id}</h2>
          <ChargersTable stationId={id} />
        </div>
      ) : (
        <ChargersTable />
      )}
    </div>
  )
}
