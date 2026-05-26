import { useState } from "react"
import { View, StyleSheet } from "react-native"
import { StationMap } from "../src/components/map/station-map"
import { StationSheet } from "../src/components/station-sheet"
import { Station } from "../src/types/station"

export default function Index() {
  const [selectedStation, setSelectedStation] = useState<Station | null>(null)

  return (
    <View style={StyleSheet.absoluteFill}>
      <StationMap onStationSelect={setSelectedStation} />
      {selectedStation && (
        <StationSheet
          station={selectedStation}
          onClose={() => setSelectedStation(null)}
        />
      )}
    </View>
  )
}
