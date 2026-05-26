import { useState, useEffect } from "react"
import { View, Text, StyleSheet, TouchableOpacity, ScrollView, ActivityIndicator } from "react-native"
import * as Linking from "expo-linking"
import { Station } from "../types/station"
import { Charger } from "../types/station"
import { fetchStationChargers } from "../services/nearby-api"
import Constants from "expo-constants"

interface StationSheetProps {
  station: Station
  onClose: () => void
}

const host = Constants.expoConfig?.hostUri?.split(":")[0] || "localhost"
const API_BASE = Constants.expoConfig?.extra?.apiUrl || `http://${host}:8080/api/v1`

const statusColors: Record<string, string> = {
  available: "#22c55e",
  occupied: "#f59e0b",
  faulted: "#ef4444",
  offline: "#9ca3af",
}

async function fetchConnectorTypes(): Promise<Record<string, string>> {
  try {
    const res = await fetch(`${API_BASE}/connector-types`)
    if (!res.ok) return {}
    const json = await res.json()
    const data = json.data || json
    const map: Record<string, string> = {}
    if (Array.isArray(data)) {
      data.forEach((ct: { id: string; name: string }) => { map[ct.id] = ct.name })
    }
    return map
  } catch {
    return {}
  }
}

export function StationSheet({ station, onClose }: StationSheetProps) {
  const [chargers, setChargers] = useState<Charger[]>([])
  const [connectorNames, setConnectorNames] = useState<Record<string, string>>({})
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    Promise.all([
      fetchStationChargers(station.id),
      fetchConnectorTypes(),
    ])
      .then(([chargerData, ctMap]) => {
        setChargers(chargerData)
        setConnectorNames(ctMap)
      })
      .catch(() => setChargers([]))
      .finally(() => setLoading(false))
  }, [station.id])

  const handleNavigate = () => {
    const url = `https://www.google.com/maps/dir/?api=1&destination=${station.latitude},${station.longitude}`
    Linking.openURL(url).catch(() => {
      // fallback: no maps app available
    })
  }

  return (
    <View style={styles.container}>
      <View style={styles.handle} />
      <View style={styles.header}>
        <View style={styles.headerText}>
          <Text style={styles.name}>{station.name}</Text>
          <Text style={styles.address}>{station.address}, {station.city}</Text>
          <Text style={styles.distance}>{station.distance_meters.toFixed(0)}m away</Text>
        </View>
        <TouchableOpacity onPress={onClose}>
          <Text style={styles.closeBtn}>✕</Text>
        </TouchableOpacity>
      </View>

      <View style={styles.chargerSummary}>
        <Text style={styles.chargerCount}>{station.available_chargers} charger(s) available</Text>
      </View>

      {loading ? (
        <ActivityIndicator style={styles.loader} color="#22c55e" />
      ) : (
        <ScrollView style={styles.chargerList}>
          {chargers.map((charger) => (
            <View key={charger.id} style={styles.chargerRow}>
              <View style={styles.chargerInfo}>
                <Text style={styles.chargerType}>{charger.power_kw}kW · {charger.current_type}</Text>
                <Text style={styles.chargerConnector}>{connectorNames[charger.connector_type_id] || charger.connector_type_id}</Text>
              </View>
              <View style={[styles.statusBadge, { backgroundColor: statusColors[charger.status] || "#9ca3af" }]}>
                <Text style={styles.statusText}>{charger.status}</Text>
              </View>
            </View>
          ))}
        </ScrollView>
      )}

      <TouchableOpacity style={styles.navigateBtn} onPress={handleNavigate}>
        <Text style={styles.navigateText}>Navigate</Text>
      </TouchableOpacity>
    </View>
  )
}

const styles = StyleSheet.create({
  container: {
    position: "absolute",
    bottom: 0,
    left: 0,
    right: 0,
    backgroundColor: "white",
    borderTopLeftRadius: 20,
    borderTopRightRadius: 20,
    paddingHorizontal: 20,
    paddingBottom: 40,
    maxHeight: "60%",
    shadowColor: "#000",
    shadowOffset: { width: 0, height: -2 },
    shadowOpacity: 0.1,
    shadowRadius: 8,
    elevation: 8,
  },
  handle: {
    width: 40,
    height: 4,
    backgroundColor: "#e5e7eb",
    borderRadius: 2,
    alignSelf: "center",
    marginTop: 10,
    marginBottom: 16,
  },
  header: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "flex-start",
  },
  headerText: {
    flex: 1,
  },
  name: {
    fontSize: 18,
    fontWeight: "700",
    color: "#111",
  },
  address: {
    fontSize: 14,
    color: "#666",
    marginTop: 2,
  },
  distance: {
    fontSize: 13,
    color: "#22c55e",
    fontWeight: "600",
    marginTop: 4,
  },
  closeBtn: {
    fontSize: 20,
    color: "#999",
    padding: 4,
  },
  chargerSummary: {
    marginTop: 12,
    paddingVertical: 8,
    borderTopWidth: 1,
    borderTopColor: "#f3f4f6",
  },
  chargerCount: {
    fontSize: 14,
    fontWeight: "600",
    color: "#374151",
  },
  loader: {
    marginVertical: 20,
  },
  chargerList: {
    maxHeight: 200,
  },
  chargerRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    paddingVertical: 10,
    borderBottomWidth: 1,
    borderBottomColor: "#f3f4f6",
  },
  chargerInfo: {
    flex: 1,
  },
  chargerType: {
    fontSize: 14,
    fontWeight: "500",
    color: "#111",
  },
  chargerConnector: {
    fontSize: 12,
    color: "#999",
    marginTop: 2,
  },
  statusBadge: {
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 12,
  },
  statusText: {
    fontSize: 11,
    fontWeight: "600",
    color: "white",
    textTransform: "capitalize",
  },
  navigateBtn: {
    backgroundColor: "#22c55e",
    borderRadius: 12,
    paddingVertical: 14,
    alignItems: "center",
    marginTop: 12,
  },
  navigateText: {
    fontSize: 16,
    fontWeight: "700",
    color: "white",
  },
})
