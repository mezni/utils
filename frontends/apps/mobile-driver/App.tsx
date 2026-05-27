import Constants from "expo-constants";
import React, { useEffect, useRef, useState } from "react";
import { Platform, View, Text, TouchableOpacity } from "react-native";

const FETCH_TIMEOUT = 5000;
type Status = "loading" | "alive" | "error";

function resolveApiUrl(): string {
  const envUrl = process.env.EXPO_PUBLIC_API_URL;
  if (envUrl) return envUrl;

  const debuggerHost = Constants.expoConfig?.extra?.debuggerHost;
  if (debuggerHost) {
    const host = debuggerHost.split(":")[0];
    return `http://${host}:8080/api/v1/health/live`;
  }

  if (Platform.OS === "android") {
    return "http://10.0.2.2:8080/api/v1/health/live";
  }

  return "http://127.0.0.1:8080/api/v1/health/live";
}

export default function App() {
  const [status, setStatus] = useState<Status>("loading");
  const [url] = useState(resolveApiUrl);
  const controllerRef = useRef<AbortController | null>(null);

  const checkHealth = () => {
    controllerRef.current?.abort();
    const controller = new AbortController();
    controllerRef.current = controller;
    const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT);

    setStatus("loading");
    fetch(url, { signal: controller.signal })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((data) => {
        setStatus(
          String(data.status ?? "").toLowerCase() === "alive"
            ? "alive"
            : "error"
        );
      })
      .catch(() => setStatus("error"))
      .finally(() => clearTimeout(timer));
  };

  useEffect(() => {
    checkHealth();
    return () => controllerRef.current?.abort();
  }, []);

  return (
    <View style={{ flex: 1, alignItems: "center", justifyContent: "center", padding: 20 }}>
      <Text style={{ fontSize: 14, color: "#666", marginBottom: 12 }}>{url}</Text>
      {status === "loading" && (
        <Text style={{ fontSize: 18 }}>Connecting...</Text>
      )}
      {status === "alive" && (
        <Text style={{ fontSize: 18 }}>Core Service: alive</Text>
      )}
      {status === "error" && (
        <View style={{ alignItems: "center" }}>
          <Text style={{ fontSize: 18, color: "red" }}>
            Connection Error
          </Text>
          <TouchableOpacity
            onPress={checkHealth}
            style={{
              marginTop: 16,
              paddingHorizontal: 24,
              paddingVertical: 12,
              backgroundColor: "#007AFF",
              borderRadius: 8,
            }}
          >
            <Text style={{ color: "white", fontSize: 16 }}>Retry</Text>
          </TouchableOpacity>
        </View>
      )}
    </View>
  );
}
