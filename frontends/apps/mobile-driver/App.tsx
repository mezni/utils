import React, { useEffect, useState } from "react";
import { View, Text, TouchableOpacity } from "react-native";

type Status = "loading" | "alive" | "error";

export default function App() {
  const [status, setStatus] = useState<Status>("loading");

  const checkHealth = () => {
    setStatus("loading");
    fetch("http://localhost:8080/api/v1/health/live")
      .then((res) => res.json())
      .then((data) => setStatus(data.status === "alive" ? "alive" : "error"))
      .catch(() => setStatus("error"));
  };

  useEffect(() => {
    checkHealth();
  }, []);

  return (
    <View style={{ flex: 1, alignItems: "center", justifyContent: "center" }}>
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
