import React, { useEffect } from 'react';
import { StyleSheet, StatusBar, Platform } from 'react-native';
import { SafeAreaProvider, SafeAreaView } from 'react-native-safe-area-context';
import axios from 'axios';
import MapScreen from './src/screens/MapScreen';

function useClickstreamTelemetry() {
  useEffect(() => {
    const dispatchAnalyticsTrace = async () => {
      try {
        const telemetryBundle = {
          event_id: `evt-${Math.random().toString(16).slice(2, 10)}`,
          client_platform: Platform.OS,
          app_version: "1.14.0",
          connected_at: new Date().toISOString(),
        };
        await axios.post('http://127.0.0.1:8080/api/v1/analytics/connect', telemetryBundle);
      } catch (_err) {
        console.log("analytics telemetry dropped silently");
      }
    };
    dispatchAnalyticsTrace();
  }, []);
}

export default function App() {
  useClickstreamTelemetry();

  return (
    <SafeAreaProvider>
    <SafeAreaView style={styles.container}>
      <StatusBar barStyle="dark-content" backgroundColor="#FFFFFF" />
      <MapScreen />
    </SafeAreaView>
    </SafeAreaProvider>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#FFFFFF',
  },
});
