import React, { Component } from 'react';
import { StyleSheet, View, Text } from 'react-native';
import MapView, { Marker, PROVIDER_DEFAULT } from 'react-native-maps';

const TUNISIA_CENTER = {
  latitude: 36.8065,
  longitude: 10.1815,
  latitudeDelta: 0.12,
  longitudeDelta: 0.06,
};

class MapErrorBoundary extends Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, errorMessage: '' };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, errorMessage: error.message || 'Map failed to initialize' };
  }

  componentDidCatch(error) {
    this.setState({ hasError: true, errorMessage: error.message || 'Map component failed to initialize' });
  }

  render() {
    if (this.state.hasError) {
      return (
        <View style={styles.container}>
          <View style={styles.errorContainer}>
            <Text style={styles.errorTitle}>Map Unavailable</Text>
            <Text style={styles.errorDescription}>{this.state.errorMessage}</Text>
          </View>
        </View>
      );
    }
    return this.props.children;
  }
}

export default function MapScreen() {
  const debugOverlay = (
    <View style={styles.debugOverlay} pointerEvents="none">
      <Text style={styles.debugText}>BorneMap Sandbox Mode</Text>
      <Text style={styles.subText}>Tunisia Map Layer Rendered Offline</Text>
    </View>
  );

  return (
    <View style={styles.container}>
      <MapErrorBoundary>
        <MapView
          provider={PROVIDER_DEFAULT}
          style={styles.map}
          initialRegion={TUNISIA_CENTER}
        >
          <Marker
            coordinate={{ latitude: 36.8065, longitude: 10.1815 }}
            title="Tunis Core Baseline"
            description="Phase 1 Offline Isolation Landmark Checkpoint"
          />
        </MapView>
      </MapErrorBoundary>
      {debugOverlay}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },
  map: {
    ...StyleSheet.absoluteFillObject,
  },
  errorContainer: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
    padding: 20,
    backgroundColor: '#F5F5F5',
  },
  errorTitle: {
    fontSize: 18,
    fontWeight: 'bold',
    color: '#333333',
    marginBottom: 8,
  },
  errorDescription: {
    fontSize: 14,
    color: '#666666',
    textAlign: 'center',
  },
  debugOverlay: {
    position: 'absolute',
    top: 30,
    left: 20,
    right: 20,
    backgroundColor: 'rgba(255, 255, 255, 0.95)',
    padding: 14,
    borderRadius: 12,
    alignItems: 'center',
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.15,
    shadowRadius: 4,
    elevation: 3,
  },
  debugText: {
    fontWeight: 'bold',
    color: '#111111',
    fontSize: 14,
  },
  subText: {
    color: '#666666',
    fontSize: 12,
    marginTop: 2,
  },
});
