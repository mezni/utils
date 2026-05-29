import React, { useState, useEffect, useRef, useCallback } from 'react';
import { View, Text, ActivityIndicator, TouchableOpacity, Platform, StyleSheet } from 'react-native';
import MapView from '../components/MapView';
import SearchBar from '../components/SearchBar';
import FilterControls from '../components/FilterControls';
import ZoomControls from '../components/ZoomControls';
import FAB from '../components/FAB';
import { fetchNearbyStations } from '../services/api';
import { useAppContext } from '../context/AppContext';
import { useSearch } from '../hooks/useSearch';
import { useFilters } from '../hooks/useFilters';
import { useStationDetail } from '../hooks/useStationDetail';
import StationDetailSheet from '../components/StationDetailSheet';

const TUNISIA_CENTER = {
  latitude: 36.8065,
  longitude: 10.1815,
  latitudeDelta: 0.08,
  longitudeDelta: 0.04,
};

class ErrorBoundary extends React.Component {
  constructor(props) { super(props); this.state = { hasError: false, message: '' }; }
  static getDerivedStateFromError(error) { return { hasError: true, message: error.message || 'Map failed' }; }
  render() {
    if (this.state.hasError) {
      return (
        <View style={{ flex: 1, justifyContent: 'center', alignItems: 'center', backgroundColor: '#FFFFFF' }}>
          <Text style={{ color: '#D32F2F', fontSize: 14, fontWeight: '500', marginBottom: 8 }}>Map Unavailable</Text>
          <Text style={{ marginTop: 10, color: '#555555', fontSize: 14 }}>{this.state.message}</Text>
        </View>
      );
    }
    return this.props.children;
  }
}

export default function MapScreen() {
  const [stations, setStations] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const abortRef = useRef(null);
  const { setSelectedStation } = useAppContext();
  const { query, results, isSearching, error: searchError, search, clear } = useSearch();
  const { activeFilters, setActiveFilters } = useFilters();
  const { station: detailStation, isLoading: detailLoading, error: detailError, sheetMode, setSheetMode, open: openDetail, close: closeDetail, retry: retryDetail } = useStationDetail();
  const [locationDisabled, setLocationDisabled] = useState(false);

  const handleMarkerPress = useCallback((station) => {
    setSelectedStation(station);
    openDetail(station.id);
  }, [setSelectedStation, openDetail]);

  const loadData = () => {
    if (abortRef.current) abortRef.current.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    setLoading(true);
    setError(false);

    fetchNearbyStations({
      lat: TUNISIA_CENTER.latitude,
      lng: TUNISIA_CENTER.longitude,
      showStaged: true,
      signal: controller.signal,
    })
      .then((data) => {
        if (!data) { setLoading(false); return; }
        setStations(data);
        setLoading(false);
      })
      .catch(() => {
        setError(true);
        setLoading(false);
      });
  };

  useEffect(() => {
    loadData();
    return () => { if (abortRef.current) abortRef.current.abort(); };
  }, []);

  const displayStations = query.length >= 2 && results.length > 0 ? results : stations;

  const handleZoomIn = useCallback(() => {}, []);
  const handleZoomOut = useCallback(() => {}, []);
  const handleLocateMe = useCallback(() => {
    if (Platform.OS === 'web') {
      if (!navigator.geolocation) { setLocationDisabled(true); return; }
      navigator.geolocation.getCurrentPosition(
        () => {},
        () => setLocationDisabled(true),
        { timeout: 5000 }
      );
    }
  }, []);

  return (
    <View style={{ flex: 1 }}>
      <ErrorBoundary>
        <MapView
          style={{ ...StyleSheet.absoluteFillObject }}
          initialRegion={TUNISIA_CENTER}
          stations={displayStations}
          onMarkerPress={handleMarkerPress}
        />
      </ErrorBoundary>

      <SearchBar
        query={query}
        setQuery={clear}
        onSearch={search}
        results={results}
        isSearching={isSearching}
        error={searchError}
        onClear={clear}
      />

      <FilterControls
        filters={activeFilters}
        onFiltersChange={setActiveFilters}
      />

      <ZoomControls
        onZoomIn={handleZoomIn}
        onZoomOut={handleZoomOut}
        onLocateMe={handleLocateMe}
        locationDisabled={locationDisabled}
      />

      <FAB onPress={() => {}} />

      <StationDetailSheet
        station={detailStation}
        isLoading={detailLoading}
        error={detailError}
        sheetMode={sheetMode}
        setSheetMode={setSheetMode}
        onClose={closeDetail}
        onRetry={retryDetail}
        onNavigate={(url) => {}} // Linking.openURL would go here
      />

      {loading && (
        <View style={{ position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, justifyContent: 'center', alignItems: 'center', backgroundColor: 'rgba(255,255,255,0.8)', zIndex: 50 }}>
          <ActivityIndicator size="large" color="#007AFF" />
          <Text style={{ marginTop: 10, color: '#555555', fontSize: 14, fontWeight: '500' }}>Loading stations...</Text>
        </View>
      )}

      {error && (
        <View style={{ position: 'absolute', top: Platform.OS === 'ios' ? 90 : 60, left: 16, right: 16, backgroundColor: '#D32F2F', borderRadius: 12, padding: 12, flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', zIndex: 60, elevation: 6 }}>
          <Text style={{ color: '#FFFFFF', fontSize: 13, fontWeight: '500', flex: 1 }}>Unable to connect to backend service</Text>
          <TouchableOpacity style={{ backgroundColor: '#FFFFFF', paddingHorizontal: 14, paddingVertical: 6, borderRadius: 8, marginLeft: 12 }} onPress={loadData}>
            <Text style={{ color: '#D32F2F', fontWeight: '700', fontSize: 13 }}>Retry</Text>
          </TouchableOpacity>
        </View>
      )}
    </View>
  );
}
