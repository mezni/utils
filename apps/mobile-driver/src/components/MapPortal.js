import React, { useState, useEffect, useRef, useCallback } from 'react';
import { View, Text, ActivityIndicator, TouchableOpacity, Platform } from 'react-native';
import MapView from './MapView';
import SearchBar from './SearchBar';
import FilterControls from './FilterControls';
import ZoomControls from './ZoomControls';
import FAB from './FAB';
import StationDetailPanel from './StationDetailPanel';
import { fetchNearbyStations } from '../services/api';
import { useAppContext } from '../context/AppContext';
import { useSearch } from '../hooks/useSearch';
import { useFilters } from '../hooks/useFilters';
import { useStationDetail } from '../hooks/useStationDetail';
import { useAnalytics } from '../hooks/useAnalytics';

const TUNISIA_CENTER = {
  latitude: 36.8065,
  longitude: 10.1815,
  latitudeDelta: 0.08,
  longitudeDelta: 0.04,
};

export default function MapPortal() {
  const [stations, setStations] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const abortRef = useRef(null);
  const { selectedStation, setSelectedStation, filters } = useAppContext();
  const { query, results, isSearching, error: searchError, search, clear } = useSearch();
  const { activeFilters, setActiveFilters } = useFilters();
  const { station: detailStation, isLoading: detailLoading, error: detailError, open: openDetail, close: closeDetail, retry: retryDetail } = useStationDetail();
  const { track } = useAnalytics();
  const [locationDisabled, setLocationDisabled] = useState(false);

  const handleMarkerPress = useCallback((station) => {
    setSelectedStation(station);
    openDetail(station.id);
    track('marker_tap', { station_id: station.id });
  }, [setSelectedStation, openDetail, track]);

  const handleSearch = useCallback((text) => {
    search(text);
    if (text && text.length >= 2) {
      track('search_submit', { query: text });
    }
  }, [search, track]);

  const handleFilterChange = useCallback((newFilters) => {
    setActiveFilters(newFilters);
    track('filter_change', { filters: newFilters });
  }, [setActiveFilters, track]);

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
    return () => {
      if (abortRef.current) abortRef.current.abort();
    };
  }, []);

  const displayStations = query.length >= 2 && results.length > 0 ? results : stations;

  const handleZoomIn = useCallback(() => {
    track('zoom_in');
  }, [track]);

  const handleZoomOut = useCallback(() => {
    track('zoom_out');
  }, [track]);

  const handleLocateMe = useCallback(() => {
    if (!navigator.geolocation) {
      setLocationDisabled(true);
      return;
    }
    navigator.geolocation.getCurrentPosition(
      () => { track('locate_me'); },
      () => { setLocationDisabled(true); },
      { timeout: 5000 }
    );
  }, [track]);

  return (
    <div style={{ width: '100%', height: '100%', position: 'relative' }}>
      <MapView
        style={{ width: '100%', height: '100%' }}
        initialRegion={TUNISIA_CENTER}
        stations={displayStations}
        onMarkerPress={handleMarkerPress}
      />

      <SearchBar
        query={query}
        setQuery={clear}
        onSearch={handleSearch}
        results={results}
        isSearching={isSearching}
        error={searchError}
        onClear={clear}
      />

      <FilterControls
        filters={activeFilters}
        onFiltersChange={handleFilterChange}
      />

      <ZoomControls
        onZoomIn={handleZoomIn}
        onZoomOut={handleZoomOut}
        onLocateMe={handleLocateMe}
        locationDisabled={locationDisabled}
      />

      <FAB onPress={() => {}} />

      <StationDetailPanel
        station={detailStation}
        isLoading={detailLoading}
        error={detailError}
        onClose={closeDetail}
        onRetry={retryDetail}
        onNavigate={(url) => window.open(url, '_blank')}
      />

      {loading && (
        <div style={{ position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', backgroundColor: 'rgba(255,255,255,0.8)', zIndex: 50 }}>
          <Text>Loading stations...</Text>
        </div>
      )}

      {error && (
        <div style={{ position: 'absolute', top: 110, left: 16, right: 16, backgroundColor: '#D32F2F', borderRadius: 12, padding: 12, display: 'flex', flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', zIndex: 60 }}>
          <Text style={{ color: '#FFFFFF', fontSize: 13, fontWeight: '500' }}>Unable to connect to backend service</Text>
          <button onClick={loadData} style={{ background: '#FFFFFF', border: 'none', borderRadius: 8, padding: '6px 14px', cursor: 'pointer', fontWeight: '700', color: '#D32F2F', fontSize: 13 }}>Retry</button>
        </div>
      )}
    </div>
  );
}
