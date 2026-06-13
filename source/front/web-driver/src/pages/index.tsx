import React, { useState, useEffect, useCallback } from 'react'
import { useThemeStore } from '../store/useThemeStore'
import { initMap, loadMapStyle, addMarkersToMap } from '../hooks/useLeafletMap'

interface Station {
  id: string
  name: string
  address: string
  distance_km: number
}

function MapPage() {
  const { isDarkMode } = useThemeStore()
  const [mapInitialized, setMapInitialized] = useState(false)
  const [markers, setMarkers] = useState<Array<{
    lat: number
    lng: number
    title: string
    description?: string
  }>>([])
  const [selectedStation, setSelectedStation] = useState<Station | null>(null)
  const [showModal, setShowModal] = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [windowWidth, setWindowWidth] = useState(window.innerWidth)

  useEffect(() => {
    const handleResize = () => {
      setWindowWidth(window.innerWidth)
    }

    window.addEventListener('resize', handleResize)
    return () => window.removeEventListener('resize', handleResize)
  }, [])

  useEffect(() => {
    initMap()
      .then(() => {
        setMapInitialized(true)
        // Add some sample markers
        const sampleMarkers = [
          {
            lat: 36.8065,
            lng: 10.1815,
            title: 'Station 1',
            description: 'Main station with 5 chargers',
          },
          {
            lat: 36.8165,
            lng: 10.1915,
            title: 'Station 2',
            description: 'Standalone station with 2 chargers',
          },
          {
            lat: 36.7965,
            lng: 10.1715,
            title: 'Station 3',
            description: 'Mall station with parking',
          },
        ]
        setMarkers(sampleMarkers)
        addMarkersToMap(sampleMarkers)
      })
      .catch((error) => {
        console.error('Failed to initialize map:', error)
      })
  }, [isDarkMode])

  useEffect(() => {
    if (mapInitialized) {
      loadMapStyle(isDarkMode)
      if (markers.length > 0) {
        addMarkersToMap(markers)
      }
    }
  }, [isDarkMode, mapInitialized, markers])

  const handleRefresh = useCallback(async () => {
    setRefreshing(true)
    await new Promise(resolve => setTimeout(resolve, 1000))
    console.log('Map refreshed')
    setRefreshing(false)
  }, [])

  const handleMarkerClick = useCallback((lat: number, lng: number, title: string) => {
    const station: Station = {
      id: title.replace('Station ', ''),
      name: title,
      address: `${lat.toFixed(4)}, ${lng.toFixed(4)}`,
      distance_km: (Math.random() * 10).toFixed(2),
    }
    setSelectedStation(station)
    setShowModal(true)
  }, [])

  if (!mapInitialized) {
    return (
      <div style={{
        width: '100vw',
        height: '100vh',
        backgroundColor: isDarkMode ? '#1a1a1a' : '#f5f5f5',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        color: isDarkMode ? '#999' : '#666',
      }}>
        {refreshing ? 'Refreshing...' : 'Initializing map...'}
      </div>
    )
  }

  return (
    <div style={{
      width: '100vw',
      height: '100vh',
      backgroundColor: isDarkMode ? '#1a1a1a' : '#f5f5f5',
      position: 'relative',
    }}>
      <div id="map" style={{
        width: '100%',
        height: '100%',
      }}></div>

      <div style={{
        position: 'absolute',
        top: '20px',
        left: '50%',
        transform: 'translateX(-50%)',
        color: isDarkMode ? '#fff' : '#000',
        textAlign: 'center',
        maxWidth: '90vw',
        padding: '0 16px',
      }}>
        <h1 style={{ margin: 0, fontSize: 'clamp(20px, 4vw, 32px)' }}>
          Map Screen
        </h1>
        <p style={{ margin: '8px 0 0 0', opacity: 0.8, fontSize: 'clamp(12px, 2vw, 16px)' }}>
          Interactive map with charging station markers
        </p>
        {refreshing && (
          <p style={{ margin: '8px 0 0 0', fontSize: '12px' }}>
            Refreshing...
          </p>
        )}
      </div>

      {showModal && selectedStation && (
        <div style={{
          position: 'fixed',
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          backgroundColor: 'rgba(0, 0, 0, 0.5)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          zIndex: 1000,
        }}>
          <div style={{
            backgroundColor: isDarkMode ? '#2a2a2a' : '#fff',
            padding: 24,
            borderRadius: 12,
            maxWidth: '90vw',
            width: '400px',
            maxHeight: '80vh',
            overflow: 'auto',
            boxShadow: '0 4px 6px rgba(0, 0, 0, 0.1)',
          }}>
            <div style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'flex-start',
              marginBottom: 16,
            }}>
              <div>
                <h2 style={{ color: isDarkMode ? '#fff' : '#000', margin: 0, fontSize: 'clamp(18px, 3vw, 24px)' }}>
                  {selectedStation.name}
                </h2>
                <p style={{
                  color: isDarkMode ? '#999' : '#666',
                  margin: '8px 0 0 0',
                  fontSize: 'clamp(12px, 2vw, 14px)',
                }}>
                  {selectedStation.address}
                </p>
              </div>
              <button
                onClick={() => setShowModal(false)}
                style={{
                  background: 'none',
                  border: 'none',
                  color: isDarkMode ? '#fff' : '#000',
                  fontSize: 'clamp(20px, 3vw, 24px)',
                  cursor: 'pointer',
                }}
              >
                ✕
              </button>
            </div>

            <div style={{
              borderTop: `1px solid ${isDarkMode ? '#444' : '#e5e5e5'}`,
              paddingTop: 16,
            }}>
              <p style={{
                color: isDarkMode ? '#999' : '#666',
                margin: '0 0 8px 0',
                fontSize: 'clamp(12px, 2vw, 14px)',
              }}>
                Distance: {selectedStation.distance_km} km away
              </p>

              <div style={{
                padding: 12,
                borderRadius: 8,
                backgroundColor: isDarkMode ? '#333' : '#f5f5f5',
                marginBottom: 12,
              }}>
                <h3 style={{
                  color: isDarkMode ? '#fff' : '#000',
                  margin: '0 0 8px 0',
                  fontSize: 'clamp(14px, 2vw, 16px)',
                }}>
                  Amenities
                </h3>
                <div style={{
                  display: 'flex',
                  gap: 8,
                  flexWrap: 'wrap',
                }}>
                  <span style={{
                    padding: '4px 8px',
                    borderRadius: 4,
                    backgroundColor: isDarkMode ? '#444' : '#e5e5e5',
                    fontSize: 'clamp(10px, 1.5vw, 12px)',
                  }}>
                    WiFi
                  </span>
                  <span style={{
                    padding: '4px 8px',
                    borderRadius: 4,
                    backgroundColor: isDarkMode ? '#444' : '#e5e5e5',
                    fontSize: 'clamp(10px, 1.5vw, 12px)',
                  }}>
                    Parking
                  </span>
                </div>
              </div>

              <button
                onClick={() => {
                  setShowModal(false)
                  console.log('Navigate to:', selectedStation)
                }}
                style={{
                  width: '100%',
                  padding: '12px',
                  backgroundColor: '#2563eb',
                  color: '#fff',
                  border: 'none',
                  borderRadius: 8,
                  fontSize: 'clamp(14px, 2vw, 16px)',
                  fontWeight: 'bold',
                  cursor: 'pointer',
                }}
              >
                Navigate to Station
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

function StationListPage() {
  const { isDarkMode } = useThemeStore()

  return (
    <div style={{
      padding: isDarkMode ? '20px' : '24px',
      minHeight: '100vh',
      backgroundColor: isDarkMode ? '#1a1a1a' : '#f5f5f5',
    }}>
      <h2 style={{ color: isDarkMode ? '#fff' : '#000' }}>
        Station List
      </h2>
      <p style={{ color: isDarkMode ? '#999' : '#666', marginTop: '10px' }}>
        List of all charging stations with pagination support
      </p>
    </div>
  )
}

function StationDetailPage() {
  const { isDarkMode } = useThemeStore()

  return (
    <div style={{
      padding: isDarkMode ? '20px' : '24px',
      minHeight: '100vh',
      backgroundColor: isDarkMode ? '#1a1a1a' : '#f5f5f5',
    }}>
      <h2 style={{ color: isDarkMode ? '#fff' : '#000' }}>
        Station Detail
      </h2>
      <p style={{ color: isDarkMode ? '#999' : '#666', marginTop: '10px' }}>
        Detailed information about selected station
      </p>
    </div>
  )
}

export default function HomePage() {
  return (
    <div style={{ minHeight: '100vh' }}>
      <MapPage />
      <StationListPage />
      <StationDetailPage />
    </div>
  )
}