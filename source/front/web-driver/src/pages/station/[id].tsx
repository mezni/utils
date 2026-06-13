import React, { useState, useEffect } from 'react'
import { useThemeStore } from '../store/useThemeStore'
import { fetchStationDetail } from '../services/stationDetailService'

export default function StationDetailPage() {
  const { isDarkMode } = useThemeStore()
  const [station, setStation] = useState<any>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [showImages, setShowImages] = useState(false)
  const [scrollPosition, setScrollPosition] = useState(0)

  // Handle scroll position for lazy loading images
  useEffect(() => {
    const handleScroll = () => {
      setScrollPosition(window.scrollY)
    }

    window.addEventListener('scroll', handleScroll)
    return () => window.removeEventListener('scroll', handleScroll)
  }, [])

  // Fetch station detail when component mounts
  useEffect(() => {
    const fetchStation = async () => {
      setIsLoading(true)
      try {
        // Simulate fetching station detail
        await new Promise(resolve => setTimeout(resolve, 500))
        setStation({
          id: '1',
          name: 'Main Street Charging Station',
          address: '123 Main Street, Tunis, Tunisia',
          geometry: {
            type: 'Point',
            coordinates: [10.1815, 36.8065],
          },
          amenities: ['WiFi', 'Parking', 'Cafe', 'Restrooms'],
          operating_hours: '24/7',
          chargers: [
            {
              id: '1',
              charger_type: 'CCS',
              connector_count: 2,
              availability_status: 'available',
              power_kw: 50,
              is_active: true,
            },
            {
              id: '2',
              charger_type: 'CCS',
              connector_count: 3,
              availability_status: 'available',
              power_kw: 75,
              is_active: true,
            },
            {
              id: '3',
              charger_type: 'CHAdeMO',
              connector_count: 2,
              availability_status: 'in_use',
              power_kw: 50,
              is_active: true,
            },
            {
              id: '4',
              charger_type: 'AC',
              connector_count: 4,
              availability_status: 'available',
              power_kw: 7,
              is_active: true,
            },
          ],
          images: [
            {
              id: '1',
              url: 'https://images.unsplash.com/photo-1559526324-4b87b5e36e44?w=800&h=600&fit=crop',
              caption: 'Main entrance',
              is_primary: true,
            },
            {
              id: '2',
              url: 'https://images.unsplash.com/photo-1590674899484-d5640e854abe?w=800&h=600&fit=crop',
              caption: 'Charging bays',
              is_primary: false,
            },
            {
              id: '3',
              url: 'https://images.unsplash.com/photo-1565514020170-44e1f781c6cd?w=800&h=600&fit=crop',
              caption: 'Waiting area',
              is_primary: false,
            },
          ],
        })
      } catch (err) {
        setError('Failed to load station details')
      } finally {
        setIsLoading(false)
      }
    }

    fetchStation()
  }, [])

  const getChargeRate = (charger: any) => {
    if (charger.charger_type === 'CCS' && charger.power_kw >= 50) {
      return '€0.30/kWh'
    } else if (charger.charger_type === 'CHAdeMO') {
      return '€0.35/kWh'
    } else {
      return '€0.25/kWh'
    }
  }

  const handleNavigate = () => {
    // Open external mapping app
    if (station && station.geometry) {
      const [lng, lat] = station.geometry.coordinates
      window.open(`https://www.google.com/maps/dir/?api=1&destination=${lat},${lng}`, '_blank')
    }
  }

  const handleDirections = () => {
    // Open directions in external map app
    if (station && station.geometry) {
      const [lng, lat] = station.geometry.coordinates
      window.open(`https://www.google.com/maps/dir/?api=1&destination=${lat},${lng}`, '_blank')
    }
  }

  if (isLoading) {
    return (
      <div style={{
        minHeight: '100vh',
        padding: '24px',
        backgroundColor: isDarkMode ? '#1a1a1a' : '#f5f5f5',
      }}>
        <div style={{
          padding: '20px',
          borderRadius: '8px',
          marginBottom: '16px',
          backgroundColor: isDarkMode ? '#2a2a2a' : '#ffffff',
          height: '200px',
        }}></div>
        <div style={{
          padding: '16px',
          borderRadius: '8px',
          marginBottom: '16px',
          backgroundColor: isDarkMode ? '#2a2a2a' : '#ffffff',
          height: '60px',
        }}></div>
        <div style={{
          padding: '16px',
          borderRadius: '8px',
          marginBottom: '16px',
          backgroundColor: isDarkMode ? '#2a2a2a' : '#ffffff',
          height: '60px',
        }}></div>
        <div style={{
          padding: '16px',
          borderRadius: '8px',
          marginBottom: '16px',
          backgroundColor: isDarkMode ? '#2a2a2a' : '#ffffff',
          height: '300px',
        }}></div>
      </div>
    )
  }

  if (error || !station) {
    return (
      <div style={{
        minHeight: '100vh',
        padding: '24px',
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        backgroundColor: isDarkMode ? '#1a1a1a' : '#f5f5f5',
      }}>
        <h2 style={{ color: isDarkMode ? '#fff' : '#000' }}>Error</h2>
        <p style={{ color: isDarkMode ? '#999' : '#666' }}>{error || 'Failed to load station'}</p>
      </div>
    )
  }

  return (
    <div style={{
      minHeight: '100vh',
      backgroundColor: isDarkMode ? '#1a1a1a' : '#f5f5f5',
    }}>
      {/* Header */}
      <div style={{
        position: 'sticky',
        top: 0,
        zIndex: 100,
        padding: '20px 24px',
        borderBottom: `1px solid ${isDarkMode ? '#333' : '#e5e5e5'}`,
        backgroundColor: isDarkMode ? '#1a1a1a' : '#f5f5f5',
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
      }}>
        <button
          onClick={() => window.history.back()}
          style={{
            padding: '8px 12px',
            backgroundColor: isDarkMode ? '#2a2a2a' : '#ffffff',
            color: isDarkMode ? '#fff' : '#000',
            border: `1px solid ${isDarkMode ? '#444' : '#e5e5e5'}`,
            borderRadius: '8px',
            cursor: 'pointer',
            fontSize: '14px',
          }}
        >
          ← Back
        </button>
        <h1 style={{ color: isDarkMode ? '#fff' : '#000', margin: 0, fontSize: '24px' }}>
          {station.name}
        </h1>
      </div>

      {/* Images Section */}
      {station.images && station.images.length > 0 && (
        <div style={{ borderBottom: `1px solid ${isDarkMode ? '#333' : '#e5e5e5'}` }}>
          <button
            style={{
              width: '100%',
              padding: '16px 24px',
              backgroundColor: isDarkMode ? '#2a2a2a' : '#f5f5f5',
              border: 'none',
              cursor: 'pointer',
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              fontSize: '16px',
              color: isDarkMode ? '#fff' : '#000',
            }}
            onClick={() => setShowImages(!showImages)}
          >
            <span>Photos ({station.images.length})</span>
            <span>{showImages ? '▼' : '▶'}</span>
          </button>

          {showImages && (
            <div style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(3, 1fr)',
              gap: '8px',
              padding: '8px',
              backgroundColor: isDarkMode ? '#2a2a2a' : '#f5f5f5',
            }}>
              {station.images.map((image: any, index: number) => (
                <img
                  key={index}
                  src={image.url}
                  alt={image.caption}
                  style={{
                    width: '100%',
                    height: '200px',
                    objectFit: 'cover',
                    borderRadius: '8px',
                  }}
                />
              ))}
            </div>
          )}
        </div>
      )}

      {/* Location Section */}
      <div style={{
        padding: '24px',
        borderBottom: `1px solid ${isDarkMode ? '#333' : '#e5e5e5'}`,
      }}>
        <h2 style={{ color: isDarkMode ? '#fff' : '#000', marginBottom: '8px' }}>Location</h2>
        <p style={{ color: isDarkMode ? '#999' : '#666', marginBottom: '8px' }}>
          {station.address}
        </p>
        {station.geometry && station.geometry.type === 'Point' && (
          <p style={{ color: isDarkMode ? '#999' : '#666', fontSize: '14px' }}>
            Coordinates: {station.geometry.coordinates[1].toFixed(4)}, {station.geometry.coordinates[0].toFixed(4)}
          </p>
        )}
      </div>

      {/* Amenities Section */}
      <div style={{
        padding: '24px',
        borderBottom: `1px solid ${isDarkMode ? '#333' : '#e5e5e5'}`,
      }}>
        <h2 style={{ color: isDarkMode ? '#fff' : '#000', marginBottom: '16px' }}>Amenities</h2>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '12px' }}>
          {station.amenities.map((amenity: string, index: number) => (
            <span
              key={index}
              style={{
                padding: '8px 16px',
                borderRadius: '8px',
                backgroundColor: isDarkMode ? '#333' : '#e0f2fe',
                color: isDarkMode ? '#fff' : '#000',
                fontSize: '14px',
              }}
            >
              {amenity}
            </span>
          ))}
        </div>
      </div>

      {/* Operating Hours Section */}
      <div style={{
        padding: '24px',
        borderBottom: `1px solid ${isDarkMode ? '#333' : '#e5e5e5'}`,
      }}>
        <h2 style={{ color: isDarkMode ? '#fff' : '#000', marginBottom: '8px' }}>Operating Hours</h2>
        <p style={{ color: isDarkMode ? '#999' : '#666' }}>
          {station.operating_hours || '24/7'}
        </p>
      </div>

      {/* Charger Details Section */}
      {station.chargers && station.chargers.length > 0 && (
        <div style={{
          padding: '24px',
          borderBottom: `1px solid ${isDarkMode ? '#333' : '#e5e5e5'}`,
        }}>
          <h2 style={{ color: isDarkMode ? '#fff' : '#000', marginBottom: '16px' }}>
            Chargers ({station.chargers.length})
          </h2>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
            {station.chargers.map((charger: any, index: number) => (
              <div
                key={index}
                style={{
                  padding: '16px',
                  borderRadius: '8px',
                  backgroundColor: isDarkMode ? '#333' : '#f5f5f5',
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                }}
              >
                <div style={{ flex: 1 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '8px' }}>
                    <span style={{
                      fontSize: '16px',
                      fontWeight: 'bold',
                      color: isDarkMode ? '#fff' : '#000',
                    }}>
                      {charger.charger_type}
                    </span>
                    <span style={{
                      padding: '4px 8px',
                      borderRadius: '4px',
                      backgroundColor: charger.power_kw >= 50 ? '#3B82F6' : charger.power_kw >= 20 ? '#10B981' : '#F59E0B',
                      color: '#fff',
                      fontSize: '12px',
                      fontWeight: 'bold',
                    }}>
                      {charger.power_kw} kW
                    </span>
                  </div>
                  <p style={{ color: isDarkMode ? '#999' : '#666', fontSize: '14px', marginBottom: '4px' }}>
                    {charger.connector_count} connector(s)
                  </p>
                  <p style={{ color: isDarkMode ? '#4ade80' : '#10B981', fontSize: '14px' }}>
                    Rate: {getChargeRate(charger)}
                  </p>
                </div>
                <div style={{
                  padding: '8px 16px',
                  borderRadius: '4px',
                  backgroundColor: charger.availability_status === 'available' ? '#10B981' : charger.availability_status === 'in_use' ? '#EF4444' : '#F59E0B',
                  color: '#fff',
                  fontSize: '14px',
                  fontWeight: 'bold',
                  minWidth: '100px',
                  textAlign: 'center',
                }}>
                  {charger.availability_status}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Action Buttons */}
      <div style={{
        padding: '24px',
        position: 'sticky',
        bottom: 0,
        zIndex: 100,
        display: 'flex',
        gap: '12px',
      }}>
        <button
          onClick={handleNavigate}
          style={{
            flex: 1,
            padding: '16px',
            backgroundColor: '#3B82F6',
            color: '#fff',
            border: 'none',
            borderRadius: '8px',
            fontSize: '16px',
            fontWeight: 'bold',
            cursor: 'pointer',
          }}
        >
          Navigate
        </button>
        <button
          onClick={handleDirections}
          style={{
            flex: 1,
            padding: '16px',
            backgroundColor: '#10B981',
            color: '#fff',
            border: 'none',
            borderRadius: '8px',
            fontSize: '16px',
            fontWeight: 'bold',
            cursor: 'pointer',
          }}
        >
          Directions
        </button>
      </div>
    </div>
  )
}