import React, { useState, useEffect, useCallback } from 'react'
import { useThemeStore } from '../store/useThemeStore'
import { fetchStations } from '../services/stationListService'
import { searchByAddress } from '../services/geocodingService'

export default function StationListPage() {
  const { isDarkMode } = useThemeStore()
  const [stations, setStations] = useState<any[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const [isSearching, setIsSearching] = useState(false)
  const [searchQuery, setSearchQuery] = useState('')
  const [searchResults, setSearchResults] = useState<any[]>([])
  const [currentPage, setCurrentPage] = useState(1)
  const [totalPages, setTotalPages] = useState(1)

  // Debounced search
  let searchTimeout: any = null

  const debouncedSearch = useCallback(async (query: string) => {
    if (searchTimeout) {
      clearTimeout(searchTimeout)
    }

    if (query.trim().length < 2) {
      setSearchResults([])
      setSearchQuery(query)
      return
    }

    setIsSearching(true)

    searchTimeout = setTimeout(async () => {
      try {
        const results = await searchByAddress(query)
        setSearchResults(results)
      } catch (error) {
        console.error('Search failed:', error)
        setSearchResults([])
      } finally {
        setIsSearching(false)
      }
    }, 300)
  }, [])

  const fetchStations = async (page: number = 1) => {
    setIsLoading(true)
    try {
      const response = await fetchStations({ page, per_page: 20 })
      setStations(response.data)
      setTotalPages(response.meta.total_pages)
    } catch (error) {
      console.error('Failed to fetch stations:', error)
    } finally {
      setIsLoading(false)
    }
  }

  const handleRefresh = useCallback(async () => {
    setCurrentPage(1)
    await fetchStations(1)
  }, [])

  const handleLoadMore = useCallback(() => {
    if (currentPage < totalPages && !isLoading) {
      setCurrentPage(currentPage + 1)
      fetchStations(currentPage + 1)
    }
  }, [currentPage, totalPages, isLoading])

  useEffect(() => {
    fetchStations(currentPage)
  }, [currentPage])

  return (
    <div style={{
      minHeight: '100vh',
      backgroundColor: isDarkMode ? '#1a1a1a' : '#f5f5f5',
    }}>
      <div style={{
        padding: '20px 24px',
        borderBottom: `1px solid ${isDarkMode ? '#333' : '#e5e5e5'}`,
      }}>
        <h2 style={{ color: isDarkMode ? '#fff' : '#000' }}>
          Station List
        </h2>
        <p style={{ color: isDarkMode ? '#999' : '#666', marginTop: '8px' }}>
          List of all charging stations with pagination support
        </p>
      </div>

      <div style={{
        padding: '16px 24px',
        position: 'sticky',
        top: 0,
        backgroundColor: isDarkMode ? '#1a1a1a' : '#f5f5f5',
        borderBottom: `1px solid ${isDarkMode ? '#333' : '#e5e5e5'}`,
        zIndex: 100,
      }}>
        <div style={{ display: 'flex', gap: '12px', maxWidth: '600px' }}>
          <input
            type="text"
            placeholder="Search stations..."
            style={{
              flex: 1,
              padding: '12px',
              borderRadius: '8px',
              border: `1px solid ${isDarkMode ? '#444' : '#d1d5db'}`,
              backgroundColor: isDarkMode ? '#2a2a2a' : '#ffffff',
              color: isDarkMode ? '#fff' : '#000',
              fontSize: '16px',
            }}
            value={searchQuery}
            onChange={(e) => {
              setSearchQuery(e.target.value)
              debouncedSearch(e.target.value)
            }}
            onKeyPress={(e) => {
              if (e.key === 'Enter' && searchQuery) {
                setIsSearching(true)
              }
            }}
          />
          {isSearching && (
            <div style={{
              display: 'flex',
              alignItems: 'center',
              padding: '12px',
            }}>
              <div style={{
                width: '20px',
                height: '20px',
                border: `2px solid ${isDarkMode ? '#444' : '#d1d5db'}`,
                borderTopColor: '#2563eb',
                borderRadius: '50%',
                animation: 'spin 1s linear infinite',
              }}></div>
            </div>
          )}
        </div>

        {searchResults.length > 0 && (
          <div style={{
            marginTop: '12px',
            padding: '12px',
            borderRadius: '8px',
            backgroundColor: isDarkMode ? '#2a2a2a' : '#f5f5f5',
          }}>
            <div style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              marginBottom: '8px',
            }}>
              <span style={{ color: isDarkMode ? '#fff' : '#000' }}>
                Search Results ({searchResults.length})
              </span>
              <button
                onClick={() => setSearchResults([])}
                style={{
                  padding: '4px 12px',
                  backgroundColor: isDarkMode ? '#444' : '#e5e5e5',
                  color: isDarkMode ? '#fff' : '#000',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: 'pointer',
                }}
              >
                Clear
              </button>
            </div>
            <p style={{ color: isDarkMode ? '#999' : '#666', fontSize: '14px' }}>
              {searchResults[0].display_name}
            </p>
          </div>
        )}
      </div>

      <div style={{
        padding: '16px 24px',
      }}>
        {isLoading && stations.length === 0 ? (
          Array.from({ length: 5 }).map((_, i) => (
            <div
              key={i}
              style={{
                padding: '16px',
                borderRadius: '8px',
                marginBottom: '12px',
                backgroundColor: isDarkMode ? '#2a2a2a' : '#ffffff',
              }}
            >
              <div style={{
                height: '16px',
                width: '60%',
                marginBottom: '12px',
                borderRadius: '4px',
                backgroundColor: isDarkMode ? '#333' : '#f0f0f0',
              }}></div>
              <div style={{
                height: '14px',
                width: '80%',
                marginBottom: '8px',
                borderRadius: '4px',
                backgroundColor: isDarkMode ? '#333' : '#f0f0f0',
              }}></div>
              <div style={{
                height: '12px',
                width: '40%',
                marginBottom: '8px',
                borderRadius: '4px',
                backgroundColor: isDarkMode ? '#333' : '#f0f0f0',
              }}></div>
            </div>
          ))
        ) : stations.length === 0 ? (
          <div style={{
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            minHeight: '400px',
            padding: '40px',
            textAlign: 'center',
          }}>
            <div style={{
              fontSize: '48px',
              marginBottom: '16px',
              opacity: 0.3,
            }}>
              ⚡
            </div>
            <h3 style={{ color: isDarkMode ? '#fff' : '#000', marginBottom: '8px' }}>
              No stations found
            </h3>
            <p style={{ color: isDarkMode ? '#999' : '#666' }}>
              Try adjusting your search or filters
            </p>
          </div>
        ) : (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))', gap: '16px' }}>
            {stations.map((station) => (
              <div
                key={station.id}
                style={{
                  padding: '16px',
                  borderRadius: '8px',
                  backgroundColor: isDarkMode ? '#2a2a2a' : '#ffffff',
                  boxShadow: '0 2px 4px rgba(0, 0, 0, 0.1)',
                  cursor: 'pointer',
                  transition: 'transform 0.2s, box-shadow 0.2s',
                }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.transform = 'translateY(-2px)'
                  e.currentTarget.style.boxShadow = '0 4px 8px rgba(0, 0, 0, 0.15)'
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.transform = 'translateY(0)'
                  e.currentTarget.style.boxShadow = '0 2px 4px rgba(0, 0, 0, 0.1)'
                }}
              >
                <h3 style={{
                  color: isDarkMode ? '#fff' : '#000',
                  marginBottom: '8px',
                  fontSize: '16px',
                }}>
                  {station.name}
                </h3>
                <p style={{ color: isDarkMode ? '#999' : '#666', fontSize: '14px', marginBottom: '8px' }}>
                  {station.address}
                </p>
                {station.distance_km && (
                  <span style={{
                    display: 'inline-block',
                    padding: '4px 8px',
                    borderRadius: '4px',
                    backgroundColor: isDarkMode ? '#333' : '#f0f0f0',
                    color: isDarkMode ? '#4ade80' : '#10B981',
                    fontSize: '12px',
                  }}>
                    {station.distance_km.toFixed(1)} km away
                  </span>
                )}
              </div>
            ))}
          </div>
        )}
      </div>

      {isLoading && stations.length > 0 && (
        <div style={{
          padding: '20px',
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          gap: '12px',
          color: isDarkMode ? '#999' : '#666',
        }}>
          <div style={{
            width: '20px',
            height: '20px',
            border: `2px solid ${isDarkMode ? '#444' : '#d1d5db'}`,
            borderTopColor: '#2563eb',
            borderRadius: '50%',
            animation: 'spin 1s linear infinite',
          }}></div>
          <span>Loading more stations...</span>
        </div>
      )}

      {stations.length > 0 && totalPages > 1 && (
        <div style={{
          padding: '20px',
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          gap: '12px',
        }}>
          <button
            onClick={() => handleLoadMore()}
            disabled={currentPage >= totalPages || isLoading}
            style={{
              padding: '10px 20px',
              backgroundColor: isDarkMode ? '#2563eb' : '#2563eb',
              color: '#fff',
              border: 'none',
              borderRadius: '8px',
              fontSize: '14px',
              cursor: currentPage >= totalPages || isLoading ? 'not-allowed' : 'pointer',
              opacity: currentPage >= totalPages || isLoading ? 0.5 : 1,
            }}
          >
            Load More
          </button>
        </div>
      )}
    </div>
  )
}