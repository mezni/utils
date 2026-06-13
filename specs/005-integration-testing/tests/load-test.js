// k6 Load Test: Nearby Search
// Tests: US5 - Performance benchmark (SC-005, FR-015)
// 50 concurrent requests, target: p95 < 100ms
// Run: k6 run load-test.js

import http from 'k6/http'
import { check, sleep } from 'k6'
import { Rate, Trend } from 'k6/metrics'

const nearbySearchTrend = new Trend('nearby_search_duration')
const errorRate = new Rate('errors')

export const options = {
  stages: [
    { duration: '10s', target: 25 },  // ramp up
    { duration: '30s', target: 50 },  // steady at 50 concurrent
    { duration: '10s', target: 0 },   // ramp down
  ],
  thresholds: {
    nearby_search_duration: ['p(95)<100'],  // SC-005: p95 < 100ms
    errors: ['rate<0.05'],                    // < 5% error rate
  },
}

const BASE_URL = 'http://localhost:8080'

// Test stations near Tunis
const testLocations = [
  { lat: 36.8065, lng: 10.1815 },
  { lat: 36.8500, lng: 10.1500 },
  { lat: 36.8000, lng: 10.2000 },
  { lat: 36.8200, lng: 10.1700 },
  { lat: 36.7800, lng: 10.1400 },
]

export default function () {
  // Pick a random test location
  const loc = testLocations[Math.floor(Math.random() * testLocations.length)]
  const radius = 10

  // Nearby search
  const nearbyStart = Date.now()
  const nearbyRes = http.get(
    `${BASE_URL}/api/v1/stations/nearby?lat=${loc.lat}&lng=${loc.lng}&radius=${radius}`,
    { tags: { endpoint: 'nearby' } }
  )
  const nearbyDuration = Date.now() - nearbyStart
  nearbySearchTrend.add(nearbyDuration)

  check(nearbyRes, {
    'nearby search status is 200': (r) => r.status === 200,
    'nearby search returns stations': (r) => {
      try {
        const body = JSON.parse(r.body)
        return Array.isArray(body.data) && body.data.length > 0
      } catch {
        return false
      }
    },
  }) || errorRate.add(1)

  // Station list (paginated)
  const listRes = http.get(
    `${BASE_URL}/api/v1/stations?page=1&per_page=20`,
    { tags: { endpoint: 'list' } }
  )
  check(listRes, {
    'station list status is 200': (r) => r.status === 200,
  }) || errorRate.add(1)

  sleep(0.5) // pause between iterations
}
