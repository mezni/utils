// SPDX-License-Identifier: MIT
// Copyright (c) 2026 BorneMap Contributors
//
// This file defines the public TypeScript contract for @bm/api-client.
// Implementations must conform to these types exactly.

import type { Station } from '@bm/types'

/**
 * Create a configured API client instance.
 * @param baseUrl - Base URL of the driver-service (e.g. "http://localhost:3000")
 */
export function createApiClient(baseUrl: string): ApiClient

/**
 * Typed error thrown on failed API requests.
 */
export class ApiError extends Error {
  /** HTTP status code, or null for network-level failures */
  readonly status: number | null
  /** Optional response body, useful for debugging */
  readonly data: unknown | null
}

/**
 * Typed API client for the driver-service.
 */
export interface ApiClient {
  /** Fetch all stations. */
  getStations(): Promise<Station[]>
  /** Fetch a single station by ID. */
  getStationById(id: string): Promise<Station>
  /** Fetch stations near the given coordinates within a radius (meters). */
  getNearbyStations(lat: number, lng: number, radius: number): Promise<Station[]>
}
