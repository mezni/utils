# Component Contracts: Driver Web App

This document defines the prop interfaces for all 9 driver-specific components. These serve as the contract between component implementations and the screens that consume them.

---

## MobileTopBar

```typescript
interface MobileTopBarProps {
  /** Brand name displayed in the top bar */
  brandName?: string
  /** Whether the sidebar/drawer is currently open */
  sidebarOpen: boolean
  /** Called when hamburger menu is toggled */
  onToggleSidebar: () => void
  /** Number of unread notifications (0 = hide bell) */
  notificationCount?: number
  /** Called when notification bell is clicked */
  onNotificationClick?: () => void
}
```

---

## SearchBar

```typescript
interface SearchBarProps {
  /** Current search input value */
  value: string
  /** Called on every keystroke */
  onChange: (value: string) => void
  /** Called when user submits (Enter key or search icon click) */
  onSubmit: (value: string) => void
  /** Placeholder text (i18n key) */
  placeholder?: string
  /** Whether to auto-focus on mount */
  autoFocus?: boolean
}
```

---

## FilterPills

```typescript
type ConnectorType = 'all' | 'Type2' | 'CCS' | 'CHAdeMO'
type AvailabilityFilter = 'all' | 'available'

interface FilterPillsProps {
  /** Currently selected charger type */
  selectedChargerType: ConnectorType
  /** Called when a charger type pill is clicked */
  onChargerTypeChange: (type: ConnectorType) => void
  /** Currently selected availability filter */
  selectedAvailability: AvailabilityFilter
  /** Called when an availability pill is clicked */
  onAvailabilityChange: (filter: AvailabilityFilter) => void
}
```

---

## MapPinMarker

```typescript
interface MapPinMarkerProps {
  /** Marker state */
  state: 'default' | 'selected' | 'unavailable'
  /** Station name for ARIA label */
  stationName: string
  /** Whether the station has available chargers */
  hasAvailable: boolean
  /** Click handler to navigate to station detail */
  onClick: () => void
  /** Position as percentage from top-left of map container */
  position: { top: string; left: string }
}
```

---

## ZoomControls

```typescript
interface ZoomControlsProps {
  /** Called when zoom in (+) is clicked */
  onZoomIn: () => void
  /** Called when zoom out (-) is clicked */
  onZoomOut: () => void
}
```

---

## StationCard

```typescript
interface StationCardProps {
  /** Station data object */
  station: {
    id: string
    name: string
    address: string
    distance: number
    chargerCount: number
    availableCount: number
    availability: 'available' | 'unavailable'
    rating: number
    reviewCount: number
  }
  /** Called when card is clicked */
  onClick: (stationId: string) => void
  /** Whether this station is in user's favorites */
  isFavorite?: boolean
  /** Called when favorite toggle button is clicked */
  onToggleFavorite?: (stationId: string) => void
}
```

---

## ChargerRow

```typescript
interface ChargerRowProps {
  /** Charger data object */
  charger: {
    id: string
    connectorType: 'Type2' | 'CCS' | 'CHAdeMO'
    powerKw: number
    availability: 'available' | 'unavailable'
    pricePerKwh: number
  }
}
```

---

## ReviewCard

```typescript
interface ReviewCardProps {
  /** Review data object */
  review: {
    id: string
    authorName: string
    rating: number
    text: string
    date: string
    language: 'ar' | 'fr' | 'en'
  }
  /** Max rating value (default: 5) */
  maxRating?: number
}
```

---

## BottomStationCard

```typescript
interface BottomStationCardProps {
  /** Station summary data */
  station: {
    id: string
    name: string
    address: string
    availability: 'available' | 'unavailable'
    distance: number
    chargerCount: number
    availableCount: number
    rating: number
  }
  /** Additional specification rows */
  specs?: Array<{
    label: string
    value: string
  }>
  /** Called when card is clicked */
  onClick: (stationId: string) => void
  /** Called when "Navigate" / "Get Directions" button is clicked */
  onNavigate?: (stationId: string) => void
}
```
