# Sprint 1.5 — Phase 1 Hardening - Data Model

**Date**: 2026-06-06
**Feature**: Sprint 1.5 Phase 1 Hardening
**Branch**: `005-phase1-hardening`

---

## Overview

This hardening sprint does NOT modify the data model. It focuses on verifying consistency across existing components and ensuring all screens work correctly in Arabic RTL layout. This document outlines the current data structures that must be verified for consistency.

---

## Existing Data Structures

### 1. Partner Data

**Used in**: Dashboard Partner screens

```typescript
interface Partner {
  id: string;           // Partner ID (e.g., PRT-XXX)
  name: string;         // Partner name
  email: string;        // Partner email
  phone: string;        // Partner phone
  address: string;      // Partner address
  status: 'active' | 'inactive';
  created_at: string;   // ISO timestamp
  updated_at: string;   // ISO timestamp
}
```

**Status Cards**:
- Total stations
- Active stations
- Inactive stations
- Total revenue (mock)
- Active users (mock)
- Satisfaction rate (mock)

---

### 2. Station Data

**Used in**: Driver Web Station List, Driver Mobile Station List, Dashboard Station screens

```typescript
interface Station {
  id: string;           // Station ID (e.g., STN-XXX)
  partner_id: string;   // Partner ID (foreign key)
  name: string;         // Station name
  address: string;      // Full address
  city: string;         // City name
  lat: number;          // Latitude
  lng: number;          // Longitude
  status: 'available' | 'in_use' | 'maintenance';
  type: 'fast_charge' | 'slow_charge' | 'dc_fast';
  charging_speed_kwh: number;  // Mock charging speed
  total_cost_per_kwh: number;  // Mock cost
  images?: string[];    // Array of image URLs
  created_at: string;   // ISO timestamp
  updated_at: string;   // ISO timestamp
}
```

**StatusBadge States**:
- `available`: Green dot (#2ECC71)
- `in_use`: Amber dot (#F39C12)
- `maintenance`: Red dot (#E74C3C)

---

### 3. Charger Data

**Used in**: Dashboard Charger Management, Station Detail screens

```typescript
interface Charger {
  id: string;           // Charger ID (e.g., CHG-XXX)
  station_id: string;   // Station ID (foreign key)
  type: 'type1' | 'type2' | 'ccs' | 'chademo';
  power_kw: number;     // Charging power in kW
  connector_count: number;  // Number of connectors
  status: 'available' | 'in_use' | 'maintenance' | 'offline';
  position: {           // Position on station map
    x: number;          // 0-100 percentage
    y: number;          // 0-100 percentage
  };
  created_at: string;   // ISO timestamp
  updated_at: string;   // ISO timestamp
}
```

---

### 4. User Data

**Used in**: Dashboard User screens, Driver Profile

```typescript
interface User {
  id: string;           // User ID (e.g., USR-XXX)
  email: string;        // User email
  name: string;         // User name
  phone: string;        // User phone
  role: 'registered_driver' | 'partner' | 'admin';
  partner_id?: string;  // Partner ID (if partner)
  created_at: string;   // ISO timestamp
  updated_at: string;   // ISO timestamp
}
```

---

### 5. Review Data

**Used in**: Driver Mobile Add Review, Dashboard Reviews screens

```typescript
interface Review {
  id: string;           // Review ID (e.g., REV-XXX)
  user_id: string;      // User ID (foreign key)
  station_id: string;   // Station ID (foreign key)
  rating: number;       // 1-5 stars
  comment: string;      // Review text
  images?: string[];    // Array of image URLs
  created_at: string;   // ISO timestamp
  updated_at: string;   // ISO timestamp
}
```

---

### 6. Mock Reports Data

**Used in**: Dashboard Reports screens

```typescript
interface ReportStats {
  period: 'today' | 'week' | 'month' | 'year';
  stations: number;     // Number of stations
  chargers: number;     // Number of chargers
  active_chargers: number;  // Currently active chargers
  total_reviews: number;   // Total reviews
  average_rating: number;  // Average rating (1-5)
  revenue: number;      // Mock revenue
  active_users: number;  // Mock active users
  satisfaction_rate: number;  // Mock satisfaction %
}
```

---

## Design Token Consistency Requirements

### Color Tokens

All color tokens must resolve to the same hex values across all three applications:

```typescript
// Brand Colors
brand.primary: #007943
brand.sageLight: #E8F3ED
brand.sageDark: #1A4A30
brand.sageText: #007943

// Semantic Colors
success: #2ECC71
warning: #F39C12
error: #E74C3C
info: #3498DB

// Neutral Colors
neutral-50: #FAFAFA
neutral-100: #F5F5F5
neutral-200: #E5E5E5
neutral-300: #D4D4D4
neutral-400: #A3A3A3
neutral-500: #737373
neutral-600: #525252
neutral-700: #404040
neutral-800: #262626
neutral-900: #171717
```

**Verification**:
- [ ] All apps reference tokens from `@borne-map/ui`
- [ ] No hardcoded colors in components
- [ ] All color values match specification
- [ ] Status colors appear correctly in all apps

---

### Typography Tokens

```typescript
// Web Dashboard (Inter font)
font-sans: 'Inter', sans-serif;
font-size-base: 16px;
font-size-lg: 18px;
font-size-xl: 24px;
font-size-2xl: 30px;
font-size-3xl: 36px;

// Driver Web & Mobile (Plus Jakarta Sans)
font-sans: 'Plus Jakarta Sans', sans-serif;
font-size-base: 16px;
font-size-lg: 18px;
font-size-xl: 24px;
font-size-2xl: 30px;
font-size-3xl: 36px;

// Line Height
line-height-tight: 1.25
line-height-normal: 1.5
line-height-relaxed: 1.75
```

**Verification**:
- [ ] Correct font family used for each app
- [ ] Consistent font sizes across all apps
- [ ] Correct line heights for readability

---

### Spacing Tokens

```typescript
// Spacing scale (8px grid)
spacing-0: 0px
spacing-1: 0.25rem (4px)
spacing-2: 0.5rem (8px)
spacing-3: 0.75rem (12px)
spacing-4: 1rem (16px)
spacing-5: 1.25rem (20px)
spacing-6: 1.5rem (24px)
spacing-8: 2rem (32px)
spacing-10: 2.5rem (40px)
spacing-12: 3rem (48px)
spacing-16: 4rem (64px)
spacing-20: 5rem (80px)
spacing-24: 6rem (96px)
```

**Verification**:
- [ ] All apps use consistent spacing scale
- [ ] No hardcoded spacing values
- [ ] Spacing respects 8px grid

---

### Radius Tokens

```typescript
radius-none: 0px
radius-sm: 0.25rem (4px)
radius-md: 0.5rem (8px)
radius-lg: 0.75rem (12px)
radius-xl: 1rem (16px)
radius-2xl: 1.5rem (24px)
radius-full: 9999px
```

**Verification**:
- [ ] Consistent border radius across all apps
- [ ] No hardcoded border radius values

---

### Shadow Tokens

```typescript
// Shadows for elevation
shadow-none: none
shadow-sm: 0 1px 2px 0 rgba(0, 0, 0, 0.05)
shadow-md: 0 4px 6px -1px rgba(0, 0, 0, 0.1)
shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1)
shadow-xl: 0 20px 25px -5px rgba(0, 0, 0, 0.1)
shadow-2xl: 0 25px 50px -12px rgba(0, 0, 0, 0.25)
```

**Verification**:
- [ ] Consistent shadow tokens across all apps
- [ ] Appropriate shadow for elevation

---

## Component Data Structures

### StatusBadge

```typescript
interface StatusBadgeProps {
  status: 'available' | 'in_use' | 'maintenance';
  variant?: 'full' | 'compact';
}
```

**States**:
- `available`: Green dot (#2ECC71) + text label "Disponible"
- `in_use`: Amber dot (#F39C12) + text label "En cours"
- `maintenance`: Red dot (#E74C3C) + text label "Maintenance"

**Verification**:
- [ ] Same color values in web and mobile variants
- [ ] Same text labels in all languages
- [ ] Consistent padding and sizing

---

### StationCard

```typescript
interface StationCardProps {
  station: Station;
  distance?: number;  // km
  onStationClick?: (stationId: string) => void;
}
```

**Fields Displayed**:
- Station name
- Station address
- Distance (optional)
- Charger count
- Status (via StatusBadge)

**Verification**:
- [ ] Same fields displayed in web and mobile
- [ ] Same visual hierarchy in web and mobile
- [ ] Consistent spacing and padding
- [ ] Consistent shadows and border radius

---

## Verification Checklist

### Data Structure Consistency

- [ ] All data interfaces are consistent across apps
- [ ] Status values are consistent (`available`, `in_use`, `maintenance`)
- [ ] StatusBadge states work correctly in all apps
- [ ] StationCard fields are consistent across web and mobile
- [ ] Color tokens resolve to same values in all apps
- [ ] Typography tokens are correct for each app
- [ ] Spacing tokens are consistent across all apps
- [ ] Radius tokens are consistent across all apps
- [ ] Shadow tokens are consistent across all apps

### RTL Data Display

- [ ] Station names display correctly in Arabic
- [ ] Station addresses display correctly in Arabic
- [ ] Distance labels display correctly in Arabic
- [ ] Status labels display correctly in Arabic
- [ ] Review text displays correctly in Arabic
- [ ] User names display correctly in Arabic
- [ ] Partner names display correctly in Arabic

---

## No Data Model Changes

This sprint is a hardening sprint with **no data model modifications**. All data structures remain the same. The focus is on:

1. Verifying consistency across all three applications
2. Ensuring all screens work correctly in Arabic RTL layout
3. Verifying design token usage
4. Testing accessibility, cross-browser, and mobile compatibility
5. Updating documentation to reflect reality

Any bugs found should be documented in `docs/project/bugs.md` and fixed if they are Class A bugs. Class B and C bugs can be deferred to Phase 2.
