# Data Models

This document defines all data models used in the BorneMap API.

## Overview

All data models follow a consistent structure with common fields:
- `id`: UUID (universally unique identifier)
- `created_at`: ISO 8601 timestamp
- `updated_at`: ISO 8601 timestamp
- `deleted_at`: ISO 8601 timestamp (soft delete)

## Common Fields

```typescript
interface BaseModel {
  id: string;
  created_at: string;
  updated_at: string;
  deleted_at: string | null;
}
```

## User Model

Represents a registered user in the system.

```typescript
interface User {
  id: string;
  email: string;
  first_name: string;
  last_name: string;
  avatar_url: string | null;
  is_active: boolean;
  created_at: string;
  updated_at: string;
  deleted_at: string | null;
  
  // Relations
  favorites: Location[];
  reviews: Review[];
}
```

### User Creation

```typescript
interface CreateUser {
  email: string;
  password: string;
  first_name?: string;
  last_name?: string;
  avatar_url?: string;
}
```

### User Update

```typescript
interface UpdateUser {
  email?: string;
  first_name?: string;
  last_name?: string;
  avatar_url?: string;
}
```

### Password Change

```typescript
interface ChangePassword {
  current_password: string;
  new_password: string;
}
```

## Location Model

Represents a point of interest in the system.

```typescript
interface Location {
  id: string;
  name: string;
  description: string | null;
  latitude: number;
  longitude: number;
  address: string | null;
  category_id: string;
  category_name: string;
  rating: number;
  reviews_count: number;
  image_url: string | null;
  amenities: string[];
  opening_hours: OpeningHours | null;
  is_verified: boolean;
  created_at: string;
  updated_at: string;
  deleted_at: string | null;
  
  // Relations
  reviews: Review[];
  favorites: Favorite[];
  media: Media[];
}
```

### Location Creation

```typescript
interface CreateLocation {
  name: string;
  description?: string;
  latitude: number;
  longitude: number;
  address?: string;
  category_id: string;
  amenities?: string[];
  opening_hours?: OpeningHours;
  image_url?: string;
}
```

### Location Update

```typescript
interface UpdateLocation {
  name?: string;
  description?: string;
  address?: string;
  category_id?: string;
  rating?: number;
  image_url?: string;
  amenities?: string[];
  opening_hours?: OpeningHours;
  is_verified?: boolean;
}
```

## Review Model

Represents user reviews for locations.

```typescript
interface Review {
  id: string;
  location_id: string;
  user_id: string;
  user_name: string;
  user_avatar_url: string | null;
  rating: number;
  comment: string;
  images: string[];
  is_verified_purchase: boolean;
  helpful_count: number;
  created_at: string;
  updated_at: string;
  deleted_at: string | null;
}
```

### Review Creation

```typescript
interface CreateReview {
  location_id: string;
  rating: number;
  comment: string;
  images?: string[];
  is_verified_purchase?: boolean;
}
```

### Review Update

```typescript
interface UpdateReview {
  rating?: number;
  comment?: string;
  images?: string[];
  is_verified_purchase?: boolean;
}
```

## Category Model

Defines categories for organizing locations.

```typescript
interface Category {
  id: string;
  name: string;
  icon: string;
  color: string;
  description: string | null;
  created_at: string;
  updated_at: string;
  deleted_at: string | null;
  
  // Relations
  locations: Location[];
}
```

## Favorite Model

Represents user favorites.

```typescript
interface Favorite {
  id: string;
  user_id: string;
  location_id: string;
  created_at: string;
  updated_at: string;
}
```

## Media Model

Represents media assets (images, videos) associated with locations.

```typescript
interface Media {
  id: string;
  location_id: string;
  type: 'image' | 'video';
  url: string;
  alt_text: string | null;
  order: number;
  created_at: string;
  updated_at: string;
}
```

## Opening Hours Model

Defines opening hours for locations.

```typescript
interface OpeningHours {
  monday: DayHours;
  tuesday: DayHours;
  wednesday: DayHours;
  thursday: DayHours;
  friday: DayHours;
  saturday: DayHours;
  sunday: DayHours;
}

interface DayHours {
  open: string;
  close: string;
  is_closed: boolean;
}

type DayOfWeek = 'monday' | 'tuesday' | 'wednesday' | 'thursday' | 'friday' | 'saturday' | 'sunday';
```

## Filter Models

### Location Filters

```typescript
interface LocationFilters {
  category?: string[];
  rating?: number;
  min_rating?: number;
  price_range?: string[];
  amenities?: string[];
  open_now?: boolean;
  search?: string;
  distance?: number;
  latitude?: number;
  longitude?: number;
}
```

### Pagination

```typescript
interface PaginationParams {
  page: number;
  page_size: number;
  sort_by?: string;
  sort_order?: 'asc' | 'desc';
}

interface PaginatedResponse<T> {
  data: T[];
  pagination: {
    page: number;
    page_size: number;
    total_items: number;
    total_pages: number;
  };
}
```

## Search Models

### Location Search

```typescript
interface LocationSearch {
  query: string;
  filters: LocationFilters;
  location?: {
    latitude: number;
    longitude: number;
  };
  radius?: number;
}
```

## Webhook Models

```typescript
interface Webhook {
  id: string;
  user_id: string;
  url: string;
  events: string[];
  is_active: boolean;
  created_at: string;
  updated_at: string;
  deleted_at: string | null;
}

interface CreateWebhook {
  url: string;
  events: string[];
}

interface UpdateWebhook {
  url?: string;
  events?: string[];
  is_active?: boolean;
}
```

## Enums

### Rating Scale

```typescript
enum Rating {
  ONE = 1,
  TWO = 2,
  THREE = 3,
  FOUR = 4,
  FIVE = 5
}
```

### Media Type

```typescript
enum MediaType {
  IMAGE = 'image',
  VIDEO = 'video'
}
```

### Location Status

```typescript
enum LocationStatus {
  ACTIVE = 'active',
  ARCHIVED = 'archived',
  DELETED = 'deleted'
}
```

### User Status

```typescript
enum UserStatus {
  ACTIVE = 'active',
  INACTIVE = 'inactive',
  SUSPENDED = 'suspended'
}
```

## Validation Rules

### Email
- Format: Standard email format
- Validation: RFC 5322 compliant
- Max length: 254 characters

### Password
- Minimum length: 12 characters
- Maximum length: 128 characters
- Requirements:
  - At least one uppercase letter
  - At least one lowercase letter
  - At least one number
  - At least one special character

### Latitude
- Range: -90 to 90
- Precision: 6 decimal places

### Longitude
- Range: -180 to 180
- Precision: 6 decimal places

### Rating
- Range: 1 to 5
- Must be a whole number

### URLs
- Must be valid HTTPS URLs for production
- Maximum length: 2048 characters

## Data Relationships

### User → Location (Favorites)
- One user can have many favorites
- One location can be favorited by many users

### User → Review
- One user can write many reviews
- One location can have many reviews

### Location → Review
- One location can have many reviews
- One review belongs to one location

### Location → Category
- One location belongs to one category
- One category can have many locations

### Location → Media
- One location can have many media items
- One media item belongs to one location

### User → Webhook
- One user can have many webhooks
- One webhook belongs to one user

## Data Constraints

### Unique Constraints
- User email (must be unique)
- Location name + category (within the same category)
- Favorite user_id + location_id (unique per user)

### Indexes
- Email index (for authentication)
- Location coordinates (for geospatial queries)
- Category ID (for category filtering)
- Created_at (for sorting by date)
- User ID (for user-specific queries)

## Soft Delete Policy

All models support soft deletes:
- Deleted items remain in the database
- `deleted_at` field stores the deletion timestamp
- Queries automatically filter out soft-deleted items
- Restore functionality available for admin users

## API Response Models

### Standard Response

```typescript
interface ApiResponse<T> {
  success: boolean;
  data: T | null;
  error?: {
    code: string;
    message: string;
    details?: any[];
  };
  meta: {
    timestamp: string;
    request_id: string;
    version: string;
  };
}
```

### Error Response

```typescript
interface ErrorResponse {
  success: false;
  error: {
    code: string;
    message: string;
    details?: ErrorDetail[];
    request_id: string;
    timestamp: string;
  };
}

interface ErrorDetail {
  field: string;
  message: string;
  code?: string;
}
```