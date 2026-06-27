# Sprint 10: Admin Metrics API

**Duration**: 2026-06-27  
**Status**: ✅ Completed  
**Focus**: Implement live user analytics API for admin dashboard  

## Goals

- Implement Admin Metrics API with live user analytics
- Replace mock data in admin dashboard with real API calls
- Add PostgreSQL-native date aggregation with indexed queries
- Enforce RBAC protection for ADMIN role only

## Completed Tasks

### ✅ Domain Models
- Added `UsersGrowthPoint`, `UsersMetrics`, `MetricsRange` to `shared/bornemap-core/src/lib.rs`
- Extended `UserRepository` trait with `count_users` and `users_growth_by_day` methods
- Added validation for time range query parameters (7d, 30d, 90d, 365d)

### ✅ Database Layer  
- Created migration `202406270004_add_users_created_at_index.sql`
- Implemented `PgUserRepository` methods with PostgreSQL queries
- Used `created_at::date` and `date_trunc` for efficient aggregation
- Added index on `users.created_at` for performance

### ✅ Application Layer
- Created `application/metrics.rs` use case for business logic
- Implemented `GetUsersMetricsUseCase` with clean architecture separation
- Added proper error handling without `unwrap()` in production code

### ✅ HTTP Layer
- Implemented `GET /api/v1/admin/metrics/users` endpoint with RBAC protection
- Added query validation for `range` parameter
- Used JWT token validation for ADMIN role enforcement
- Returned data in global response envelope format

### ✅ Frontend Integration
- Updated `DashboardPage` to consume live API data
- Replaced mock chart data with real user growth metrics
- Added loading states and error handling
- Maintained responsive design with Framer Motion animations

### ✅ Documentation
- Updated API contract with new endpoint specifications
- Added authorization matrix entry for metrics endpoint
- Updated validation rules for range parameter
- Incremented API version to v15

## Technical Implementation

### Backend (Rust/Actix-web)
```rust
// Domain models
pub struct UsersGrowthPoint {
    pub date: NaiveDate,
    pub count: i64,
}

pub struct UsersMetrics {
    pub total: i64,
    pub growth: Vec<UsersGrowthPoint>,
}

// PostgreSQL query
SELECT 
  date_trunc('day', created_at)::date as date,
  COUNT(*) as count
FROM users 
WHERE created_at >= $1 
GROUP BY date 
ORDER BY date;
```

### Frontend (React/TypeScript)
```typescript
interface UsersGrowthPoint {
  date: string;
  count: number;
}

interface DashboardMetrics {
  total: number;
  growth: UsersGrowthPoint[];
}

// API call
const { data } = useQuery<DashboardMetrics>({
  queryKey: ['dashboard', 'metrics'],
  queryFn: () => api.get('/api/v1/admin/metrics/users', {
    params: { range: '30d' }
  }),
  refetchInterval: 30_000,
});
```

## API Endpoint

### GET /api/v1/admin/metrics/users

**Authorization**: Bearer JWT (ADMIN role required)

**Query Parameters**:
- `range` (optional): `7d`, `30d`, `90d`, `365d` (default: `30d`)

**Response**:
```json
{
  "data": {
    "total": 1250,
    "growth": [
      {
        "date": "2026-06-20",
        "count": 45
      },
      {
        "date": "2026-06-21",
        "count": 52
      }
    ]
  },
  "meta": null,
  "error": null
}
```

## Performance Optimizations

- PostgreSQL index on `users.created_at` for fast date queries
- Efficient date aggregation using `date_trunc`
- Frontend refetches data every 30 seconds for live updates
- Proper error boundaries and loading states

## Testing

- Unit tests for domain models and validation
- Integration tests for PostgreSQL queries
- HTTP endpoint tests with RBAC enforcement
- Frontend component tests with mock API responses

## Next Steps

- Sprint 11: Additional admin metrics (active users, new users today, total trackers)
- Sprint 12: Real-time dashboard updates with WebSocket
- Sprint 13: Export functionality for metrics data

## Files Modified

- `shared/bornemap-core/src/lib.rs` - Domain models
- `shared/bornemap-db/migrations/202406270004_add_users_created_at_index.sql` - Database migration
- `services/auth-service/src/infrastructure/pg_user_repo.rs` - Repository implementation
- `services/auth-service/src/application/metrics.rs` - Use case
- `services/auth-service/src/http/admin_metrics.rs` - HTTP handler
- `services/auth-service/src/http/mod.rs` - Route registration
- `apps/admin-dashboard/src/features/dashboard/DashboardPage.tsx` - Frontend integration
- `docs/API_CONTRACT.md` - Updated API contract
- `docs/sprints/sprint_10.md` - Sprint documentation
