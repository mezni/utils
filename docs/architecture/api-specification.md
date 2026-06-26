# API Specification

This document defines the complete API specification for the BorneMap project.

## Base URL

```
Development: http://localhost:8080/api/v1
Production: https://borne-map-api.example.com/api/v1
```

## Authentication

### JWT Token
All API requests require a valid JWT token in the Authorization header:
```
Authorization: Bearer <access_token>
```

### Token Refresh
Token refresh endpoint:
```
POST /auth/refresh
```

## API Versions

Currently supported: v1

## Rate Limiting

- **Public Endpoints**: 100 requests per minute per IP
- **Authenticated Endpoints**: 1000 requests per minute per user
- **Rate limit headers**:
  ```
  X-RateLimit-Limit: 100
  X-RateLimit-Remaining: 95
  X-RateLimit-Reset: 1698765432
  ```

## Response Format

### Success Response
```json
{
  "success": true,
  "data": { ... },
  "meta": {
    "timestamp": "2024-06-25T10:00:00Z",
    "request_id": "uuid"
  }
}
```

### Error Response
```json
{
  "success": false,
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid input data",
    "details": [
      {
        "field": "email",
        "message": "Invalid email format"
      }
    ]
  },
  "meta": {
    "timestamp": "2024-06-25T10:00:00Z",
    "request_id": "uuid"
  }
}
```

## Endpoints

### Authentication

#### Register User
```http
POST /auth/register
Content-Type: application/json

{
  "email": "user@example.com",
  "password": "SecurePassword123!",
  "first_name": "John",
  "last_name": "Doe"
}
```

**Response**: 201 Created
```json
{
  "success": true,
  "data": {
    "user": {
      "id": "uuid",
      "email": "user@example.com",
      "first_name": "John",
      "last_name": "Doe"
    },
    "access_token": "jwt_token",
    "refresh_token": "jwt_token"
  }
}
```

#### Login
```http
POST /auth/login
Content-Type: application/json

{
  "email": "user@example.com",
  "password": "SecurePassword123!"
}
```

**Response**: 200 OK

#### Refresh Token
```http
POST /auth/refresh
Content-Type: application/json

{
  "refresh_token": "refresh_token"
}
```

**Response**: 200 OK

#### Logout
```http
POST /auth/logout
Authorization: Bearer <access_token>
```

**Response**: 200 OK

### User Management

#### Get Current User
```http
GET /users/me
Authorization: Bearer <access_token>
```

**Response**: 200 OK

#### Update User Profile
```http
PATCH /users/me
Authorization: Bearer <access_token>
Content-Type: application/json

{
  "first_name": "Jane",
  "last_name": "Smith"
}
```

**Response**: 200 OK

### Locations

#### Get All Locations
```http
GET /locations?page=1&page_size=20&search=query
Authorization: Bearer <access_token>
```

**Response**: 200 OK
```json
{
  "success": true,
  "data": {
    "locations": [
      {
        "id": "uuid",
        "name": "Central Park",
        "description": "A beautiful park in the city center",
        "latitude": 40.7829,
        "longitude": -73.9654,
        "address": "100 Central Park West, New York, NY",
        "category": "Park",
        "rating": 4.5,
        "reviews_count": 1250,
        "image_url": "https://borne-map.example.com/images/central-park.jpg"
      }
    ],
    "pagination": {
      "page": 1,
      "page_size": 20,
      "total_items": 150,
      "total_pages": 8
    }
  }
}
```

#### Get Location Details
```http
GET /locations/:id
Authorization: Bearer <access_token>
```

**Response**: 200 OK

#### Create Location
```http
POST /locations
Authorization: Bearer <access_token>
Content-Type: application/json

{
  "name": "Coffee Shop",
  "description": "A cozy coffee shop",
  "latitude": 40.7829,
  "longitude": -73.9654,
  "address": "123 Main Street",
  "category": "Cafe",
  "rating": 0,
  "reviews_count": 0
}
```

**Response**: 201 Created

#### Update Location
```http
PATCH /locations/:id
Authorization: Bearer <access_token>
Content-Type: application/json

{
  "name": "Updated Coffee Shop",
  "rating": 4.0
}
```

**Response**: 200 OK

#### Delete Location
```http
DELETE /locations/:id
Authorization: Bearer <access_token>
```

**Response**: 204 No Content

### Categories

#### Get All Categories
```http
GET /categories
Authorization: Bearer <access_token>
```

**Response**: 200 OK
```json
{
  "success": true,
  "data": {
    "categories": [
      {
        "id": "uuid",
        "name": "Restaurant",
        "icon": "restaurant",
        "color": "#FF6B6B"
      },
      {
        "id": "uuid",
        "name": "Park",
        "icon": "park",
        "color": "#4ECDC4"
      }
    ]
  }
}
```

### Reviews

#### Get Reviews for Location
```http
GET /locations/:id/reviews?page=1&page_size=10
Authorization: Bearer <access_token>
```

**Response**: 200 OK

#### Create Review
```http
POST /locations/:id/reviews
Authorization: Bearer <access_token>
Content-Type: application/json

{
  "rating": 5,
  "comment": "Excellent place!",
  "images": ["image_url1", "image_url2"]
}
```

**Response**: 201 Created

#### Update Review
```http
PATCH /locations/:id/reviews/:review_id
Authorization: Bearer <access_token>
Content-Type: application/json

{
  "rating": 4,
  "comment": "Great place, but a bit crowded"
}
```

**Response**: 200 OK

#### Delete Review
```http
DELETE /locations/:id/reviews/:review_id
Authorization: Bearer <access_token>
```

**Response**: 204 No Content

### Favorites

#### Get User Favorites
```http
GET /users/favorites
Authorization: Bearer <access_token>
```

**Response**: 200 OK

#### Add to Favorites
```http
POST /users/favorites/:location_id
Authorization: Bearer <access_token>
```

**Response**: 200 OK

#### Remove from Favorites
```http
DELETE /users/favorites/:location_id
Authorization: Bearer <access_token>
```

**Response**: 204 No Content

## Error Codes

### Client Errors (4xx)
- `400 Bad Request`: Invalid input data
- `401 Unauthorized`: Invalid or expired token
- `403 Forbidden`: Insufficient permissions
- `404 Not Found`: Resource not found
- `422 Unprocessable Entity`: Validation error

### Server Errors (5xx)
- `500 Internal Server Error`: Server-side error
- `503 Service Unavailable`: Service temporarily unavailable

## Rate Limiting Errors

- `429 Too Many Requests`: Rate limit exceeded
- `Retry-After`: Seconds until rate limit resets

## Pagination

All list endpoints support pagination with the following parameters:

- `page`: Page number (default: 1)
- `page_size`: Number of items per page (default: 20, max: 100)

## Search

### Search Locations
```http
GET /locations/search?q=keyword&category=category&latitude=40.7829&longitude=-73.9654&radius=5000
```

Parameters:
- `q`: Search query
- `category`: Filter by category
- `latitude`, `longitude`: Center point for radius search
- `radius`: Search radius in meters

## Filtering

### Location Filters
- `category`: Filter by category
- `rating`: Minimum rating
- `price_range`: Price range filter
- `open_now`: Filter by open status (true/false)
- `amenities`: Filter by amenities

## Sorting

### Sorting Parameters
- `sort_by`: Field to sort by (name, rating, reviews_count, created_at)
- `sort_order`: asc or desc (default: desc for rating, asc for name)

## Webhooks

### Webhook Events
- `location.created`: Location created
- `location.updated`: Location updated
- `review.created`: Review created
- `user.registered`: User registered

### Subscribe to Webhooks
```http
POST /webhooks
Authorization: Bearer <access_token>
Content-Type: application/json

{
  "url": "https://your-site.com/webhooks",
  "events": ["location.created", "review.created"]
}
```

## Data Models

See [Data Models](./data-models.md) for detailed schema definitions.

## Rate Limits

See [Rate Limits](./rate-limits.md) for detailed rate limit information.

## API Documentation

- **Swagger/OpenAPI**: https://borne-map-api.example.com/api-docs
- **Postman Collection**: [Link to Postman collection]

## Support

For API support and issues:
- Email: api-support@borne-map.example.com
- Documentation: https://borne-map.example.com/docs
- Rate limit issues: https://borne-map.example.com/docs/rate-limits