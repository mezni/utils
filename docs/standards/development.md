# Development Standards

This document establishes the code quality, testing, and development standards for the BorneMap project.

## Rust Backend Standards

### Code Style

#### Formatting
- All Rust code must follow `cargo fmt` standards
- Use `cargo fmt --all` before committing
- No manual formatting preference over tooling

#### Clippy Lints
- All code must pass `cargo clippy --all-targets -- -D warnings`
- No warnings, no errors
- Address all clippy suggestions before merging

#### Naming Conventions
- Use idiomatic Rust naming:
  - Structs and Enums: `PascalCase`
  - Functions and Methods: `snake_case`
  - Constants: `SCREAMING_SNAKE_CASE`
  - Private fields: `underscore_prefix`
  - Public API: `camelCase`

#### Error Handling
```rust
// ✅ GOOD: Use Result and propagate errors
fn process_data(data: &[u8]) -> Result<String, Error> {
    let parsed = parse(data)?;
    if !parsed.is_valid() {
        return Err(Error::ValidationFailed);
    }
    Ok(format_output(parsed))
}

// ❌ BAD: Use unwrap() or expect()
fn process_data(data: &[u8]) -> String {
    let parsed = parse(data).unwrap(); // BAD
    format_output(parsed)
}
```

### Type Safety

#### Use Type State Pattern
```rust
struct StateMachine {
    // Private fields
}

impl StateMachine {
    fn new() -> Self {
        // Constructor enforces valid initial state
    }
    
    fn advance_to_next(&mut self) -> Result<(), Error> {
        // Enforce state transitions
        Ok(())
    }
}
```

#### Newtype Pattern for Validation
```rust
struct Email(String);

impl Email {
    fn new(email: &str) -> Result<Self, ValidationError> {
        if !email.contains('@') {
            return Err(ValidationError::InvalidEmail);
        }
        Ok(Email(email.to_string()))
    }
    
    fn as_str(&self) -> &str {
        &self.0
    }
}
```

### Testing Standards

#### Unit Tests
Place tests inline at the end of implementation files:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_valid_email_creation() {
        let email = Email::new("test@example.com").unwrap();
        assert_eq!(email.as_str(), "test@example.com");
    }
    
    #[test]
    fn test_invalid_email_creation() {
        let result = Email::new("invalid-email");
        assert!(result.is_err());
    }
    
    #[test]
    fn test_state_machine_transitions() {
        let mut sm = StateMachine::new();
        assert!(sm.advance_to_next().is_ok());
        assert!(sm.advance_to_next().is_err()); // Can't advance past end
    }
}
```

#### Integration Tests
Place in `tests/` directory at workspace root:

```rust
// tests/integration_test.rs
use borne_map_api::services::location_service;

#[tokio::test]
async fn test_location_service_integration() {
    let service = location_service::LocationService::new();
    
    let result = service.create_location(LocationCreate {
        name: "Test Location",
        latitude: 40.7128,
        longitude: -74.0060,
        category_id: "1".to_string(),
    }).await;
    
    assert!(result.is_ok());
    
    let created = result.unwrap();
    assert_eq!(created.name, "Test Location");
}
```

#### Documentation Tests
Write runnable examples in documentation:

```rust
/// Creates a new location with the given parameters.
///
/// # Arguments
/// * `name` - The name of the location
/// * `latitude` - Latitude coordinate
/// * `longitude` - Longitude coordinate
/// * `category_id` - Category identifier
///
/// # Returns
/// A `Result` containing the created location or an error
///
/// # Examples
/// ```
/// use borne_map_api::models::LocationCreate;
///
/// let location = LocationCreate::new(
///     "Central Park",
///     40.7829,
///     -73.9654,
///     "1".to_string()
/// ).unwrap();
/// assert_eq!(location.name, "Central Park");
/// ```
fn create_location(name: &str, latitude: f64, longitude: f64, category_id: String) -> Result<Location, Error> {
    // Implementation
}
```

### Performance Considerations

#### Avoid Unnecessary Cloning
```rust
// ✅ GOOD: Use references
fn process_items(items: &[Item]) -> Vec<ProcessedItem> {
    items.iter()
        .map(|item| process_item(item))
        .collect()
}

// ❌ BAD: Unnecessary clone
fn process_items(items: Vec<Item>) -> Vec<ProcessedItem> {
    items.into_iter()
        .map(|item| process_item(item))
        .collect()
}
```

#### Use Iterators Efficiently
```rust
// ✅ GOOD: Efficient collection
fn process_many(data: &[Data]) -> Result<Vec<Output>, Error> {
    data.iter()
        .map(|d| process_one(d))
        .collect::<Result<Vec<_>, _>>()
}

// ❌ BAD: Inefficient approach
fn process_many(data: &[Data]) -> Result<Vec<Output>, Error> {
    let mut results = Vec::new();
    for d in data {
        results.push(process_one(d)?);
    }
    Ok(results)
}
```

#### Error Handling Performance
```rust
// ✅ GOOD: Propagate errors efficiently
fn process_nested(data: &NestedData) -> Result<String, Error> {
    let first = process_first(data)?;
    let second = process_second(&first)?;
    Ok(format!("{}-{}", first, second))
}

// ❌ BAD: Nested if-let
fn process_nested(data: &NestedData) -> Option<String> {
    let first = process_first(data)?;
    let second = process_second(&first)?;
    Some(format!("{}-{}", first, second))
}
```

## Frontend Standards

### TypeScript Best Practices

#### Strict Type Checking
```typescript
// ✅ GOOD: Strict typing
interface Location {
  id: string;
  name: string;
  coordinates: {
    latitude: number;
    longitude: number;
  };
}

function createLocation(data: Omit<Location, 'id'>): Location {
  return {
    id: generateUUID(),
    ...data,
  };
}

// ❌ BAD: Loose typing
function createLocation(data: any): Location {
  return data as Location;
}
```

#### Union Types for Variants
```typescript
// ✅ GOOD: Union types
type ApiResponse<T> =
  | { success: true; data: T }
  | { success: false; error: ApiError };

function handleResponse<T>(
  response: ApiResponse<T>
): T | null {
  if (response.success) {
    return response.data;
  }
  return null;
}
```

### React Component Standards

#### Functional Components with Hooks
```typescript
// ✅ GOOD: Modern React
function LocationList({ locations, isLoading }: LocationListProps) {
  if (isLoading) {
    return <LoadingState />;
  }

  return (
    <div className="location-list">
      {locations.map((location) => (
        <LocationCard key={location.id} location={location} />
      ))}
    </div>
  );
}
```

#### Custom Hooks for Logic
```typescript
// ✅ GOOD: Custom hook
function useLocationFilter(locations: Location[]) {
  const [filter, setFilter] = useState<string>('');
  
  const filtered = useMemo(() => {
    return locations.filter(loc => 
      loc.name.toLowerCase().includes(filter.toLowerCase())
    );
  }, [locations, filter]);

  return { filter, setFilter, filtered };
}
```

### API Client Standards

#### Type-Safe API Client
```typescript
// ✅ GOOD: Type-safe API calls
interface LocationService {
  getLocations(filters?: LocationFilters): Promise<PaginatedResponse<Location>>;
  getLocation(id: string): Promise<Location>;
  createLocation(data: CreateLocation): Promise<Location>;
  updateLocation(id: string, data: UpdateLocation): Promise<Location>;
  deleteLocation(id: string): Promise<void>;
}

class LocationApiClient implements LocationService {
  async getLocations(filters?: LocationFilters): Promise<PaginatedResponse<Location>> {
    const response = await api.get('/locations', { params: filters });
    return response.data;
  }
  
  async getLocation(id: string): Promise<Location> {
    const response = await api.get(`/locations/${id}`);
    return response.data;
  }
  
  // ... other methods
}
```

### Testing Standards

#### Unit Tests with React Testing Library
```typescript
// ✅ GOOD: Comprehensive tests
describe('LocationCard', () => {
  it('renders location name', () => {
    const location = createMockLocation();
    render(<LocationCard location={location} />);
    
    expect(screen.getByText(location.name)).toBeInTheDocument();
  });

  it('shows rating with stars', () => {
    const location = createMockLocation({ rating: 4.5 });
    render(<LocationCard location={location} />);
    
    const stars = screen.getAllByRole('img', { name: 'star' });
    expect(stars).toHaveLength(5);
  });

  it('handles loading state', () => {
    render(<LocationCard isLoading={true} />);
    expect(screen.getByRole('progressbar')).toBeInTheDocument();
  });
});
```

#### Integration Tests with Vitest
```typescript
// ✅ GOOD: API integration tests
describe('Location API', () => {
  it('creates location successfully', async () => {
    const locationData = {
      name: 'Test Location',
      latitude: 40.7128,
      longitude: -74.0060,
      category_id: '1',
    };

    const result = await api.post('/locations', locationData);
    expect(result.status).toBe(201);
    expect(result.data.name).toBe('Test Location');
  });

  it('handles 404 for non-existent location', async () => {
    const result = await api.get('/locations/non-existent');
    expect(result.status).toBe(404);
  });
});
```

## Code Review Checklist

### Rust
- [ ] All code passes `cargo fmt`
- [ ] All code passes `cargo clippy -- -D warnings`
- [ ] All tests pass (`cargo test`)
- [ ] New functions have documentation comments
- [ ] Error handling uses `Result<T, E>` not `unwrap()`/`expect()`
- [ ] Type safety enforced at compile-time
- [ ] No `unsafe` blocks used unless justified

### Frontend
- [ ] TypeScript strict mode enabled
- [ ] All components use functional patterns
- [ ] Hooks are custom where applicable
- [ ] API calls are type-safe
- [ ] Error boundaries implemented
- [ ] Loading states included
- [ ] All tests pass

### Testing
- [ ] Unit tests for all business logic
- [ ] Integration tests for API endpoints
- [ ] Component tests for UI components
- [ ] Tests cover both happy and unhappy paths
- [ ] No tests marked as skipped or ignored

### Security
- [ ] No hardcoded secrets or credentials
- [ ] Input validation implemented
- [ ] XSS protection in place
- [ ] CSRF tokens for state-changing requests
- [ ] Rate limiting applied where appropriate
- [ ] SQL injection prevented (parameterized queries)

### Code Quality
- [ ] Functions have single responsibility
- [ ] No code duplication
- [ ] Names are descriptive and meaningful
- [ ] Code is properly commented
- [ ] Complex logic has detailed explanations
- [ ] Follows project coding standards

## Performance Standards

### Rust
- Avoid unnecessary memory allocations
- Use efficient data structures (HashMap, Vec vs HashMap<String, Vec<>>)
- Implement proper caching for expensive operations
- Use benchmarking to identify hot paths

### Frontend
- Lazy load components and data
- Implement virtual scrolling for large lists
- Optimize bundle size (code splitting, tree shaking)
- Use React.memo and useMemo judiciously
- Implement proper data fetching strategies

## Accessibility Standards

### WCAG 2.1 AA Compliance
- All interactive elements have keyboard focus styles
- Color contrast ratio ≥ 4.5:1 for text
- All images have alt text
- Form labels are properly associated
- ARIA attributes used correctly
- Screen reader friendly navigation

## Documentation Standards

### Inline Documentation
- Public functions and types must have documentation
- Complex logic must have detailed explanations
- Examples in documentation comments
- Parameters and return values documented

### Project Documentation
- README.md kept up to date
- API documentation updated with changes
- Architecture decisions documented
- Contribution guidelines clear
- Troubleshooting guides available

## Git Workflow

### Commit Messages
- Use conventional commits format
- Start with type: (feat, fix, docs, style, refactor, test, chore)
- Keep messages concise but descriptive
- Include issue reference if applicable
- Example: `feat(locations): add category filtering`

### Branch Naming
- Use feature branch naming: `sprint-[X]-[feature-name]`
- Keep names descriptive and concise
- Example: `sprint-1-location-search`

### Pull Requests
- Reference related issues
- Include comprehensive description
- Self-review checklist
- Request review from appropriate team members
- Require CI/CD approval before merging