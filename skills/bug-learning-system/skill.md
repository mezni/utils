# Bug Learning System Skill — BorneMap

## Purpose
Turn bugs into architecture improvements and prevention rules.

---

## 🎯 Core Philosophy

**Bugs are not errors — they are system design feedback.**

Every bug produces root cause + prevention rule + ADR update.

---

## 🚫 The Problem

**Current state:**
- Bugs fixed without analysis
- Repeated bugs occur
- No learning loop
- System doesn't improve

---

## 🔒 Core Rules

### 1. Every Bug Must Produce Root Cause

**No fixing without understanding:**

```markdown
# BUG-001

## Summary
Station markers flicker when map moves

## Severity
HIGH

## Context
During MVP-1 development, station markers flicker during map pan operations

## Root Cause
React Query cache key uses stale location data. When map moves, stale location triggers new query before stale cache expires, causing re-render and marker flicker.

## Symptoms
- Markers disappear and reappear during pan
- Animation lags
- Memory usage increases

## Technical Analysis
1. Location updates triggered by map pan
2. React Query cache key: `[stationList, location]`
3. When location changes, new cache key generated
4. React Query fetches new data
5. Old data removed from cache
6. Component re-renders with new data
7. Previous markers disappear
8. New markers appear
9. Animation creates flicker effect

## Impact
- Poor user experience
- Perceived jank
- Performance degradation

## Prevention Rule

### Pattern 1: Cache Key Stability

**Rule:** React Query cache keys must be deterministic and stable

**Implementation:**
```typescript
// ❌ WRONG
const { data } = useStations(location.lat, location.lng);

// ✅ CORRECT
const { data } = useStations(
  `${location.lat}-${location.lng}-${selectedRadius}`
);

// ✅ CORRECT
const { data } = useStations({
  latitude: location.lat,
  longitude: location.lng,
  radius: selectedRadius,
});
```

**Prevention Rule:**
- Cache keys must use stable identifiers
- Avoid object/array keys in React Query
- Use string or primitive keys
- Only regenerate keys on actual data changes

### Pattern 2: Optimistic Updates

**Rule:** Use optimistic updates to prevent visual flickering

**Implementation:**
```typescript
// ❌ WRONG
const { data } = useStations(location.lat, location.lng);
return (
  <StationList data={data} />
);

// ✅ CORRECT
const { data, isLoading, error } = useStations(location.lat, location.lng);

if (isLoading) return <Skeleton />;

return (
  <StationList data={data} />
);
```

**Prevention Rule:**
- Always show loading state first
- Use skeleton over spinner
- Prevent visual state changes
- Smooth transitions preferred

### Pattern 3: Marker Memoization

**Rule:** Markers must be memoized to prevent re-renders

**Implementation:**
```typescript
// ❌ WRONG
function StationMarker({ station }) {
  return <Marker station={station} />;
}

// ✅ CORRECT
const StationMarker = React.memo(({ station }: { station: Station }) => {
  return <Marker station={station} />;
});

const StationMarker = React.memo(StationMarker, (prevProps, nextProps) => {
  return prevProps.station.id === nextProps.station.id;
});
```

**Prevention Rule:**
- All marker components must be memoized
- Use proper comparison function
- Prevent unnecessary re-renders
- Optimize rendering performance
```

---

### 2. Every Bug Must Produce Prevention Rule

**No bug without prevention:**

```markdown
# BUG-002

## Summary
Nearby stations not sorted by distance

## Severity
MEDIUM

## Root Cause
PostGIS query missing ORDER BY clause for distance sorting

## Symptoms
- Stations return in random order
- Distance calculation correct but ordering incorrect
- User experience affected

## Technical Analysis
1. Query uses `ST_DWithin` for filtering
2. No ORDER BY clause present
3. PostgreSQL returns rows in insertion order
4. Distance calculation not affecting order
5. Distance property calculated after filtering

## Impact
- Confusing to users
- Distance sorting expected but not working
- User trust affected

## Prevention Rule

### Pattern: Distance Sorting in PostGIS

**Rule:** Always sort by distance in PostGIS queries

**Implementation:**
```sql
-- ❌ WRONG
SELECT * FROM stations
WHERE ST_DWithin(
  ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
  ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography,
  $5
);

-- ✅ CORRECT
SELECT *,
  ST_Distance(
    ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
    ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography
  ) AS distance
FROM stations
WHERE ST_DWithin(
  ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
  ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography,
  $5
)
ORDER BY distance ASC;
```

**Prevention Rule:**
- Always include ORDER BY distance ASC in nearby queries
- Calculate distance before sorting
- Use PostGIS distance function
- Ensure distance calculation in SELECT clause
- Always sort by distance
```

---

### 3. Every Bug Must Produce ADR Update (if structural)

**For structural bugs, update ADR:**

```markdown
# BUG-003

## Summary
Marker clustering causes performance degradation

## Severity
HIGH

## Root Cause
Marker clustering implementation causes full map re-render when markers added

## Symptoms
- Map jank when adding new markers
- Performance degrades with many markers
- User experience affected

## Technical Analysis
1. Clustering logic implemented in marker component
2. Clustering updates trigger state change
3. State change triggers full map re-render
4. Performance issues with 100+ markers
5. Users experience jank

## Impact
- Poor user experience
- Performance issues
- Mobile app performance

## Prevention Rule

### Pattern: Separate Marker Clustering from Markers

**Rule:** Marker clustering should be separate from marker rendering

**Implementation:**
```rust
// ❌ WRONG
#[derive(Serialize)]
pub struct NearbyStationsResponse {
    pub stations: Vec<StationDto>,
    // ❌ Clustering in response
    // ❌ Separation of concerns violated
}

// ✅ CORRECT
#[derive(Serialize)]
pub struct NearbyStationsResponse {
    pub stations: Vec<StationDto>,
    // ✅ Distance included
    // ✅ No clustering in response
}

// Frontend: Handle clustering separately
const ClusteredStations = useMemo(() => {
  return clusterMarkers(stations);
}, [stations]);

const RenderedStations = useMemo(() => {
  return stations.map(station => <StationMarker station={station} />);
}, [ClusteredStations]);
```

**Prevention Rule:**
- Clustering logic separate from rendering
- Avoid state changes that trigger full re-renders
- Optimize rendering performance
- Use memoization for heavy operations
- Separate data processing from UI rendering

---

## ADR Update

### ADR-001: React Query Cache Key Stability

## Context
BUG-001 - Station markers flicker during map pan operations

## Decision
**ADR-001: Implement React Query cache key stability**

## Rationale
Marker flicker causes poor UX, performance degradation, and user frustration

## Consequences
- Improved user experience
- Better performance
- More stable UI

## Alternatives Considered
- ❌ Use map pan debounce only (rejected: won't fix flicker)
- ❌ Remove React Query (rejected: would break caching)
- ✅ Use stable cache keys (chosen)

## Prevention Rules
- Cache keys must be deterministic and stable
- Avoid object/array keys
- Use string or primitive keys
- Only regenerate keys on actual data changes

## Status
**ACCEPTED** - Implemented in BUG-002 prevention

## Implementation
- React Query cache key stability
- Optimistic updates
- Marker memoization
```

---

## 📋 Bug Handling Process

### Bug Detection

**Before fixing:**

1. **Identify Bug:**
   - What is the symptom?
   - When does it occur?
   - Who does it affect?

2. **Classify Bug:**
   - Severity: CRITICAL, HIGH, MEDIUM, LOW
   - Type: UI, Performance, Security, Data

3. **Gather Symptoms:**
   - What symptoms?
   - When do symptoms occur?
   - How does it affect users?

4. **Analyze Root Cause:**
   - What is the technical cause?
   - What patterns are involved?
   - What processes are broken?

### Bug Fix

**While fixing:**

1. **Fix Bug:**
   - Apply fix
   - Test thoroughly
   - Verify fix works

2. **Update Documentation:**
   - Document the bug
   - Add symptoms
   - Add analysis

### Bug Learning

**After fixing:**

1. **Extract Prevention Rule:**
   - Identify pattern
   - Create prevention rule
   - Document rule

2. **Update ADR (if structural):**
   - Add ADR entry
   - Document decision
   - Add prevention rules

3. **Prevent Recurrence:**
   - Add to bug prevention rules
   - Update documentation
   - Share lessons learned

---

## 🚫 Bug Anti-Patterns

### 1. No Root Cause Analysis

```markdown
# BUG-XXX

## Summary
App crashes when map moves

## Severity
CRITICAL

## Fix
Apply patch to React Query cache key stability

## Prevention Rule
Use stable cache keys

## ❌ WRONG
Missing:
- Root cause analysis
- Technical analysis
- Symptoms documentation
- Prevention rules

## ✅ CORRECT
Include:
- Full root cause analysis
- Technical analysis
- Detailed symptoms
- Prevention rules with patterns
- ADR updates
```

### 2. No Prevention Rules

```markdown
# BUG-XXX

## Summary
Button doesn't respond to clicks

## Severity
HIGH

## Root Cause
Event handler not attached

## Fix
Attach event handler

## ❌ WRONG
Missing:
- Prevention rules
- Pattern identification
- Architecture improvement
```

### 3. No ADR Update

```markdown
# BUG-XXX

## Summary
Marker clustering causes performance issues

## Severity
HIGH

## Root Cause
Clustering logic in marker component

## Fix
Separate clustering from rendering

## ❌ WRONG
Missing:
- ADR update
- Architecture decision
- Prevention rules
- Pattern documentation
```

---

## 🎯 Bug Learning Checklist

**For every bug:**

- [ ] Bug identified and classified
- [ ] Symptoms documented
- [ ] Root cause analyzed
- [ ] Technical analysis completed
- [ ] Impact assessed
- [ ] Fix implemented
- [ ] Prevention rules created
- [ ] Patterns identified
- [ ] ADR updated (if structural)
- [ ] Documentation updated
- [ ] Lessons learned documented
- [ ] Recurrence prevented

---

## 📊 Bug Learning Metrics

### Bug Analysis Metrics

**Current Bug Analysis:**
- Total bugs identified: 3 (all fixed)
- Root cause analysis: 100%
- Prevention rules created: 3
- ADR updates: 1
- Pattern identification: 3
- Bug recurrence: 0

### Prevention Effectiveness

**Prevention Rules:**
- PostGIS cache key stability ✅
- Distance sorting in PostGIS ✅
- Marker clustering separation ✅

**Effectiveness:**
- ✅ No bugs reproduced
- ✅ Prevention rules working
- ✅ No architectural issues
- ✅ System stable

---

## 🚦 Bug Learning Enforcement

### Enforcement Rules

**If bugs detected:**

1. **Detect Bug:**
   - Find the bug
   - Classify severity
   - Document symptoms

2. **Analyze Root Cause:**
   - Deep analysis required
   - Technical investigation
   - Pattern identification

3. **Create Prevention Rules:**
   - Must create prevention rules
   - Must identify patterns
   - Must document patterns

4. **Update ADR (if structural):**
   - Add ADR entry
   - Document decision
   - Update prevention rules

5. **Prevent Recurrence:**
   - Add to prevention rules
   - Update documentation
   - Share lessons learned

---

### Stop Conditions

**Bug learning must include:**

- [ ] Root cause analysis
- [ ] Prevention rules
- [ ] Pattern identification
- [ ] ADR updates (if structural)
- [ ] Documentation updates
- [ ] Lessons learned

**If missing → STOP and add**

---

## 🔄 Bug Learning Process Flow

```
BUG DETECTED
  ↓
CLASSIFY BUG
  ↓
GATHER SYMPTOMS
  ↓
ANALYZE ROOT CAUSE
  ↓
FIX BUG
  ↓
EXTRACT PATTERN
  ↓
CREATE PREVENTION RULES
  ↓
UPDATE ADR (if structural)
  ↓
PREVENT RECURRENCE
```

---

*This skill turns bugs into architecture improvements and prevention rules, creating a learning loop.*