# Feature Clarification: Mobile & Web Driver Apps

**Feature Branch**: `004-driver-apps`

**Created**: 2026-06-12

**Status**: ✅ All Clarifications Resolved

## Current State

Specification created with:
- 7 user stories (P1: 3, P2: 3, P3: 1)
- 35 functional requirements
- 10 success criteria
- 13 edge cases covered

## Clarification Questions

### Question 1: Geocoding API Rate Limits

**Context**: We plan to use OSM Nominatim API for geocoding text search.

**Question**: What should be the timeout and retry behavior for OSM Nominatim API calls?

**Options**:
- A) Timeout 5s, retry 3 times with exponential backoff
- B) Timeout 10s, retry 2 times with linear backoff ✅ RESOLVED
- C) Timeout 30s, no retries (treat as permanent failure)

**Preferred**: B

**Reasoning**: OSM Nominatim is a free public API without rate limits for legitimate use. 10s timeout gives enough time for network latency, 2 retries provide resilience for transient failures without overloading the service.

---

### Question 2: Station Image Loading Strategy

**Context**: Station details may include images (stored in database or fetched from URLs).

**Question**: Should we implement lazy loading for station images?

**Options**:
- A) Yes, load images only when station detail is visible ✅ RESOLVED
- B) Yes, load all images when station list loads
- C) No, load images synchronously with station details

**Preferred**: A

**Reasoning**: Lazy loading improves perceived performance, especially for stations with multiple images. Load images only when user taps a station to view details.

---

### Question 3: Error Recovery Strategy for Failed Navigation

**Context**: Navigation button on station detail opens external mapping app.

**Question**: If the selected station address is invalid or navigation cannot be triggered, what should happen?

**Options**:
- A) Show error message in app with copy-to-clipboard button for address ✅ RESOLVED
- B) Open Google Maps directly with a placeholder search query
- C) Show error and ask user to enter address manually

**Preferred**: A

**Reasoning**: Address validation happens in backend. If frontend receives invalid address, showing clear error with copy option gives user maximum control. External mapping apps handle other edge cases better than our app.

---

### Question 4: Theme Persistence

**Context**: Dark mode can be toggled by users.

**Question**: Where should theme preference be stored and persisted?

**Options**:
- A) AsyncStorage (React Native) + localStorage (Web) ✅ RESOLVED
- B) UserDefaults (iOS) + SharedPreferences (Android)
- C) React Query cache

**Preferred**: A

**Reasoning**: AsyncStorage/localStorage provides platform-agnostic storage that persists across app restarts without platform-specific code. React Query cache doesn't survive app restarts, and UserDefaults/SharedPreferences require platform-specific code for cross-platform consistency.

---

### Question 5: Map Marker Clustering

**Context**: Map displays many stations (potentially 1000+).

**Question**: Should we implement marker clustering (group nearby markers)?

**Options**:
- A) Yes, cluster markers within 50m radius
- B) No, show individual markers (let user zoom to separate)
- C) Yes, show cluster badges with counts ✅ RESOLVED

**Preferred**: C

**Reasoning**: Clustering improves performance with 1000+ markers by reducing visible marker count. Cluster badges show user they can zoom to see more stations. Implementing clustering adds complexity but significantly improves UX for dense station areas.

---

### Question 6: Refresh Data Frequency

**Context**: Station availability data may change over time.

**Question**: How frequently should app refresh station data?

**Options**:
- A) Every time user pulls to refresh (manual only)
- B) Manual refresh + automatic refresh every 10 minutes while app is in foreground
- C) Manual refresh + automatic refresh every 30 minutes while app is in foreground
- D) Manual refresh only (no auto-refresh) ✅ RESOLVED

**Preferred**: D

**Reasoning**: Real-time availability is not available yet (backend doesn't provide updates). Auto-refresh would add unnecessary network load. Manual refresh gives users control. Future phases can add live updates.

---

### Question 7: Offline Caching Strategy

**Context**: Users may experience network interruptions.

**Question**: What should be cached offline?

**Options**:
- A) Cache last 50 stations + station details (recently viewed) ✅ RESOLVED
- B) Cache all stations within current radius + all station details
- C) No caching, always fetch from network

**Preferred**: A

**Reasoning**: Caching 50 stations is sufficient for common use cases while keeping cache size manageable. Includes station details for offline reading. Cache can be invalidated and refreshed when network returns.

---

### Question 8: Web App Authentication

**Context**: Web app may require user accounts for certain features.

**Question**: Should the web app support login for saving favorites or history?

**Options**:
- A) Yes, implement account login with OAuth
- B) Yes, implement email/password signup
- C) No, keep web app as public access (no accounts) ✅ RESOLVED
- D) Yes, implement mock login for demo purposes

**Preferred**: C

**Reasoning**: MVP scope focuses on station discovery, not user accounts. Delaying authentication to Phase 5 allows us to focus on core discovery flow. Web app can remain public-access to maximize accessibility.

---

### Question 9: Loading State Granularity

**Context**: App shows multiple types of loading states.

**Question**: What granularity should loading states have?

**Options**:
- A) Global loading spinner for all API requests
- B) Per-screen loading skeletons + global spinner for first load
- C) Per-screen skeletons + minimal global spinner for initial app launch ✅ RESOLVED
- D) Only skeletons, no global spinner

**Preferred**: C

**Reasoning**: Skeleton screens provide immediate visual feedback for data fetching. Global spinner only for initial app launch reduces cognitive load. No global spinner for subsequent navigations keeps UI clean.

---

### Question 10: Map Provider Choice for Mobile

**Context**: Need to choose between Google Maps SDK, Mapbox, or react-native-maps.

**Question**: Which mapping provider should we use for mobile app?

**Options**:
- A) react-native-maps (open-source, no API key needed) ✅ RESOLVED
- B) Mapbox SDK (requires API key and paid tier for >50k maps/day)
- C) Google Maps SDK (requires API key and costs money)

**Preferred**: A

**Reasoning**: react-native-maps is free, open-source, and doesn't require API keys. Perfect for MVP. Mapbox and Google Maps have free tiers but require setup and billing. react-native-maps also provides Leaflet support for web.

---

## Clarification Summary

**Total Questions**: 10
**Resolved**: ✅ All questions have preferred options confirmed by user

**Next Steps**:
1. ✅ Document resolved clarifications (done)
2. ✅ Update spec with final answers
3. Proceed to `/speckit.plan`

## Resolved Decisions

All 10 clarifications have been resolved with the preferred options documented above. These decisions will be integrated into the final specification before proceeding to implementation planning.
