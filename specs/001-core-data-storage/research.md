# Research: Core Data & Storage Foundations

## Technology Decisions

### Decision: PostgreSQL 16 + PostGIS 3.4

**Rationale**: Constitution §2 mandates PostgreSQL + PostGIS with PostGIS SQL
function `ST_DWithin` for spatial queries. The `postgis/postgis:16-3.4` image
is the latest stable PostGIS version on PostgreSQL 16, providing both geometry
and geography type support.

**Alternatives considered**:
- PostgreSQL 15 + PostGIS 3.3: Adequate but older; no reason to not use latest
- MySQL + GIS extensions: Insufficient PostGIS-level spatial function support
- MongoDB + GeoJSON: No compile-time SQL guarantees, violates constitution §7

### Decision: Geography type for distance calculations

**Rationale**: The geography type in PostGIS uses a spherical earth model,
providing accurate geodesic distances over the Tunisian terrain. Geometry type
with planar math would introduce distortion at the scale (~600km from Tunis
to Sfax). FR-005 requires "accurate distance calculations over the Earth's
surface."

**Alternatives considered**:
- Geometry type with `ST_Transform` to a local projection (UTM 32N): More
  complex, requires per-region projection management. Geography is simpler and
  meets the 1m accuracy requirement.

### Decision: osm2pgsql for OSM ingestion

**Rationale**: Osm2pgsql is the mature, well-maintained standard for loading
OSM PBF data into PostGIS. It supports flexible style files to select only
relevant map features (roads, places, boundaries), keeping the `gis` schema
lean.

**Alternatives considered**:
- osmium + custom scripts: More flexible but more code to maintain
- Imposm: Good but adds a Python dependency; osm2pgsql is more universal
- Custom Go parser: Unnecessary — standard tools exist

### Decision: Named Docker volume for data persistence

**Rationale**: Standard PostGIS Docker pattern. A named volume (`pgdata`)
ensures seed data and OSM imports survive container restarts. FR-008's
idempotency requirement means re-init is safe but unnecessary on restart.

**Alternatives considered**:
- Bind mount: Works but less portable across OS and CI environments
- No volume (ephemeral): Would require re-import on every restart — poor UX

### Decision: NanoID entity prefixes for primary keys

**Rationale**: Constitution §4 defines entity ID prefixes (OPR_, STA_, CHG_).
NanoID provides URL-safe, collision-resistant string IDs without sequential
enumeration.

**Alternatives considered**:
- UUID v4: Standard but without entity-type prefix (harder to identify in logs)
- Serial integers: Sequential, guessable, exposes record count
- ULID: Good but adds complexity; NanoID with prefixes is simpler

### Decision: Single init.sql for schema + seed + function

**Rationale**: For MVP-1 local dev, a single `init.sql` executed on container
first-start (PostgreSQL's `/docker-entrypoint-initdb.d/` convention) is the
simplest approach. The file is organized into clearly commented sections
(schemas, tables, seed data, spatial function).

**Alternatives considered**:
- Migration framework (Flyway, Sqitch): Over-engineering for MVP-1 local dev
  where the database is regularly wiped and rebuilt
- Multiple init scripts: Possible but single file is simpler to read in order
