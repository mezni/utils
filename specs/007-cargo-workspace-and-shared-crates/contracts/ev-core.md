# ev-core — Public API Contract

**Crate**: `ev-core` (shared library)

**Location**: `source/crates/ev-core/src/lib.rs`

## Public Functions

### `generate_id`

```rust
pub fn generate_id(prefix: &str, length: usize) -> String
```

Generates a NanoID with the given alphanumeric prefix and random character length.

**Panics**:
- If `length` is 0
- If the default alphabet has fewer than 2 characters (should never happen)

**Example**: `generate_id("PRT", 8)` → `"PRT_kXm9aB3q"`

---

### `generate_id_with_alphabet`

```rust
pub fn generate_id_with_alphabet(prefix: &str, length: usize, alphabet: &str) -> String
```

Generates a NanoID with a custom alphabet.

**Panics**:
- If `length` is 0
- If `alphabet` has fewer than 2 characters

**Example**: `generate_id_with_alphabet("STN", 6, "0123456789")` → `"STN_483910"`

## Public Enum Types

### `ConnectorType`

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectorType {
    Type2,
    Type3,
    CCS,
    CHAdeMO,
}
```

### `ChargerStatus`

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChargerStatus {
    Available,
    InUse,
    Maintenance,
    Offline,
}
```

### `PartnerType`

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PartnerType {
    Business,
    Personal,
}
```

### `StationStatus`

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StationStatus {
    Available,
    Partial,
    Unavailable,
}
```

## Public Error Types

### `EnumParseError`

```rust
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum EnumParseError {
    #[error("unknown {enum_name} variant: {value}")]
    UnknownVariant { enum_name: &'static str, value: String },
}
```

Returned when deserializing an enum from an unrecognized string value.
