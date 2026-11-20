# Comments

Comment the why, and only when the code cannot state it: a constraint from
outside the file, a deliberate omission, or a decision whose alternative a
reader would otherwise pick.

If a competent reader could infer it from the code, the code already says it.
Rustdoc (`///`, `//!`) is always fine — it documents the public surface.

# Code Style and Lints

- Follow [Google's Rust Style Guide](https://google.github.io/styleguide/rustguide.md).
- Do not suppress Clippy warnings inline (`#[allow(...)]` or `#[expect(...)]`) at all. Resolve the warning or
  restructure the code instead.
- Avoid using leading underscores (`_`) on identifiers, variables, parameters, or struct fields.
- Code formatting must be verified via `cargo +nightly fmt --check`.
- Always check local repo `.md` files whether ignored or not.

# Framework-Agnostic Documentation & Codebase

- Do not mention any web frameworks (e.g., AdonisJS, Laravel, Express, Axum, Actix, Rails, Django, Druto) in rustdocs,
  codebase comments, identifiers, or error messages.
- Reference only RFCs, W3C standards, official specifications, and Kouta types.

# Skills

Activate and adhere to the following specialized skills when applicable:

- **`rust-best-practices`**: Use when writing, reviewing, or refactoring Rust code, designing types and error handling
  with `Result`, choosing ownership/borrowing patterns, or optimizing performance.
- **`rust-testing`**: Use when writing unit, integration, async, or property-based tests, setting up mocks, or measuring
  test coverage.
- **`codebase-design`**: Use when designing module interfaces, defining seams and boundaries, or improving testability
  and code structure.
- **`diagnosing-bugs`**: Use when investigating build failures, runtime errors, panics, or performance regressions.

# `kouta` Architecture & Design Rules

## Scope

This crate owns multi-driver caching backends, multi-tier storage coordination (L1 memory + L2 Redis), single-flight
cache stampede defense, soft TTL / stale-while-revalidate grace periods, and timestamp-based tag invalidation
for Rust applications.

## Design Rules

1. **Precision & Thread Safety**:
    - Cache store drivers must be strictly `Send + Sync + 'static` to support execution across async tasks and threads.
    - All driver state modifications (increments, decrements, atomic adds, entry expiration) must be race-condition
      free.
    - In-memory drivers must avoid unbounded heap growth through bounded eviction policies and capacity limits.

2. **Stampede Defense Invariant**:
    - `remember()` and `remember_forever()` must ensure single-flight execution per key under concurrent access.
    - If $N$ concurrent tasks request the same non-existent or expired key, the factory future must be executed exactly
      once, and the resulting value distributed to all waiting tasks.
    - If the factory execution panics, times out, or fails, in-flight locks must be released immediately without
      poisoning subsequent callers.

3. **Stale-While-Revalidate & Grace Invariant**:
    - Entries with grace configured must calculate physical driver TTL as `logical_ttl + grace_duration`.
    - If an entry is within its grace window and the background factory fails, the graced value must be returned as
      fallback (`stale-if-error`).
    - Stale revalidation tasks must be decoupled from caller latency where configured.

4. **Tag Invalidation Correctness**:
    - Tag invalidation must follow the timestamp vector pattern: invalidating a tag sets a monotonically increasing
      timestamp on the tag namespace key.
    - An entry associated with tags is valid if and only if its `created_at` timestamp is strictly greater than the
      latest invalidation timestamp of every associated tag.

5. **Tiered Store & Cluster Bus Coordination**:
    - In `TieredCacheStore`, reads check L1 memory first before falling back to L2 Redis; successful L2 reads must
      backfill L1 memory.
    - Writes and deletions must update both tiers.
    - When the Redis synchronization bus is active, remote mutations must trigger local L1 invalidation without cyclic
      message loops.

6. **Fault Tolerance & Resilience**:
    - Distributed driver failures (e.g. Redis timeouts or disconnections) must degrade gracefully: `TieredCacheStore`
      must fall back to local L1 memory operations rather than crashing caller requests.

## Testing Requirements

- **Store Roundtrips**: Test `get`, `put`, `put_many`, `add`, `pull`, `forget`, `forget_many`, `increment`, `decrement`,
  and `flush` across `MemoryCacheStore`, `RedisCacheStore` (using `RedisFake`), and `TieredCacheStore`.
- **TTL Expiration**: Verify deterministic expiration of keys across drivers upon TTL elapsing.
- **Stampede Defense**: Simulate concurrent task bursts on an empty key and assert the factory closure executes exactly
  once.
- **Stale-While-Revalidate**: Verify that expired entries within grace period serve stale values while revalidation
  occurs, and verify fallback behavior when revalidation yields an error.
- **Tag Invalidation**: Verify that invalidating tag $T$ invalidates all keys tagged with $T$, while leaving keys
  tagged with $U$ intact.
- **Tiered Cache & Backfill**: Verify that reading a key present only in L2 backfills L1, and subsequent reads hit L1.
- **Cross-Node Bus Synchronization**: Test that simulated pub/sub invalidation messages evict matching entries from
  local in-memory caches.
