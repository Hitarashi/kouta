# Kouta

[![Crates.io](https://img.shields.io/badge/crates.io-v0.1.0-orange.svg)](https://crates.io/crates/kouta)
[![Documentation](https://docs.rs/kouta/badge.svg)](https://docs.rs/kouta)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

A high-performance, framework-agnostic multi-driver and multi-tier caching library for Rust.

Kouta provides sub-microsecond in-memory performance alongside horizontally scalable distributed storage,
automatic single-flight cache stampede defense, soft TTL / stale-while-revalidate (RFC 5861) grace periods,
and $O(1)$ timestamp-vector tag invalidation.

## Features

- **Multiple Storage Drivers**:
  - `MemoryCacheStore`: High-performance, concurrent, bounded in-memory cache.
  - `FileCacheStore`: Local filesystem disk persistence with atomic writes.
  - `RedisCacheStore`: Distributed asynchronous Redis caching backed by `fred` (feature-gated).
  - `TieredCacheStore`: Hybrid two-tier caching (L1 Memory + L2 Distributed) with automatic backfill and fallback insurance.
  - `NullCacheStore`: Zero-allocation no-op store for disabling caching in development or testing.
- **Cache Stampede Defense**: In-flight single-flight request coalescing ensures an expensive factory closure executes exactly once across concurrent requests on a cache miss.
- **Stale-While-Revalidate (RFC 5861)**: Serve stale data within an allowed grace window while revalidating asynchronously in the background.
- **Tag-Based Invalidation**: Fast $O(1)$ timestamp-vector invalidation across tagged entries without expensive wildcard scans.
- **Observability**: Rich asynchronous lifecycle events (`Hit`, `Miss`, `Put`, `Forget`, `StaleRevalidate`, `TierBackfill`).

## Installation

Add `kouta` to your `Cargo.toml`:

```toml
[dependencies]
kouta = "0.1.0"
```

To enable the distributed Redis backend:

```toml
[dependencies]
kouta = { version = "0.1.0", features = ["redis"] }
```

## Quick Start

### In-Memory Cache

```rust,no_run
use std::{sync::Arc, time::Duration};
use kouta::{Cache, MemoryCacheStore};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(MemoryCacheStore::new());
    let cache = Cache::new(store);

    // Store a value with a 1-hour TTL
    cache.put("user:42", &"Alice", Duration::from_secs(3600)).await?;

    // Retrieve the value
    let user: Option<String> = cache.get("user:42").await?;
    assert_eq!(user, Some("Alice".to_string()));

    // Atomic stampede-safe computation (single-flight)
    let profile = cache
        .remember("profile:42", Duration::from_secs(300), || async {
            // Expensive computation or database query
            Ok::<String, kouta::CacheError>("UserProfileData".to_string())
        })
        .await?;

    Ok(())
}
```

### Hybrid Multi-Tier Cache (L1 Memory + L2 Redis)

```rust,no_run
#[cfg(feature = "redis")]
use std::{sync::Arc, time::Duration};
#[cfg(feature = "redis")]
use kouta::{Cache, MemoryCacheStore, RedisCacheStore, TieredCacheStore};

#[cfg(feature = "redis")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let memory = MemoryCacheStore::new();
    let fred_client = fred::clients::Client::default();
    fred_client.connect();
    fred_client.wait_for_connect().await?;

    let redis_store = Arc::new(RedisCacheStore::new(Arc::new(fred_client)));
    let tiered = Arc::new(TieredCacheStore::new(memory, redis_store));
    let cache = Cache::new(tiered);

    // Reads check L1 first; on miss, fetch from L2 and automatically backfill L1.
    cache.put("config:site", &"Enabled", Duration::from_secs(7200)).await?;

    Ok(())
}
```

### Tag Invalidation

```rust,no_run
use std::{sync::Arc, time::Duration};
use kouta::{Cache, MemoryCacheStore};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(MemoryCacheStore::new());
    let cache = Cache::new(store);

    // Tag entries during insertion
    cache.tags(&["users", "org:1"])
        .put("user:101", &"Bob", Duration::from_secs(3600))
        .await?;

    // Invalidate all entries tagged with "users"
    cache.invalidate_tag("users").await?;

    // Key is now considered logically expired
    assert!(cache.get::<String>("user:101").await?.is_none());

    Ok(())
}
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
