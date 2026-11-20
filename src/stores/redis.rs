//! Asynchronous distributed Redis cache store backed by [`RedisCommands`].

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::RwLock;

use crate::{
    entry::CacheEntry,
    error::CacheError,
    store::{BoxFuture, CacheStore},
};

/// Default key prefix for cache entries stored in Redis.
pub const DEFAULT_REDIS_KEY_PREFIX: &str = "kouta:cache:";

/// Asynchronous Redis command contract required by [`RedisCacheStore`].
pub trait RedisCommands: Send + Sync {
    /// Retrieves a string value by key.
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<String>, CacheError>>;

    /// Sets a key to a string value with a time-to-live expiration in seconds.
    fn set_ex<'a>(
        &'a self,
        key: &'a str,
        value: &'a str,
        seconds: u64,
    ) -> BoxFuture<'a, Result<(), CacheError>>;

    /// Sets a key only if it does not already exist. Returns true if key was set.
    fn set_nx<'a>(
        &'a self,
        key: &'a str,
        value: &'a str,
    ) -> BoxFuture<'a, Result<bool, CacheError>>;

    /// Sets a time-to-live expiration on a key in seconds.
    fn expire<'a>(&'a self, key: &'a str, seconds: u64) -> BoxFuture<'a, Result<bool, CacheError>>;

    /// Deletes a key. Returns true if the key was deleted.
    fn del<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<bool, CacheError>>;

    /// Deletes multiple keys. Returns the count of deleted keys.
    fn del_many<'a>(&'a self, keys: &'a [&'a str]) -> BoxFuture<'a, Result<i64, CacheError>>;

    /// Increments the numeric value of a key by amount.
    fn incr_by<'a>(&'a self, key: &'a str, amount: i64) -> BoxFuture<'a, Result<i64, CacheError>>;

    /// Finds all keys matching pattern.
    fn keys<'a>(&'a self, pattern: &'a str) -> BoxFuture<'a, Result<Vec<String>, CacheError>>;

    /// Pings the Redis server.
    fn ping<'a>(&'a self) -> BoxFuture<'a, Result<(), CacheError>>;
}

#[cfg(feature = "redis")]
impl RedisCommands for fred::clients::Client {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<String>, CacheError>> {
        use fred::interfaces::KeysInterface;
        Box::pin(async move {
            KeysInterface::get::<Option<String>, _>(self, key)
                .await
                .map_err(|err| CacheError::Store(err.to_string()))
        })
    }

    fn set_ex<'a>(
        &'a self,
        key: &'a str,
        value: &'a str,
        seconds: u64,
    ) -> BoxFuture<'a, Result<(), CacheError>> {
        use fred::{interfaces::KeysInterface, types::Expiration};
        Box::pin(async move {
            KeysInterface::set::<(), _, _>(
                self,
                key,
                value,
                Some(Expiration::EX(seconds.max(1) as i64)),
                None,
                false,
            )
            .await
            .map_err(|err| CacheError::Store(err.to_string()))
        })
    }

    fn set_nx<'a>(
        &'a self,
        key: &'a str,
        value: &'a str,
    ) -> BoxFuture<'a, Result<bool, CacheError>> {
        use fred::{interfaces::KeysInterface, types::SetOptions};
        Box::pin(async move {
            let result: Option<String> =
                KeysInterface::set(self, key, value, None, Some(SetOptions::NX), false)
                    .await
                    .map_err(|err| CacheError::Store(err.to_string()))?;
            Ok(result.is_some())
        })
    }

    fn expire<'a>(&'a self, key: &'a str, seconds: u64) -> BoxFuture<'a, Result<bool, CacheError>> {
        use fred::interfaces::KeysInterface;
        Box::pin(async move {
            KeysInterface::expire::<bool, _>(self, key, seconds.max(1) as i64, None)
                .await
                .map_err(|err| CacheError::Store(err.to_string()))
        })
    }

    fn del<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<bool, CacheError>> {
        use fred::interfaces::KeysInterface;
        Box::pin(async move {
            let count: i64 = KeysInterface::del(self, key)
                .await
                .map_err(|err| CacheError::Store(err.to_string()))?;
            Ok(count > 0)
        })
    }

    fn del_many<'a>(&'a self, keys: &'a [&'a str]) -> BoxFuture<'a, Result<i64, CacheError>> {
        use fred::interfaces::KeysInterface;
        Box::pin(async move {
            if keys.is_empty() {
                return Ok(0);
            }
            let keys_vec: Vec<String> = keys.iter().map(|k| (*k).to_string()).collect();
            let count: i64 = KeysInterface::del(self, keys_vec)
                .await
                .map_err(|err| CacheError::Store(err.to_string()))?;
            Ok(count)
        })
    }

    fn incr_by<'a>(&'a self, key: &'a str, amount: i64) -> BoxFuture<'a, Result<i64, CacheError>> {
        use fred::interfaces::KeysInterface;
        Box::pin(async move {
            KeysInterface::incr_by::<i64, _>(self, key, amount)
                .await
                .map_err(|err| CacheError::Store(err.to_string()))
        })
    }

    fn keys<'a>(&'a self, pattern: &'a str) -> BoxFuture<'a, Result<Vec<String>, CacheError>> {
        use fred::{
            interfaces::ClientLike,
            types::{ClusterHash, CustomCommand},
        };
        Box::pin(async move {
            ClientLike::custom::<Vec<String>, String>(
                self,
                CustomCommand::new_static("KEYS", ClusterHash::FirstKey, false),
                vec![pattern.to_string()],
            )
            .await
            .map_err(|err| CacheError::Store(err.to_string()))
        })
    }

    fn ping<'a>(&'a self) -> BoxFuture<'a, Result<(), CacheError>> {
        use fred::interfaces::ClientLike;
        Box::pin(async move {
            ClientLike::ping::<()>(self, None)
                .await
                .map_err(|err| CacheError::Store(err.to_string()))
        })
    }
}

#[cfg(feature = "redis")]
impl RedisCommands for fred::clients::Pool {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<String>, CacheError>> {
        use fred::interfaces::KeysInterface;
        Box::pin(async move {
            KeysInterface::get::<Option<String>, _>(self, key)
                .await
                .map_err(|err| CacheError::Store(err.to_string()))
        })
    }

    fn set_ex<'a>(
        &'a self,
        key: &'a str,
        value: &'a str,
        seconds: u64,
    ) -> BoxFuture<'a, Result<(), CacheError>> {
        use fred::{interfaces::KeysInterface, types::Expiration};
        Box::pin(async move {
            KeysInterface::set::<(), _, _>(
                self,
                key,
                value,
                Some(Expiration::EX(seconds.max(1) as i64)),
                None,
                false,
            )
            .await
            .map_err(|err| CacheError::Store(err.to_string()))
        })
    }

    fn set_nx<'a>(
        &'a self,
        key: &'a str,
        value: &'a str,
    ) -> BoxFuture<'a, Result<bool, CacheError>> {
        use fred::{interfaces::KeysInterface, types::SetOptions};
        Box::pin(async move {
            let result: Option<String> =
                KeysInterface::set(self, key, value, None, Some(SetOptions::NX), false)
                    .await
                    .map_err(|err| CacheError::Store(err.to_string()))?;
            Ok(result.is_some())
        })
    }

    fn expire<'a>(&'a self, key: &'a str, seconds: u64) -> BoxFuture<'a, Result<bool, CacheError>> {
        use fred::interfaces::KeysInterface;
        Box::pin(async move {
            KeysInterface::expire::<bool, _>(self, key, seconds.max(1) as i64, None)
                .await
                .map_err(|err| CacheError::Store(err.to_string()))
        })
    }

    fn del<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<bool, CacheError>> {
        use fred::interfaces::KeysInterface;
        Box::pin(async move {
            let count: i64 = KeysInterface::del(self, key)
                .await
                .map_err(|err| CacheError::Store(err.to_string()))?;
            Ok(count > 0)
        })
    }

    fn del_many<'a>(&'a self, keys: &'a [&'a str]) -> BoxFuture<'a, Result<i64, CacheError>> {
        use fred::interfaces::KeysInterface;
        Box::pin(async move {
            if keys.is_empty() {
                return Ok(0);
            }
            let keys_vec: Vec<String> = keys.iter().map(|k| (*k).to_string()).collect();
            let count: i64 = KeysInterface::del(self, keys_vec)
                .await
                .map_err(|err| CacheError::Store(err.to_string()))?;
            Ok(count)
        })
    }

    fn incr_by<'a>(&'a self, key: &'a str, amount: i64) -> BoxFuture<'a, Result<i64, CacheError>> {
        use fred::interfaces::KeysInterface;
        Box::pin(async move {
            KeysInterface::incr_by::<i64, _>(self, key, amount)
                .await
                .map_err(|err| CacheError::Store(err.to_string()))
        })
    }

    fn keys<'a>(&'a self, pattern: &'a str) -> BoxFuture<'a, Result<Vec<String>, CacheError>> {
        use fred::{
            interfaces::ClientLike,
            types::{ClusterHash, CustomCommand},
        };
        Box::pin(async move {
            ClientLike::custom::<Vec<String>, String>(
                self,
                CustomCommand::new_static("KEYS", ClusterHash::FirstKey, false),
                vec![pattern.to_string()],
            )
            .await
            .map_err(|err| CacheError::Store(err.to_string()))
        })
    }

    fn ping<'a>(&'a self) -> BoxFuture<'a, Result<(), CacheError>> {
        use fred::interfaces::ClientLike;
        Box::pin(async move {
            ClientLike::ping::<()>(self, None)
                .await
                .map_err(|err| CacheError::Store(err.to_string()))
        })
    }
}

/// In-memory Redis test double implementing [`RedisCommands`].
#[derive(Debug, Default)]
pub struct RedisFake {
    storage: RwLock<HashMap<String, (String, Option<Instant>)>>,
}

impl RedisFake {
    /// Constructs a new, empty in-memory Redis fake store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            storage: RwLock::new(HashMap::new()),
        }
    }
}

impl RedisCommands for RedisFake {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<String>, CacheError>> {
        Box::pin(async move {
            let mut store = self.storage.write().await;
            if let Some((value, expiration)) = store.get(key) {
                if let Some(expiry_instant) = expiration {
                    if Instant::now() >= *expiry_instant {
                        store.remove(key);
                        return Ok(None);
                    }
                }
                Ok(Some(value.clone()))
            } else {
                Ok(None)
            }
        })
    }

    fn set_ex<'a>(
        &'a self,
        key: &'a str,
        value: &'a str,
        seconds: u64,
    ) -> BoxFuture<'a, Result<(), CacheError>> {
        Box::pin(async move {
            let mut store = self.storage.write().await;
            let expiry = Instant::now() + Duration::from_secs(seconds.max(1));
            store.insert(key.to_string(), (value.to_string(), Some(expiry)));
            Ok(())
        })
    }

    fn set_nx<'a>(
        &'a self,
        key: &'a str,
        value: &'a str,
    ) -> BoxFuture<'a, Result<bool, CacheError>> {
        Box::pin(async move {
            let mut store = self.storage.write().await;
            if let Some((_, expiration)) = store.get(key) {
                if let Some(expiry_instant) = expiration {
                    if Instant::now() >= *expiry_instant {
                        store.remove(key);
                    } else {
                        return Ok(false);
                    }
                } else {
                    return Ok(false);
                }
            }
            store.insert(key.to_string(), (value.to_string(), None));
            Ok(true)
        })
    }

    fn expire<'a>(&'a self, key: &'a str, seconds: u64) -> BoxFuture<'a, Result<bool, CacheError>> {
        Box::pin(async move {
            let mut store = self.storage.write().await;
            if let Some((_, expiration)) = store.get_mut(key) {
                *expiration = Some(Instant::now() + Duration::from_secs(seconds.max(1)));
                Ok(true)
            } else {
                Ok(false)
            }
        })
    }

    fn del<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<bool, CacheError>> {
        Box::pin(async move {
            let mut store = self.storage.write().await;
            Ok(store.remove(key).is_some())
        })
    }

    fn del_many<'a>(&'a self, keys: &'a [&'a str]) -> BoxFuture<'a, Result<i64, CacheError>> {
        Box::pin(async move {
            let mut store = self.storage.write().await;
            let mut count = 0i64;
            for key in keys {
                if store.remove(*key).is_some() {
                    count += 1;
                }
            }
            Ok(count)
        })
    }

    fn incr_by<'a>(&'a self, key: &'a str, amount: i64) -> BoxFuture<'a, Result<i64, CacheError>> {
        Box::pin(async move {
            let mut store = self.storage.write().await;
            let current_value = if let Some((value, expiration)) = store.get(key) {
                if let Some(expiry_instant) = expiration {
                    if Instant::now() >= *expiry_instant {
                        0i64
                    } else {
                        value.parse::<i64>().unwrap_or(0)
                    }
                } else {
                    value.parse::<i64>().unwrap_or(0)
                }
            } else {
                0i64
            };

            let next_value = current_value + amount;
            let existing_expiration = store.get(key).and_then(|entry| entry.1);
            store.insert(
                key.to_string(),
                (next_value.to_string(), existing_expiration),
            );
            Ok(next_value)
        })
    }

    fn keys<'a>(&'a self, pattern: &'a str) -> BoxFuture<'a, Result<Vec<String>, CacheError>> {
        Box::pin(async move {
            let store = self.storage.read().await;
            let now = Instant::now();
            let prefix = pattern.trim_end_matches('*');

            let matched: Vec<String> = store
                .iter()
                .filter(|(key, (_, expiry))| {
                    if let Some(expiry_instant) = expiry {
                        if now >= *expiry_instant {
                            return false;
                        }
                    }
                    key.starts_with(prefix)
                })
                .map(|(key, _)| key.clone())
                .collect();

            Ok(matched)
        })
    }

    fn ping<'a>(&'a self) -> BoxFuture<'a, Result<(), CacheError>> {
        Box::pin(async move { Ok(()) })
    }
}

/// Asynchronous distributed cache store backed by [`RedisCommands`].
#[derive(Debug, Clone)]
pub struct RedisCacheStore<C> {
    client: Arc<C>,
    prefix: String,
}

impl<C> RedisCacheStore<C> {
    /// Constructs a new Redis cache store using the default key prefix (`kouta:cache:`).
    pub fn new(client: Arc<C>) -> Self {
        Self {
            client,
            prefix: DEFAULT_REDIS_KEY_PREFIX.to_string(),
        }
    }

    /// Constructs a new Redis cache store with a custom key prefix.
    pub fn with_prefix(client: Arc<C>, prefix: impl Into<String>) -> Self {
        Self {
            client,
            prefix: prefix.into(),
        }
    }

    /// Returns a reference to the underlying Redis client handle.
    pub fn client(&self) -> &Arc<C> {
        &self.client
    }

    /// Returns the configured key prefix.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Formats the fully qualified storage key for a cache identifier.
    pub fn format_key(&self, key: &str) -> String {
        format!("{}{key}", self.prefix)
    }
}

impl<C: RedisCommands + 'static> CacheStore for RedisCacheStore<C> {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<CacheEntry>, CacheError>> {
        Box::pin(async move {
            let full_key = self.format_key(key);
            let content = self.client.get(&full_key).await?;

            if let Some(raw) = content {
                let entry = CacheEntry::from_storage_str(&raw)?;
                Ok(Some(entry))
            } else {
                Ok(None)
            }
        })
    }

    fn put<'a>(
        &'a self,
        key: &'a str,
        entry: &'a CacheEntry,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<(), CacheError>> {
        Box::pin(async move {
            let full_key = self.format_key(key);
            let content = entry.to_storage_string()?;
            let ttl_secs = ttl.as_secs().max(1);

            self.client.set_ex(&full_key, &content, ttl_secs).await?;
            Ok(())
        })
    }

    fn add<'a>(
        &'a self,
        key: &'a str,
        entry: &'a CacheEntry,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, CacheError>> {
        Box::pin(async move {
            let full_key = self.format_key(key);
            let content = entry.to_storage_string()?;
            let added = self.client.set_nx(&full_key, &content).await?;

            if added {
                let ttl_secs = ttl.as_secs().max(1);
                let expire_result = self.client.expire(&full_key, ttl_secs).await;
                drop(expire_result);
                Ok(true)
            } else {
                Ok(false)
            }
        })
    }

    fn pull<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<CacheEntry>, CacheError>> {
        Box::pin(async move {
            let full_key = self.format_key(key);
            let content = self.client.get(&full_key).await?;

            if let Some(raw) = content {
                let delete_result = self.client.del(&full_key).await;
                drop(delete_result);
                let entry = CacheEntry::from_storage_str(&raw)?;
                Ok(Some(entry))
            } else {
                Ok(None)
            }
        })
    }

    fn forget<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<bool, CacheError>> {
        Box::pin(async move {
            let full_key = self.format_key(key);
            let removed = self.client.del(&full_key).await?;
            Ok(removed)
        })
    }

    fn forget_many<'a>(&'a self, keys: &'a [&'a str]) -> BoxFuture<'a, Result<usize, CacheError>> {
        Box::pin(async move {
            let full_keys: Vec<String> = keys.iter().map(|key| self.format_key(key)).collect();
            let key_refs: Vec<&str> = full_keys.iter().map(AsRef::as_ref).collect();
            let removed = self.client.del_many(&key_refs).await?;
            Ok(removed as usize)
        })
    }

    fn increment<'a>(
        &'a self,
        key: &'a str,
        amount: i64,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<i64, CacheError>> {
        Box::pin(async move {
            let full_key = self.format_key(key);
            let new_value = self.client.incr_by(&full_key, amount).await?;
            if ttl > Duration::ZERO {
                let expire_result = self.client.expire(&full_key, ttl.as_secs().max(1)).await;
                drop(expire_result);
            }
            Ok(new_value)
        })
    }

    fn flush(&self) -> BoxFuture<'_, Result<(), CacheError>> {
        Box::pin(async move {
            let pattern = format!("{}*", self.prefix);
            let keys = self.client.keys(&pattern).await?;

            if !keys.is_empty() {
                let key_refs: Vec<&str> = keys.iter().map(AsRef::as_ref).collect();
                self.client.del_many(&key_refs).await?;
            }
            Ok(())
        })
    }
}
