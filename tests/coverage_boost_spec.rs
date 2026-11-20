use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use kouta::{
    Cache, CacheConfig, CacheEntry, CacheError, CacheEvent, CacheManager, CacheStore,
    FileCacheStore, MemoryCacheStore, StoreConfig, TieredCacheStore, current_unix_timestamp_ms,
    format_tag_key, is_entry_tag_valid,
};
use serde::{Serialize, Serializer};

struct FailingSerializer;

impl Serialize for FailingSerializer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        drop(serializer);
        Err(serde::ser::Error::custom("serialization forced failure"))
    }
}

#[tokio::test]
async fn test_entry_serialization_failures() {
    let bad = FailingSerializer;
    let err1 = CacheEntry::from_value(&bad, Duration::from_secs(60), Duration::ZERO, Vec::new());
    assert!(matches!(err1, Err(CacheError::Serialization(_))));

    let err2 = CacheEntry::from_value_with_clock(
        &bad,
        Duration::from_secs(60),
        Duration::ZERO,
        Vec::new(),
        1000,
    );
    assert!(matches!(err2, Err(CacheError::Serialization(_))));
}

#[tokio::test]
async fn test_cache_remember_hits_and_pull_invalidated_tags() {
    let store = Arc::new(MemoryCacheStore::new());
    let cache = Cache::new(store.clone());

    // 1. Pre-populate an item
    cache
        .put("pre_cached", &"initial_value", Duration::from_secs(3600))
        .await
        .unwrap();

    // 2. Remember on existing fresh item returns the cached value without invoking factory
    let executed = Arc::new(AtomicUsize::new(0));
    let exec_clone = Arc::clone(&executed);
    let val: String = cache
        .remember(
            "pre_cached",
            Duration::from_secs(3600),
            move || async move {
                exec_clone.fetch_add(1, Ordering::SeqCst);
                Ok("new_value".to_string())
            },
        )
        .await
        .unwrap();

    assert_eq!(val, "initial_value");
    assert_eq!(executed.load(Ordering::SeqCst), 0);

    // 3. Remember forever
    let forever_val: String = cache
        .remember_forever("pre_cached", || async { Ok("unused".to_string()) })
        .await
        .unwrap();
    assert_eq!(forever_val, "initial_value");

    // 4. Pull on an entry whose tags are invalidated
    cache
        .tags(&["account"])
        .put("user_acc", &"data", Duration::from_secs(3600))
        .await
        .unwrap();
    cache.tags(&["account"]).flush().await.unwrap();

    let pulled: Option<String> = cache.pull("user_acc").await.unwrap();
    assert!(pulled.is_none());

    // 5. Get on an entry that is physically expired returns None and emits Miss
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let events_clone = Arc::clone(&events);
    let event_cache = Cache::new(store.clone()).with_events(Arc::new(move |event| {
        events_clone.lock().unwrap().push(event.clone());
    }));

    let expired_entry =
        CacheEntry::from_value_with_clock(&"old", Duration::ZERO, Duration::ZERO, Vec::new(), 100)
            .unwrap();
    store
        .put("physically_expired", &expired_entry, Duration::ZERO)
        .await
        .unwrap();

    let got: Option<String> = event_cache.get("physically_expired").await.unwrap();
    assert!(got.is_none());
    let recorded = events.lock().unwrap();
    assert!(
        recorded
            .iter()
            .any(|e| matches!(e, CacheEvent::Miss { key } if key == "physically_expired"))
    );
}

#[tokio::test]
async fn test_tagged_cache_bulk_operations() {
    let store = Arc::new(MemoryCacheStore::new());
    let cache = Cache::new(store);

    let tagged = cache.tags(&["catalog", "v1"]);
    tagged
        .put("item:1", &"Laptop", Duration::from_secs(1800))
        .await
        .unwrap();
    tagged.put_forever("item:2", &"Mouse").await.unwrap();

    assert_eq!(
        tagged.get::<String>("item:1").await.unwrap(),
        Some("Laptop".to_string())
    );
    assert_eq!(
        tagged.get::<String>("item:2").await.unwrap(),
        Some("Mouse".to_string())
    );

    tagged.flush().await.unwrap();

    assert!(tagged.get::<String>("item:1").await.unwrap().is_none());
    assert!(tagged.get::<String>("item:2").await.unwrap().is_none());
}

#[tokio::test]
async fn test_manager_configuration_variants_and_delegation() {
    let mut config = CacheConfig::default();
    config.add_store(
        "tiered_custom",
        StoreConfig::Tiered {
            memory_store: "memory".to_string(),
            distributed_store: "null".to_string(),
            fallback_insurance: Some(false),
            short_circuit: Some(false),
        },
    );

    #[cfg(feature = "redis")]
    config.add_store(
        "redis_uninit",
        StoreConfig::Redis {
            prefix: Some("app:".to_string()),
        },
    );

    let manager = CacheManager::new(config);

    // Non-existent store
    assert!(matches!(
        manager.store("does_not_exist"),
        Err(CacheError::StoreNotFound(_))
    ));

    // Custom tiered store
    let tiered_cache = manager.store("tiered_custom").unwrap();
    assert!(tiered_cache.has("nonexistent").await.is_ok());

    #[cfg(feature = "redis")]
    {
        // Uninitialized Redis store from config returns Error
        assert!(matches!(
            manager.store("redis_uninit"),
            Err(CacheError::Config(_))
        ));
    }

    // Default store delegation: remember, forget, flush
    let remembered: String = manager
        .remember("mgr_key", Duration::from_secs(60), || async {
            Ok("manager_data".to_string())
        })
        .await
        .unwrap();
    assert_eq!(remembered, "manager_data");

    assert!(manager.forget("mgr_key").await.unwrap());
    assert!(manager.flush().await.is_ok());
}

#[tokio::test]
async fn test_singleflight_leader_dropped_before_completion() {
    let coordinator = Arc::new(kouta::SingleFlight::new());

    let coord_clone = Arc::clone(&coordinator);
    let (leader_started_tx, leader_started_rx) = tokio::sync::oneshot::channel();

    // Spawn leader task that starts but gets dropped before finishing
    let leader_handle = tokio::spawn(async move {
        coord_clone
            .execute("dropped_key", || async move {
                assert!(leader_started_tx.send(()).is_ok());
                tokio::time::sleep(Duration::from_millis(500)).await;
                Ok::<String, CacheError>("never_reached".to_string())
            })
            .await
    });

    leader_started_rx.await.unwrap();

    // Spawn follower task while leader is in-flight
    let coord_follower = Arc::clone(&coordinator);
    let follower_handle = tokio::spawn(async move {
        coord_follower
            .execute("dropped_key", || async move {
                Ok::<String, CacheError>("follower_factory".to_string())
            })
            .await
    });

    // Abort the leader task
    leader_handle.abort();

    // Follower must receive the dropped leader error without hanging
    let follower_res = follower_handle.await.unwrap();
    assert!(matches!(follower_res, Err(CacheError::Store(_))));
}

#[tokio::test]
async fn test_file_cache_store_prune_and_expired_file() {
    let temp_dir = std::env::temp_dir().join(format!("kouta_prune_test_{}", std::process::id()));
    let store = FileCacheStore::new(&temp_dir).unwrap();

    // 1. Write an unexpired entry
    let active_entry = CacheEntry::from_value(
        &"active",
        Duration::from_secs(3600),
        Duration::ZERO,
        Vec::new(),
    )
    .unwrap();
    store
        .put("active_item", &active_entry, Duration::from_secs(3600))
        .await
        .unwrap();

    // 2. Write an already expired entry
    let expired_entry = CacheEntry::from_value_with_clock(
        &"expired",
        Duration::ZERO,
        Duration::ZERO,
        Vec::new(),
        1,
    )
    .unwrap();
    store
        .put("expired_item", &expired_entry, Duration::ZERO)
        .await
        .unwrap();

    // 3. Create a stray .tmp file in the directory
    let stray_tmp = temp_dir.join("temp_123_456_789.tmp");
    std::fs::write(&stray_tmp, "temporary").unwrap();

    // 4. clean_expired should delete both the .tmp file and the expired entry
    let pruned = store.clean_expired().unwrap();
    assert!(pruned >= 2);
    assert!(!stray_tmp.exists());

    // 5. Test forget_many, increment, decrement, and flush on FileCacheStore
    store
        .put("f1", &active_entry, Duration::from_secs(3600))
        .await
        .unwrap();
    store
        .put("f2", &active_entry, Duration::from_secs(3600))
        .await
        .unwrap();
    let removed_count = store.forget_many(&["f1", "f2", "not_there"]).await.unwrap();
    assert_eq!(removed_count, 2);

    let inc1 = store
        .increment("counter", 5, Duration::from_secs(3600))
        .await
        .unwrap();
    assert_eq!(inc1, 5);
    let inc2 = store
        .decrement("counter", 2, Duration::from_secs(3600))
        .await
        .unwrap();
    assert_eq!(inc2, 3);

    store.flush().await.unwrap();
    assert!(store.get("counter").await.unwrap().is_none());

    drop(std::fs::remove_dir_all(&temp_dir));
}

#[tokio::test]
async fn test_tag_validation_internal_tag_key() {
    let store = MemoryCacheStore::new();
    let internal_tag_entry = CacheEntry::from_value(
        &format_tag_key("test_tag"),
        Duration::from_secs(3600),
        Duration::ZERO,
        vec!["tag_a".to_string()],
    )
    .unwrap();

    let is_valid = is_entry_tag_valid(&internal_tag_entry, &store)
        .await
        .unwrap();
    assert!(is_valid);
}

#[cfg(feature = "redis")]
#[tokio::test]
async fn test_redis_fake_edge_cases() {
    use kouta::{RedisCacheStore, RedisCommands, RedisFake};

    let fake = Arc::new(RedisFake::new());
    let store = RedisCacheStore::new(fake.clone());

    // 1. Expire on non-existent key returns false
    assert!(!store.client().expire("ghost", 10).await.unwrap());

    // 2. Expired key in RedisFake gets removed on get
    store.client().set_ex("expired_k", "val", 1).await.unwrap();
    // Simulate expired by advancing or using expire with 0
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(store.get("expired_k").await.unwrap().is_none());

    // 3. set_nx on expired key replaces it
    store
        .client()
        .set_ex("will_expire", "val", 1)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(
        store
            .client()
            .set_nx("will_expire", "new_val")
            .await
            .unwrap()
    );

    // 4. set_nx on unexpired key with no expiration returns false
    assert!(store.client().set_nx("permanent", "val").await.unwrap());
    assert!(!store.client().set_nx("permanent", "val2").await.unwrap());

    // 5. increment on expired key resets to 0 + amount
    store.client().set_ex("exp_num", "100", 1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let new_val = store.client().incr_by("exp_num", 5).await.unwrap();
    assert_eq!(new_val, 5);

    // 6. increment on non-numeric value resets to 0 + amount
    store.client().set_nx("str_val", "abc").await.unwrap();
    let num_val = store.client().incr_by("str_val", 7).await.unwrap();
    assert_eq!(num_val, 7);

    // 7. keys with pattern matching mixed expired and active keys
    store.client().set_nx("prefix:active", "1").await.unwrap();
    store.client().set_ex("prefix:dead", "1", 1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let matched = store.client().keys("prefix:*").await.unwrap();
    assert!(matched.contains(&"prefix:active".to_string()));
    assert!(!matched.contains(&"prefix:dead".to_string()));

    // 8. del_many with empty slice
    assert_eq!(store.client().del_many(&[]).await.unwrap(), 0);

    // 9. RedisCacheStore pull on missing key
    assert!(store.pull("missing_pull").await.unwrap().is_none());

    // 10. RedisCacheStore increment with Duration::ZERO
    let zero_ttl_inc = store
        .increment("no_ttl_counter", 10, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(zero_ttl_inc, 10);

    // 11. RedisCacheStore flush on empty
    store.flush().await.unwrap();
    store.flush().await.unwrap();
}

#[cfg(feature = "redis")]
#[tokio::test]
async fn test_fred_client_and_pool_commands_contracts() {
    use fred::interfaces::ClientLike;
    use kouta::RedisCommands;

    let config = fred::types::config::Config {
        mocks: Some(Arc::new(fred::mocks::SimpleMap::new())),
        ..Default::default()
    };
    let client = fred::clients::Client::new(config, None, None, None);
    let connect_handle = client.connect();
    client.wait_for_connect().await.unwrap();

    assert!(
        RedisCommands::set_ex(&client, "key", "val", 10)
            .await
            .is_ok()
    );
    assert_eq!(
        RedisCommands::get(&client, "key").await.unwrap(),
        Some("val".to_string())
    );
    assert!(RedisCommands::set_nx(&client, "key", "val2").await.is_ok());
    assert!(RedisCommands::expire(&client, "key", 10).await.is_err());
    assert!(RedisCommands::incr_by(&client, "counter", 5).await.is_err());
    assert_eq!(RedisCommands::del_many(&client, &[]).await.unwrap(), 0);
    assert!(RedisCommands::del(&client, "key").await.unwrap());
    assert_eq!(
        RedisCommands::del_many(&client, &["nonexistent1", "nonexistent2"])
            .await
            .unwrap(),
        0
    );
    assert!(RedisCommands::keys(&client, "*").await.is_err());
    assert!(RedisCommands::ping(&client).await.is_err());

    let pool = fred::clients::Pool::from_clients(vec![client.clone()]).unwrap();
    assert!(
        RedisCommands::set_ex(&pool, "pool_key", "val", 10)
            .await
            .is_ok()
    );
    assert_eq!(
        RedisCommands::get(&pool, "pool_key").await.unwrap(),
        Some("val".to_string())
    );
    assert!(
        RedisCommands::set_nx(&pool, "pool_key", "val2")
            .await
            .is_ok()
    );
    assert!(RedisCommands::expire(&pool, "pool_key", 10).await.is_err());
    assert!(RedisCommands::incr_by(&pool, "pool_cnt", 3).await.is_err());
    assert_eq!(RedisCommands::del_many(&pool, &[]).await.unwrap(), 0);
    assert!(RedisCommands::del(&pool, "pool_key").await.unwrap());
    assert_eq!(
        RedisCommands::del_many(&pool, &["nonexistent1", "nonexistent2"])
            .await
            .unwrap(),
        0
    );
    assert!(RedisCommands::keys(&pool, "*").await.is_err());
    assert!(RedisCommands::ping(&pool).await.is_err());

    drop(connect_handle);
}

struct ExpiredL2Store(CacheEntry);

impl CacheStore for ExpiredL2Store {
    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> kouta::store::BoxFuture<'a, Result<Option<CacheEntry>, CacheError>> {
        let entry = self.0.clone();
        Box::pin(async move {
            if key.is_empty() {
                Ok(None)
            } else {
                Ok(Some(entry))
            }
        })
    }

    fn put<'a>(
        &'a self,
        key: &'a str,
        entry: &'a CacheEntry,
        ttl: Duration,
    ) -> kouta::store::BoxFuture<'a, Result<(), CacheError>> {
        let reason = format!("put {key} ttl {ttl:?} entry {entry:?}");
        Box::pin(async move {
            if reason.is_empty() {
                Err(CacheError::Store("never".to_string()))
            } else {
                Ok(())
            }
        })
    }

    fn add<'a>(
        &'a self,
        key: &'a str,
        entry: &'a CacheEntry,
        ttl: Duration,
    ) -> kouta::store::BoxFuture<'a, Result<bool, CacheError>> {
        let reason = format!("add {key} ttl {ttl:?} entry {entry:?}");
        Box::pin(async move {
            if reason.is_empty() {
                Err(CacheError::Store("never".to_string()))
            } else {
                Ok(true)
            }
        })
    }

    fn pull<'a>(
        &'a self,
        key: &'a str,
    ) -> kouta::store::BoxFuture<'a, Result<Option<CacheEntry>, CacheError>> {
        let reason = format!("pull {key}");
        Box::pin(async move {
            if reason.is_empty() {
                Err(CacheError::Store("never".to_string()))
            } else {
                Ok(None)
            }
        })
    }

    fn forget<'a>(&'a self, key: &'a str) -> kouta::store::BoxFuture<'a, Result<bool, CacheError>> {
        let reason = format!("forget {key}");
        Box::pin(async move {
            if reason.is_empty() {
                Err(CacheError::Store("never".to_string()))
            } else {
                Ok(true)
            }
        })
    }

    fn forget_many<'a>(
        &'a self,
        keys: &'a [&'a str],
    ) -> kouta::store::BoxFuture<'a, Result<usize, CacheError>> {
        let count = keys.len();
        Box::pin(async move { Ok(count) })
    }

    fn increment<'a>(
        &'a self,
        key: &'a str,
        amount: i64,
        ttl: Duration,
    ) -> kouta::store::BoxFuture<'a, Result<i64, CacheError>> {
        let reason = format!("incr {key} {amount} {ttl:?}");
        Box::pin(async move {
            if reason.is_empty() {
                Err(CacheError::Store("never".to_string()))
            } else {
                Ok(amount)
            }
        })
    }

    fn flush(&self) -> kouta::store::BoxFuture<'_, Result<(), CacheError>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn test_tiered_cache_backfill_skipped_for_expired_l2() {
    let memory = MemoryCacheStore::new();

    let now_ms = current_unix_timestamp_ms();
    let expired_entry = CacheEntry::from_value_with_clock(
        &"l2_expired",
        Duration::ZERO,
        Duration::ZERO,
        Vec::new(),
        now_ms.saturating_sub(10_000),
    )
    .unwrap();

    let l2_store = Arc::new(ExpiredL2Store(expired_entry));
    let tiered = TieredCacheStore::new(memory.clone(), l2_store);

    let fetched = tiered.get("l2_key").await.unwrap();
    assert!(fetched.is_some());
    // L1 should not be backfilled because remaining_ms == 0
    assert!(memory.get("l2_key").await.unwrap().is_none());
}
