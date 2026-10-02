use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::Ok;
use serde::{Deserialize, Serialize};

use crate::client::ClientId;
use crate::clock::Clock;
use crate::store::Key;

#[derive(Clone, Serialize, Deserialize)]
pub struct Lease {
    pub key: Key,
    pub client_id: ClientId,
    pub expires_at: u64,
}

#[derive(Clone)]
pub struct LeaseTable {
    clock: Arc<dyn Clock>,
    skew_bound_ms: u64,
    leases: Arc<RwLock<HashMap<Key, Vec<Lease>>>>,
    lease_duration: u64,
}

impl LeaseTable {
    pub fn new(clock: Arc<dyn Clock>, skew_bound_ms: u64, lease_duration: u64) -> Self {
        Self {
            clock,
            skew_bound_ms,
            lease_duration,
            leases: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn grant(&self, key: Key, client_id: ClientId) -> anyhow::Result<Lease> {
        let lease = Lease {
            key: key.clone(),
            client_id: client_id,
            expires_at: self.clock.now() + self.lease_duration,
        };

        let mut writer = self
            .leases
            .write()
            .map_err(|e| anyhow::anyhow!("lease table lock poisoned: {e}"))?;

        writer.entry(key).or_insert(vec![]).push(lease.clone());
        Ok(lease)
    }

    pub fn active_leases(&self, key: &Key) -> anyhow::Result<Vec<Lease>> {
        let reader = self
            .leases
            .read()
            .map_err(|e| anyhow::anyhow!("lease table lock poisoned: {e}"))?;

        let Some(leases) = reader.get(key) else {
            return Ok(vec![]);
        };

        let active_leases: Vec<Lease> = leases
            .iter()
            .filter(|l| !self.is_expired(l))
            .cloned()
            .collect();

        Ok(active_leases)
    }

    pub fn release(&self, key: &Key, client_id: ClientId) -> anyhow::Result<()> {
        let mut writer = self
            .leases
            .write()
            .map_err(|e| anyhow::anyhow!("failed to release lease, lock poisoned {e}"))?;

        if let Some(leases) = writer.get_mut(key) {
            leases.retain(|lease| lease.client_id != client_id);
            if leases.is_empty() {
                writer.remove(key);
            }
        }
        Ok(())
    }

    pub fn prune_expired(&self) -> anyhow::Result<()> {
        let mut writer = self
            .leases
            .write()
            .map_err(|e| anyhow::anyhow!("failed to release lease, lock poisoned {e}"))?;

        writer.retain(|_k, leases| {
            leases.retain(|l| !self.is_expired(l));
            !leases.is_empty()
        });

        Ok(())
    }

    fn is_expired(&self, lease: &Lease) -> bool {
        // server only trusts a lease as expired once its OWN clock is
        // skew_bound_ms past expires_at — i.e. it waits a little extra,
        // in case the client's clock is behind
        self.clock.now() >= lease.expires_at + self.skew_bound_ms
    }
}

#[cfg(test)]
mod test {

    use std::time::Duration;

    use bytes::Bytes;

    use crate::{clock::SystemClock, store::Value};

    use super::*;
    fn new_lease_table() -> LeaseTable {
        let clock = Arc::new(SystemClock::default());
        let skew_bound_ms = 100;
        let lease_duration = 1000;
        LeaseTable::new(clock, skew_bound_ms, lease_duration)
    }

    #[tokio::test]
    async fn test_grant_lease_returns_expiry_in_future() {
        let lease_table = new_lease_table();
        let key = Key::from_string(String::from("key"));
        let value = Value::from_bytes(Bytes::from("value"));

        let client_id = ClientId::new(0);
        let lease = lease_table.grant(key, client_id).unwrap();
        assert!(lease.expires_at > SystemClock::default().now());
    }

    #[tokio::test]
    async fn test_grant_lease_on_different_keys_independently() {
        let table = new_lease_table();
        let key_x = Key::from_string(String::from("x"));
        let key_y = Key::from_string(String::from("y"));
        table.grant(key_x.clone(), ClientId::new(1)).unwrap();
        table.grant(key_y.clone(), ClientId::new(2)).unwrap();

        let active_x = table.active_leases(&key_x).unwrap();
        let active_y = table.active_leases(&key_y).unwrap();

        assert_eq!(active_x.len(), 1);
        assert_eq!(active_x[0].client_id, ClientId::new(1));

        assert_eq!(active_y.len(), 1);
        assert_eq!(active_y[0].client_id, ClientId::new(2));

        // key "x" doesn't show client 2's lease,
        // and key "y" doesn't show client 1's lease
        assert_ne!(active_x[0].client_id, active_y[0].client_id);
    }

    #[tokio::test]
    async fn test_multiple_clients_can_hold_lease_on_same_key() {
        let lease_table = new_lease_table();
        let key = Key::from_string(String::from("key"));
        let client_id = ClientId::new(0);
        let lease = lease_table.grant(key.clone(), client_id).unwrap();
        let lease2 = lease_table.grant(key.clone(), ClientId::new(2)).unwrap();
        let lease3 = lease_table.grant(key, ClientId::new(3)).unwrap();

        assert_eq!(lease.key, lease2.key);
        assert_eq!(lease.key, lease3.key);
        assert_ne!(lease.client_id, lease2.client_id);
        assert_ne!(lease.client_id, lease3.client_id);
    }

    #[tokio::test]
    async fn granting_same_client_same_key_twice_results_in_two_entries() {
        let lease_table = new_lease_table();
        let key = Key::from_string(String::from("key"));
        let client_id = ClientId::new(0);
        let lease = lease_table.grant(key.clone(), client_id.clone()).unwrap();
        let lease2 = lease_table.grant(key.clone(), client_id).unwrap();

        let active_leases = lease_table.active_leases(&key).unwrap();
        assert_eq!(active_leases.len(), 2);
    }

    #[tokio::test]
    async fn test_lease_is_active_before_expiry() {
        let lease_table = new_lease_table();
        let key = Key::from_string(String::from("key"));
        let client_id = ClientId::new(0);
        lease_table.grant(key.clone(), client_id.clone()).unwrap();

        let active_leases = lease_table.active_leases(&key).unwrap();
        assert_eq!(active_leases.len(), 1);
        assert_eq!(active_leases[0].key, key);
    }

    #[tokio::test]
    async fn test_lease_is_expired_after_expiry_time() {
        let lease_table = new_lease_table();
        let key = Key::from_string(String::from("key"));
        let client_id = ClientId::new(0);
        let lease = lease_table.grant(key.clone(), client_id.clone()).unwrap();

        let now = lease_table.clock.now();
        let sleep_duration = lease.expires_at.saturating_sub(now);
        println!("sleeping for {} millis", sleep_duration);
        //add skew duration of 100 seconds also , since server will assume key valid for skew
        //duration also
        tokio::time::sleep(Duration::from_millis(sleep_duration + 100)).await;

        let active_leases = lease_table.active_leases(&key).unwrap();
        assert_eq!(active_leases.len(), 0);
    }

    #[tokio::test]
    async fn test_is_expired_applies_skew_bound() {
        let lease_table = new_lease_table();
        let key = Key::from_string(String::from("key"));
        let client_id = ClientId::new(0);
        let lease = lease_table.grant(key.clone(), client_id.clone()).unwrap();

        let now = lease_table.clock.now();
        let sleep_duration = lease.expires_at.saturating_sub(now);
        println!("sleeping for {} millis", sleep_duration);
        tokio::time::sleep(Duration::from_millis(sleep_duration)).await;

        let active_leases = lease_table.active_leases(&key).unwrap();
        assert_eq!(active_leases.len(), 1);
        assert_eq!(active_leases[0].key, key);
    }

    #[tokio::test]
    async fn test_releasing_key_removes_only_that_clients_entry() {
        let lease_table = new_lease_table();
        let key = Key::from_string(String::from("key"));
        let client_id = ClientId::new(0);
        lease_table.grant(key.clone(), client_id.clone()).unwrap();

        let key2 = Key::from_string(String::from("key"));
        let client_id2 = ClientId::new(1);
        lease_table.grant(key2.clone(), client_id2.clone()).unwrap();

        let active_leases = lease_table.active_leases(&key).unwrap();
        assert_eq!(active_leases.len(), 2);

        lease_table.release(&key, client_id).unwrap();
        let active_leases = lease_table.active_leases(&key).unwrap();
        assert_eq!(active_leases.len(), 1);
        assert_eq!(active_leases[0].key, key2);
    }

    #[tokio::test]
    async fn test_release_on_key_with_no_entry_is_noop() {
        let lease_table = new_lease_table();
        let key = Key::from_string(String::from("key"));
        let client_id = ClientId::new(0);
        lease_table.release(&key, client_id.clone()).unwrap();

        let active_leases = lease_table.active_leases(&key).unwrap();
        assert_eq!(active_leases.len(), 0);
    }

    #[tokio::test]
    async fn test_release_for_unknown_client_is_noop() {
        let lease_table = new_lease_table();
        let key = Key::from_string(String::from("key"));
        let client_id = ClientId::new(0);
        lease_table.grant(key.clone(), client_id.clone()).unwrap();

        lease_table.release(&key, ClientId::new(67)).unwrap();

        let active_leases = lease_table.active_leases(&key).unwrap();
        assert_eq!(active_leases.len(), 1);
        assert_eq!(active_leases[0].key, key);
        assert_eq!(active_leases[0].client_id, client_id);
    }

    #[tokio::test]
    async fn release_twice_is_idempotent() {
        let lease_table = new_lease_table();
        let key = Key::from_string(String::from("key"));
        let client_id = ClientId::new(0);
        lease_table.grant(key.clone(), client_id.clone()).unwrap();

        lease_table.release(&key, client_id.clone()).unwrap();
        lease_table.release(&key, client_id.clone()).unwrap();

        let active_leases = lease_table.active_leases(&key).unwrap();
        assert_eq!(active_leases.len(), 0);
    }

    #[tokio::test]
    async fn test_concurrent_grants_on_same_key_dont_lose_entries() {
        let lease_table = new_lease_table();
        let key = Key::from_string(String::from("key"));
        let mut handles = Vec::new();

        for i in 0..10 {
            let table = lease_table.clone();
            let key = key.clone();
            let handle = tokio::task::spawn(async move {
                let client_id = ClientId::new(i);
                table.grant(key, client_id).unwrap();
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let active = lease_table.active_leases(&key).unwrap();
        assert_eq!(active.len(), 10);

        let mut client_ids: Vec<ClientId> = active.iter().map(|l| l.client_id.clone()).collect();
        client_ids.sort();
    }

    #[tokio::test]
    async fn test_concurrent_grant_and_release_same_client_dont_corrupt_state() {
        let lease_table = new_lease_table();
        let key = Key::from_string(String::from("key"));
        let client_id = ClientId::new(1);

        // seed one lease first, so release has something to race against
        lease_table.grant(key.clone(), client_id.clone()).unwrap();

        let table_a = lease_table.clone();
        let key_a = key.clone();
        let client_a = client_id.clone();
        let grant_task = tokio::task::spawn(async move {
            for _ in 0..50 {
                table_a.grant(key_a.clone(), client_a.clone()).unwrap();
            }
        });

        let table_b = lease_table.clone();
        let key_b = key.clone();
        let client_b = client_id.clone();
        let release_task = tokio::task::spawn(async move {
            for _ in 0..50 {
                table_b.release(&key_b, client_b.clone()).unwrap();
            }
        });

        grant_task.await.unwrap();
        release_task.await.unwrap();

        // whatever the final state is, it must be VALID: either 0 or some small
        // number of entries for this client, never a panic, never a torn/corrupt Vec
        let active = lease_table.active_leases(&key).unwrap();
        let this_client_count = active.iter().filter(|l| l.client_id == client_id).count();
        assert!(
            this_client_count <= 1,
            "client ended up with more than one lease entry, state is corrupted"
        );
    }
}
