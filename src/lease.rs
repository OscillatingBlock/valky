use std::collections::HashMap;
use std::ops::Add;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Ok;
use serde::{Deserialize, Serialize};
use tokio::time;

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
            .map_err(|e| anyhow::anyhow!("failed to grant lease, lock poisoned: {e}"))?;

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
            .map_err(|e| anyhow::anyhow!("failed to release lease, lock poisoned"))?;

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
            .map_err(|e| anyhow::anyhow!("failed to release lease, lock poisoned"))?;

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
