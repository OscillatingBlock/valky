use crate::client::ClientId;
use crate::clock::Clock;
use crate::lease::{Lease, LeaseTable};
use crate::store::{Key, ServerStore, Value};

use std::sync::Arc;

// abstracts "tell client X to invalidate key Y" so tests can fake it
pub trait ClientNotifier: Send + Sync {
    fn send_invalidate(&self, client_id: ClientId, key: &Key);
}

pub struct Server {
    store: ServerStore,
    leases: LeaseTable,
    notifier: Arc<dyn ClientNotifier>,
    clock: Arc<dyn Clock>,
}

impl Server {
    pub fn read(&mut self, key: &Key, client_id: ClientId) -> Option<(Value, Lease)> {
        self.leases.grant(key.clone(), client_id).ok()
    }

    // this is the interesting one — needs to block/wait pending acks or expiry
    pub fn write(&mut self, key: Key, value: Value) -> anyhow::Result<()> {
        unimplemented!()
    }

    pub fn on_invalidate_ack(&mut self, client_id: ClientId, key: &Key) {
        unimplemented!()
    }
}
