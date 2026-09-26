use crate::lease::Lease;
use crate::store::{Key, Value};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum Request {
    Read { key: Key },
    Write { key: Key, value: Value },
    InvalidateAck { key: Key }, // client -> server, response to a push
    ReleaseLease { key: Key },
}

#[derive(Serialize, Deserialize)]
pub enum Response {
    ReadOk { value: Value, lease: Lease },
    WriteOk,
    Error { message: String },
}

// server-initiated, not a reply — same enum or separate depending on
// how your framing distinguishes push vs reply
#[derive(Serialize, Deserialize)]
pub enum ServerPush {
    Invalidate { key: Key },
}
