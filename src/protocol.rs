use std::io;
use std::sync::Arc;

use crate::client::ClientId;
use crate::lease::Lease;
use crate::server::ServerId;
use crate::store::{Key, Value};
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum NodeId {
    Client(ClientId),
    Server(ServerId),
}

//clients send Request to server
#[derive(Serialize, Deserialize)]
pub enum Request {
    Read { key: Key },
    Write { key: Key, value: Value },
    InvalidateAck { key: Key }, // client -> server, response to a push
    ReleaseLease { key: Key },
}

//server sends Response to clients
#[derive(Serialize, Deserialize)]
pub enum Response {
    ReadOk { value: Value, lease: Lease },
    WriteOk,
    Error(AppError),
}

#[derive(Serialize, Deserialize)]
pub enum AppError {
    ReadErr { for_key: Key, error: String },
    Other(String),
}

// server-initiated message to client (its not a reply to any client msg)
#[derive(Serialize, Deserialize)]
pub enum ServerPush {
    Invalidate { key: Key },
}

#[derive(Serialize, Deserialize)]
pub enum Message {
    Client(ClientMessage),
    Server(ServerMessage),
    HandshakeType(Handshake),
}

#[derive(Serialize, Deserialize)]
pub struct Handshake {
    pub node_id: NodeId,
}

//Client will send this, server will recieve this

#[derive(Serialize, Deserialize)]
pub struct ClientMessage {
    pub client_id: ClientId,
    pub request: Request,
}

//Server will send this, client will recieve this

#[derive(Serialize, Deserialize)]
pub struct ServerMessage {
    pub server_id: ServerId,
    pub payload: ServerMessagePayload,
}

#[derive(Serialize, Deserialize)]
pub enum ServerMessagePayload {
    Reply(Response),
    Push(ServerPush),
}

pub struct OutgoingFromServer {
    pub to: OutgoingReciever,
    pub msg: Message,
}

pub enum OutgoingReciever {
    Client(ClientId),
    Broadcast,
}

//no need for OutgoingFromClient as client msgs are always sent to server only

#[derive(Clone)]
pub struct Codec;

impl Codec {
    pub fn new() -> Self {
        Codec {}
    }
}

//Arc<Message> allows us to copy the message data without copying the message
impl Encoder<Arc<Message>> for Codec {
    type Error = io::Error;
    fn encode(&mut self, item: Arc<Message>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // `&*item` dereferences the Arc into a &Message borrow,
        // which serde_json can serialize without copying the message data.
        let payload = serde_json::to_vec(&*item)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        dst.put_u32(payload.len() as u32);
        dst.put_slice(&payload);
        Ok(())
    }
}

impl Decoder for Codec {
    type Item = Message;
    type Error = io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // 4 byte length header
        if src.len() < 4 {
            //not enough bytes yet
            return Ok(None);
        }

        let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
        let frame_len = 4 + len;

        if src.len() < frame_len {
            src.reserve(frame_len - src.len());
            //not enough bytes yet
            return Ok(None);
        }

        let mut frame = src.split_to(frame_len);
        //skip header
        frame.advance(4);

        let msg: Message = serde_json::from_slice(&frame)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Some(msg))
    }
}
