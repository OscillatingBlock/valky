use std::io;
use std::sync::Arc;

use crate::client::ClientId;
use crate::lease::Lease;
use crate::server::ServerId;
use crate::store::{Key, Value};
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
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

/// Framing tests: the 4-byte big-endian length prefix must survive partial
/// TCP delivery and back-to-back frames in one buffer. (`Message` has no
/// `PartialEq`, so assertions match on the decoded variants.)
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn read_msg(client: u64, key: &str) -> Message {
        Message::Client(ClientMessage {
            client_id: ClientId::new(client),
            request: crate::protocol::Request::Read {
                key: Key::from_string(key.to_string()),
            },
        })
    }

    fn encode(msg: Message) -> BytesMut {
        let mut codec = Codec::new();
        let mut buf = BytesMut::new();
        codec.encode(Arc::new(msg), &mut buf).unwrap();
        buf
    }

    #[tokio::test]
    async fn test_codec_roundtrips_client_read() {
        let buf = encode(read_msg(7, "k"));
        let mut codec = Codec::new();
        let mut buf = buf;
        match codec.decode(&mut buf).unwrap().expect("expected a message") {
            Message::Client(cm) => {
                assert_eq!(cm.client_id, ClientId::new(7));
                match cm.request {
                    crate::protocol::Request::Read { key } => {
                        assert_eq!(key, Key::from_string("k".to_string()))
                    }
                    _ => panic!("expected Read request"),
                }
            }
            _ => panic!("expected Client message"),
        }
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[tokio::test]
    async fn test_codec_roundtrips_server_read_ok() {
        let key = Key::from_string("k".to_string());
        let msg = Message::Server(ServerMessage {
            server_id: ServerId::new(1),
            payload: ServerMessagePayload::Reply(Response::ReadOk {
                value: Value::from_bytes(Bytes::copy_from_slice(b"v")),
                lease: Lease {
                    key: key.clone(),
                    client_id: ClientId::new(7),
                    expires_at: 12345,
                },
            }),
        });
        let mut buf = encode(msg);
        let mut codec = Codec::new();
        match codec.decode(&mut buf).unwrap().expect("expected a message") {
            Message::Server(sm) => match sm.payload {
                ServerMessagePayload::Reply(Response::ReadOk { value, lease }) => {
                    assert_eq!(&value.as_bytes()[..], b"v");
                    assert_eq!(lease.key, key);
                    assert_eq!(lease.expires_at, 12345);
                }
                _ => panic!("expected ReadOk reply"),
            },
            _ => panic!("expected Server message"),
        }
    }

    #[tokio::test]
    async fn test_codec_waits_for_partial_frame() {
        let full = encode(read_msg(7, "partial"));
        // Split mid-header: nothing decodable yet.
        let mut part = BytesMut::from(&full[..2]);
        let mut codec = Codec::new();
        assert!(codec.decode(&mut part).unwrap().is_none());

        // Rest arrives: exactly one message, buffer drained.
        part.extend_from_slice(&full[2..]);
        let msg = codec.decode(&mut part).unwrap().expect("expected a message");
        assert!(matches!(msg, Message::Client(_)));
        assert!(codec.decode(&mut part).unwrap().is_none());
    }

    #[tokio::test]
    async fn test_codec_decodes_back_to_back_frames() {
        let mut codec = Codec::new();
        let mut buf = encode(read_msg(1, "a"));
        buf.extend_from_slice(&encode(read_msg(2, "b")));

        for want in [ClientId::new(1), ClientId::new(2)] {
            match codec.decode(&mut buf).unwrap().expect("expected a message") {
                Message::Client(cm) => assert_eq!(cm.client_id, want),
                _ => panic!("expected Client message"),
            }
        }
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }
}
