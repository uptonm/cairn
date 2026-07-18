use super::Transport;
use crate::{Message, NodeId};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;

const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

type WriterSlot = Arc<Mutex<Option<OwnedWriteHalf>>>;

pub struct TcpTransport {
    node_id: NodeId,
    local_addr: SocketAddr,
    peers: RwLock<HashMap<NodeId, SocketAddr>>,
    writers: Mutex<HashMap<NodeId, WriterSlot>>,
    receiver: mpsc::UnboundedReceiver<(NodeId, Message)>,
    accept_task: JoinHandle<()>,
}

impl TcpTransport {
    pub async fn bind(
        node_id: NodeId,
        bind_addr: SocketAddr,
        peers: HashMap<NodeId, SocketAddr>,
    ) -> crate::Result<TcpTransport> {
        let listener = TcpListener::bind(bind_addr).await?;
        let local_addr = listener.local_addr()?;
        let (sender, receiver) = mpsc::unbounded_channel();
        let accept_task = tokio::spawn(accept_connections(listener, sender));

        Ok(TcpTransport {
            node_id,
            local_addr,
            peers: RwLock::new(peers),
            writers: Mutex::new(HashMap::new()),
            receiver,
            accept_task,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn set_peer(&self, node_id: NodeId, addr: SocketAddr) {
        self.peers.write().await.insert(node_id, addr);
        let writer = self.writers.lock().await.get(&node_id).cloned();
        if let Some(writer) = writer {
            *writer.lock().await = None;
        }
    }

    async fn peer_addr(&self, node_id: NodeId) -> crate::Result<SocketAddr> {
        self.peers
            .read()
            .await
            .get(&node_id)
            .copied()
            .ok_or_else(|| {
                io_error(
                    ErrorKind::InvalidInput,
                    format!("unknown destination node {node_id}"),
                )
            })
    }

    async fn writer_slot(&self, node_id: NodeId) -> WriterSlot {
        Arc::clone(
            self.writers
                .lock()
                .await
                .entry(node_id)
                .or_insert_with(|| Arc::new(Mutex::new(None))),
        )
    }
}

impl Drop for TcpTransport {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

#[async_trait::async_trait]
impl Transport for TcpTransport {
    async fn send(&self, to: NodeId, msg: Message) -> crate::Result<()> {
        self.peer_addr(to).await?;
        let frame = encode_frame(&msg)?;
        let writer = self.writer_slot(to).await;
        let mut writer = writer.lock().await;
        let peer_addr = self.peer_addr(to).await?;

        if write_frame_once(&mut writer, self.node_id, peer_addr, &frame)
            .await
            .is_ok()
        {
            return Ok(());
        }

        write_frame_once(&mut writer, self.node_id, peer_addr, &frame).await
    }

    async fn recv(&mut self) -> Option<(NodeId, Message)> {
        self.receiver.recv().await
    }
}

fn encode_frame(message: &Message) -> crate::Result<Vec<u8>> {
    let payload = bincode::serialize(message).map_err(|error| {
        crate::Error::Corruption(format!("message serialization failed: {error}"))
    })?;
    if payload.len() > MAX_FRAME_SIZE {
        return Err(crate::Error::Corruption(format!(
            "message payload exceeds {MAX_FRAME_SIZE} byte frame limit"
        )));
    }
    let payload_len = u32::try_from(payload.len()).map_err(|_| {
        crate::Error::Corruption("message payload does not fit in a u32 frame length".to_string())
    })?;
    let mut frame = Vec::with_capacity(payload.len() + 4);
    frame.extend_from_slice(&payload_len.to_le_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

async fn write_frame_once(
    writer: &mut Option<OwnedWriteHalf>,
    node_id: NodeId,
    peer_addr: SocketAddr,
    frame: &[u8],
) -> crate::Result<()> {
    if writer.is_none() {
        *writer = Some(connect_writer(node_id, peer_addr).await?);
    }

    let result = match writer {
        Some(writer) => writer.write_all(frame).await,
        None => {
            return Err(io_error(
                ErrorKind::NotConnected,
                format!("writer for peer {peer_addr} is unavailable"),
            ))
        }
    };
    if result.is_err() {
        *writer = None;
    }
    result.map_err(Into::into)
}

async fn connect_writer(node_id: NodeId, peer_addr: SocketAddr) -> crate::Result<OwnedWriteHalf> {
    let mut stream = TcpStream::connect(peer_addr).await?;
    stream.write_all(&node_id.to_le_bytes()).await?;
    let (_, writer) = stream.into_split();
    Ok(writer)
}

async fn accept_connections(
    listener: TcpListener,
    sender: mpsc::UnboundedSender<(NodeId, Message)>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let sender = sender.clone();
                tokio::spawn(async move {
                    let _ = read_stream(stream, sender).await;
                });
            }
            Err(_) => tokio::task::yield_now().await,
        }
    }
}

async fn read_stream(
    mut stream: TcpStream,
    sender: mpsc::UnboundedSender<(NodeId, Message)>,
) -> crate::Result<()> {
    let mut handshake = [0; 8];
    stream.read_exact(&mut handshake).await?;
    let source_id = NodeId::from_le_bytes(handshake);

    loop {
        let mut length = [0; 4];
        stream.read_exact(&mut length).await?;
        let payload_len = u32::from_le_bytes(length) as usize;
        if payload_len > MAX_FRAME_SIZE {
            return Err(crate::Error::Corruption(format!(
                "incoming payload exceeds {MAX_FRAME_SIZE} byte frame limit"
            )));
        }

        let mut payload = vec![0; payload_len];
        stream.read_exact(&mut payload).await?;
        let message = bincode::deserialize(&payload).map_err(|error| {
            crate::Error::Corruption(format!("message deserialization failed: {error}"))
        })?;
        sender.send((source_id, message)).map_err(|_| {
            io_error(
                ErrorKind::BrokenPipe,
                "TCP transport inbound receiver is closed",
            )
        })?;
    }
}

fn io_error(kind: ErrorKind, message: impl Into<String>) -> crate::Error {
    std::io::Error::new(kind, message.into()).into()
}

#[cfg(test)]
mod tests {
    use super::TcpTransport;
    use crate::transport::Transport;
    use crate::{
        AppendEntriesReq, AppendEntriesResp, InstallSnapshotReq, InstallSnapshotResp, LogEntry,
        Message, RequestVoteReq, RequestVoteResp,
    };
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    fn all_messages() -> Vec<Message> {
        vec![
            Message::RequestVote(RequestVoteReq {
                term: 2,
                candidate_id: 7,
                last_log_index: 11,
                last_log_term: 1,
                pre_vote: true,
            }),
            Message::RequestVoteResp(RequestVoteResp {
                term: 2,
                vote_granted: true,
            }),
            Message::AppendEntries(AppendEntriesReq {
                term: 3,
                leader_id: 7,
                prev_log_index: 10,
                prev_log_term: 2,
                entries: vec![LogEntry {
                    term: 3,
                    index: 11,
                    command: b"set x".to_vec(),
                }],
                leader_commit: 9,
            }),
            Message::AppendEntriesResp(AppendEntriesResp {
                term: 3,
                success: false,
                conflict_index: Some(8),
            }),
            Message::InstallSnapshot(InstallSnapshotReq {
                term: 4,
                leader_id: 7,
                last_index: 11,
                last_term: 3,
                data: b"snapshot".to_vec(),
            }),
            Message::InstallSnapshotResp(InstallSnapshotResp { term: 4 }),
        ]
    }

    #[tokio::test]
    async fn exchanges_every_message_variant_bidirectionally_over_real_sockets() {
        let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let mut one = TcpTransport::bind(1, bind_addr, HashMap::new())
            .await
            .unwrap();
        let mut two = TcpTransport::bind(2, bind_addr, HashMap::new())
            .await
            .unwrap();

        one.set_peer(2, two.local_addr()).await;
        two.set_peer(1, one.local_addr()).await;

        for message in all_messages() {
            let expected = message.clone();
            one.send(2, message).await.unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), two.recv())
                    .await
                    .unwrap(),
                Some((1, expected))
            );
        }

        for message in all_messages() {
            let expected = message.clone();
            two.send(1, message).await.unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), one.recv())
                    .await
                    .unwrap(),
                Some((2, expected))
            );
        }
    }

    #[tokio::test]
    async fn writes_exact_handshake_and_frame_to_raw_peer() {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let peer_addr = listener.local_addr().unwrap();
        let expected = Message::RequestVoteResp(RequestVoteResp {
            term: 17,
            vote_granted: true,
        });
        let raw_peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut handshake = [0; 8];
            stream.read_exact(&mut handshake).await.unwrap();
            let mut length = [0; 4];
            stream.read_exact(&mut length).await.unwrap();
            let mut payload = vec![0; u32::from_le_bytes(length) as usize];
            stream.read_exact(&mut payload).await.unwrap();
            (handshake, length, payload)
        });

        let transport = TcpTransport::bind(
            9,
            SocketAddr::from(([127, 0, 0, 1], 0)),
            HashMap::from([(2, peer_addr)]),
        )
        .await
        .unwrap();
        transport.send(2, expected.clone()).await.unwrap();

        let (handshake, length, payload) = raw_peer.await.unwrap();
        assert_eq!(handshake, 9_u64.to_le_bytes());
        assert_eq!(
            u32::from_le_bytes(length) as usize,
            bincode::serialize(&expected).unwrap().len()
        );
        assert_eq!(bincode::deserialize::<Message>(&payload).unwrap(), expected);
    }

    #[tokio::test]
    async fn reconnects_once_after_cached_writer_fails() {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let peer_addr = listener.local_addr().unwrap();
        let expected = Message::InstallSnapshot(InstallSnapshotReq {
            term: 23,
            leader_id: 1,
            last_index: 41,
            last_term: 22,
            data: vec![0x5a; 8 * 1024 * 1024],
        });
        let raw_peer = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let mut handshake = [0; 8];
            first.read_exact(&mut handshake).await.unwrap();
            assert_eq!(handshake, 1_u64.to_le_bytes());
            drop(first);

            let (mut second, _) = listener.accept().await.unwrap();
            second.read_exact(&mut handshake).await.unwrap();
            assert_eq!(handshake, 1_u64.to_le_bytes());
            let mut length = [0; 4];
            second.read_exact(&mut length).await.unwrap();
            let mut payload = vec![0; u32::from_le_bytes(length) as usize];
            second.read_exact(&mut payload).await.unwrap();
            bincode::deserialize::<Message>(&payload).unwrap()
        });

        let transport = TcpTransport::bind(
            1,
            SocketAddr::from(([127, 0, 0, 1], 0)),
            HashMap::from([(2, peer_addr)]),
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), transport.send(2, expected.clone()))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), raw_peer)
                .await
                .unwrap()
                .unwrap(),
            expected
        );
    }

    #[tokio::test]
    async fn oversized_inbound_frame_closes_only_its_stream() {
        let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let mut receiver = TcpTransport::bind(2, bind_addr, HashMap::new())
            .await
            .unwrap();
        let mut malformed = TcpStream::connect(receiver.local_addr()).await.unwrap();
        malformed.write_all(&99_u64.to_le_bytes()).await.unwrap();
        malformed
            .write_all(&((super::MAX_FRAME_SIZE as u32) + 1).to_le_bytes())
            .await
            .unwrap();
        drop(malformed);

        let sender = TcpTransport::bind(1, bind_addr, HashMap::from([(2, receiver.local_addr())]))
            .await
            .unwrap();
        let expected = Message::InstallSnapshotResp(InstallSnapshotResp { term: 29 });
        sender.send(2, expected.clone()).await.unwrap();

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap(),
            Some((1, expected))
        );
    }
}
