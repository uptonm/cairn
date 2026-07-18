use super::Transport;
use crate::{Message, NodeId};
use bincode::Options;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch, Mutex, RwLock};
use tokio::task::{JoinHandle, JoinSet};

const INBOUND_CHANNEL_CAPACITY: usize = 256;
const MAX_INBOUND_CONNECTIONS: usize = 32;
const INITIAL_ACCEPT_BACKOFF: Duration = Duration::from_millis(10);
const MAX_ACCEPT_BACKOFF: Duration = Duration::from_secs(1);

struct CachedWriter {
    peer_addr: SocketAddr,
    writer: OwnedWriteHalf,
}

struct EncodedFrame {
    length: [u8; 4],
    payload: Vec<u8>,
}

type WriterSlot = Arc<Mutex<Option<CachedWriter>>>;

pub struct TcpTransport {
    node_id: NodeId,
    local_addr: SocketAddr,
    peers: RwLock<HashMap<NodeId, SocketAddr>>,
    writers: Mutex<HashMap<NodeId, WriterSlot>>,
    receiver: mpsc::Receiver<(NodeId, Message)>,
    shutdown: watch::Sender<bool>,
    _accept_task: JoinHandle<()>,
}

impl TcpTransport {
    pub async fn bind(
        node_id: NodeId,
        bind_addr: SocketAddr,
        peers: HashMap<NodeId, SocketAddr>,
    ) -> crate::Result<TcpTransport> {
        let listener = TcpListener::bind(bind_addr).await?;
        let local_addr = listener.local_addr()?;
        let (sender, receiver) = mpsc::channel(INBOUND_CHANNEL_CAPACITY);
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let accept_task = tokio::spawn(accept_connections(listener, sender, shutdown_receiver));

        Ok(TcpTransport {
            node_id,
            local_addr,
            peers: RwLock::new(peers),
            writers: Mutex::new(HashMap::new()),
            receiver,
            shutdown,
            _accept_task: accept_task,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn set_peer(&self, node_id: NodeId, addr: SocketAddr) {
        self.peers.write().await.insert(node_id, addr);
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
        let _ = self.shutdown.send(true);
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

fn encode_frame(message: &Message) -> crate::Result<EncodedFrame> {
    let payload = bincode::serialize(message).map_err(|error| {
        crate::Error::Corruption(format!("message serialization failed: {error}"))
    })?;
    let payload_len = u32::try_from(payload.len()).map_err(|_| {
        crate::Error::Corruption("message payload does not fit in a u32 frame length".to_string())
    })?;
    Ok(EncodedFrame {
        length: payload_len.to_le_bytes(),
        payload,
    })
}

async fn write_frame_once(
    writer_slot: &mut Option<CachedWriter>,
    node_id: NodeId,
    peer_addr: SocketAddr,
    frame: &EncodedFrame,
) -> crate::Result<()> {
    let mut writer = match writer_slot.take() {
        Some(cached) if cached.peer_addr == peer_addr => cached.writer,
        Some(_) | None => connect_writer(node_id, peer_addr).await?,
    };
    let result = async {
        writer.write_all(&frame.length).await?;
        writer.write_all(&frame.payload).await
    }
    .await;
    match result {
        Ok(()) => {
            *writer_slot = Some(CachedWriter { peer_addr, writer });
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

async fn connect_writer(node_id: NodeId, peer_addr: SocketAddr) -> crate::Result<OwnedWriteHalf> {
    let mut stream = TcpStream::connect(peer_addr).await?;
    stream.write_all(&node_id.to_le_bytes()).await?;
    let (_, writer) = stream.into_split();
    Ok(writer)
}

async fn accept_connections(
    listener: TcpListener,
    sender: mpsc::Sender<(NodeId, Message)>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut readers = JoinSet::new();
    let mut accept_backoff = INITIAL_ACCEPT_BACKOFF;

    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            _ = readers.join_next(), if !readers.is_empty() => {}
            accepted = listener.accept(), if readers.len() < MAX_INBOUND_CONNECTIONS => {
                match accepted {
                    Ok((stream, _)) => {
                        accept_backoff = INITIAL_ACCEPT_BACKOFF;
                        let sender = sender.clone();
                        readers.spawn(async move { read_stream(stream, sender).await });
                    }
                    Err(_) => {
                        let delay = accept_backoff;
                        accept_backoff =
                            accept_backoff.saturating_mul(2).min(MAX_ACCEPT_BACKOFF);
                        let shutdown_requested = tokio::select! {
                            _ = tokio::time::sleep(delay) => false,
                            _ = shutdown.changed() => true,
                        };
                        if shutdown_requested {
                            break;
                        }
                    }
                }
            }
        }
    }

    readers.abort_all();
    while readers.join_next().await.is_some() {}
}

async fn read_stream(
    mut stream: TcpStream,
    sender: mpsc::Sender<(NodeId, Message)>,
) -> crate::Result<()> {
    let mut handshake = [0; 8];
    stream.read_exact(&mut handshake).await?;
    let source_id = NodeId::from_le_bytes(handshake);

    loop {
        let mut length = [0; 4];
        stream.read_exact(&mut length).await?;
        let payload_len = u32::from_le_bytes(length) as usize;
        let mut payload = vec![0; payload_len];
        stream.read_exact(&mut payload).await?;
        let message = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .reject_trailing_bytes()
            .deserialize(&payload)
            .map_err(|error| {
                crate::Error::Corruption(format!("message deserialization failed: {error}"))
            })?;
        sender.send((source_id, message)).await.map_err(|_| {
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
    use std::collections::{HashMap, HashSet};
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;

    async fn write_raw_message(stream: &mut TcpStream, message: &Message) {
        let payload = bincode::serialize(message).unwrap();
        let length = u32::try_from(payload.len()).unwrap();
        stream.write_all(&length.to_le_bytes()).await.unwrap();
        stream.write_all(&payload).await.unwrap();
    }

    async fn read_raw_message(stream: &mut TcpStream) -> Message {
        let mut length = [0; 4];
        stream.read_exact(&mut length).await.unwrap();
        let mut payload = vec![0; u32::from_le_bytes(length) as usize];
        stream.read_exact(&mut payload).await.unwrap();
        bincode::deserialize(&payload).unwrap()
    }

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
    async fn transports_snapshot_larger_than_previous_frame_limit() {
        let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let mut receiver = TcpTransport::bind(2, bind_addr, HashMap::new())
            .await
            .unwrap();
        let sender = TcpTransport::bind(1, bind_addr, HashMap::from([(2, receiver.local_addr())]))
            .await
            .unwrap();
        let expected = Message::InstallSnapshot(InstallSnapshotReq {
            term: 29,
            leader_id: 1,
            last_index: 101,
            last_term: 28,
            data: vec![0x6b; 16 * 1024 * 1024],
        });

        sender.send(2, expected.clone()).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), receiver.recv())
                .await
                .unwrap(),
            Some((1, expected))
        );
    }

    #[tokio::test]
    async fn trailing_bincode_bytes_close_only_the_malformed_stream() {
        let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let mut receiver = TcpTransport::bind(2, bind_addr, HashMap::new())
            .await
            .unwrap();
        let mut malformed = TcpStream::connect(receiver.local_addr()).await.unwrap();
        malformed.write_all(&99_u64.to_le_bytes()).await.unwrap();
        let invalid = Message::InstallSnapshotResp(InstallSnapshotResp { term: 29 });
        let mut payload = bincode::serialize(&invalid).unwrap();
        payload.push(0xff);
        malformed
            .write_all(&u32::try_from(payload.len()).unwrap().to_le_bytes())
            .await
            .unwrap();
        malformed.write_all(&payload).await.unwrap();
        drop(malformed);

        let sender = TcpTransport::bind(1, bind_addr, HashMap::from([(2, receiver.local_addr())]))
            .await
            .unwrap();
        let expected = Message::InstallSnapshotResp(InstallSnapshotResp { term: 31 });
        sender.send(2, expected.clone()).await.unwrap();

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap(),
            Some((1, expected))
        );
    }

    #[tokio::test]
    async fn cancelled_frame_write_forces_a_fresh_connection() {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let peer_addr = listener.local_addr().unwrap();
        let (handshake_seen, handshake_received) = oneshot::channel();
        let (continue_first, continue_first_received) = oneshot::channel();
        let raw_peer = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let mut handshake = [0; 8];
            first.read_exact(&mut handshake).await.unwrap();
            assert_eq!(handshake, 1_u64.to_le_bytes());
            handshake_seen.send(()).unwrap();
            continue_first_received.await.unwrap();

            let mut discarded = Vec::new();
            tokio::time::timeout(Duration::from_secs(1), first.read_to_end(&mut discarded))
                .await
                .unwrap()
                .unwrap();

            let (mut second, _) = listener.accept().await.unwrap();
            second.read_exact(&mut handshake).await.unwrap();
            assert_eq!(handshake, 1_u64.to_le_bytes());
            read_raw_message(&mut second).await
        });

        let transport = Arc::new(
            TcpTransport::bind(
                1,
                SocketAddr::from(([127, 0, 0, 1], 0)),
                HashMap::from([(2, peer_addr)]),
            )
            .await
            .unwrap(),
        );
        let large = Message::InstallSnapshot(InstallSnapshotReq {
            term: 37,
            leader_id: 1,
            last_index: 103,
            last_term: 36,
            data: vec![0x4c; 8 * 1024 * 1024],
        });
        let sending_transport = Arc::clone(&transport);
        let send = tokio::spawn(async move { sending_transport.send(2, large).await });

        handshake_received.await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!send.is_finished(), "large frame write did not block");
        send.abort();
        assert!(send.await.unwrap_err().is_cancelled());
        continue_first.send(()).unwrap();

        let expected = Message::RequestVoteResp(RequestVoteResp {
            term: 38,
            vote_granted: true,
        });
        transport.send(2, expected.clone()).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), raw_peer)
                .await
                .unwrap()
                .unwrap(),
            expected
        );
    }

    #[tokio::test]
    async fn cancelled_peer_update_cannot_reuse_the_old_address() {
        let old_listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let old_addr = old_listener.local_addr().unwrap();
        let new_listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let new_addr = new_listener.local_addr().unwrap();
        let (old_received, first_old_message) = oneshot::channel();
        let (release_old, release_old_received) = oneshot::channel();
        let old_peer = tokio::spawn(async move {
            let (mut stream, _) = old_listener.accept().await.unwrap();
            let mut handshake = [0; 8];
            stream.read_exact(&mut handshake).await.unwrap();
            old_received
                .send(read_raw_message(&mut stream).await)
                .unwrap();
            let _ = release_old_received.await;
        });

        let transport = Arc::new(
            TcpTransport::bind(
                1,
                SocketAddr::from(([127, 0, 0, 1], 0)),
                HashMap::from([(2, old_addr)]),
            )
            .await
            .unwrap(),
        );
        let first = Message::RequestVoteResp(RequestVoteResp {
            term: 41,
            vote_granted: true,
        });
        transport.send(2, first.clone()).await.unwrap();
        assert_eq!(first_old_message.await.unwrap(), first);

        let writer = transport.writer_slot(2).await;
        let writer_guard = writer.lock().await;
        let updating_transport = Arc::clone(&transport);
        let update = tokio::spawn(async move { updating_transport.set_peer(2, new_addr).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if transport.peer_addr(2).await.unwrap() == new_addr {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        update.abort();
        let _ = update.await;
        drop(writer_guard);

        let new_peer = tokio::spawn(async move {
            let (mut stream, _) = new_listener.accept().await.unwrap();
            let mut handshake = [0; 8];
            stream.read_exact(&mut handshake).await.unwrap();
            read_raw_message(&mut stream).await
        });
        let expected = Message::RequestVoteResp(RequestVoteResp {
            term: 42,
            vote_granted: false,
        });
        transport.send(2, expected.clone()).await.unwrap();
        let delivered = tokio::time::timeout(Duration::from_secs(1), new_peer).await;
        let _ = release_old.send(());
        let _ = old_peer.await;

        assert_eq!(delivered.unwrap().unwrap(), expected);
    }

    #[tokio::test]
    async fn dropping_transport_closes_idle_inbound_reader() {
        let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let mut transport = TcpTransport::bind(2, bind_addr, HashMap::new())
            .await
            .unwrap();
        let mut client = TcpStream::connect(transport.local_addr()).await.unwrap();
        client.write_all(&1_u64.to_le_bytes()).await.unwrap();
        let message = Message::InstallSnapshotResp(InstallSnapshotResp { term: 47 });
        write_raw_message(&mut client, &message).await;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), transport.recv())
                .await
                .unwrap(),
            Some((1, message))
        );

        drop(transport);
        let mut byte = [0];
        let closed = tokio::time::timeout(Duration::from_secs(1), client.read(&mut byte)).await;
        assert!(
            matches!(closed, Ok(Ok(0)) | Ok(Err(_))),
            "idle inbound socket remained open after transport drop"
        );
    }

    #[tokio::test]
    async fn bounds_simultaneous_inbound_reader_tasks() {
        const EXPECTED_MAX_CONNECTIONS: usize = 32;

        let mut transport =
            TcpTransport::bind(2, SocketAddr::from(([127, 0, 0, 1], 0)), HashMap::new())
                .await
                .unwrap();
        let mut clients = Vec::new();
        for offset in 0..=EXPECTED_MAX_CONNECTIONS {
            let source_id = 100 + offset as u64;
            let mut client = TcpStream::connect(transport.local_addr()).await.unwrap();
            client.write_all(&source_id.to_le_bytes()).await.unwrap();
            write_raw_message(
                &mut client,
                &Message::InstallSnapshotResp(InstallSnapshotResp { term: source_id }),
            )
            .await;
            clients.push((source_id, client));
        }

        let mut received = HashSet::new();
        for _ in 0..EXPECTED_MAX_CONNECTIONS {
            let (source_id, _) = tokio::time::timeout(Duration::from_secs(1), transport.recv())
                .await
                .unwrap()
                .unwrap();
            received.insert(source_id);
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(50), transport.recv())
                .await
                .is_err(),
            "listener admitted more than the bounded reader count"
        );

        let active = received.iter().next().copied().unwrap();
        let active_index = clients
            .iter()
            .position(|(source_id, _)| *source_id == active)
            .unwrap();
        drop(clients.swap_remove(active_index));
        let (final_source, _) = tokio::time::timeout(Duration::from_secs(1), transport.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(received.insert(final_source));
        assert_eq!(received.len(), EXPECTED_MAX_CONNECTIONS + 1);
    }
}
