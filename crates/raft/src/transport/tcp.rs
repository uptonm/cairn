use super::Transport;
use crate::{Message, NodeId};
use bincode::Options;
use std::collections::HashMap;
use std::io::{ErrorKind, Read};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch, Mutex, OwnedSemaphorePermit, RwLock, Semaphore};
use tokio::task::{JoinHandle, JoinSet};

const INBOUND_CHANNEL_CAPACITY: usize = 256;
const MAX_INBOUND_CONNECTIONS: usize = 32;
const INBOUND_BYTE_BUDGET: usize = 16 * 1024 * 1024;
const INBOUND_READ_CHUNK_SIZE: usize = 64 * 1024;
const INBOUND_IDLE_TIMEOUT: Duration = Duration::from_secs(1);
const INITIAL_ACCEPT_BACKOFF: Duration = Duration::from_millis(10);
const MAX_ACCEPT_BACKOFF: Duration = Duration::from_secs(1);
const MAX_ACCEPT_RETRIES: usize = 8;

struct CachedWriter {
    peer_addr: SocketAddr,
    writer: OwnedWriteHalf,
}

struct EncodedFrame {
    length: [u8; 4],
    payload: Vec<u8>,
}

#[derive(Clone)]
struct InboundBudget {
    semaphore: Arc<Semaphore>,
    capacity: usize,
}

struct InboundMessage {
    source_id: NodeId,
    message: Message,
    _budget_permit: OwnedSemaphorePermit,
}

struct AcceptFailurePolicy {
    retries: usize,
    next_delay: Duration,
}

struct ChunkReader<'a> {
    chunks: &'a [Vec<u8>],
    chunk_index: usize,
    chunk_offset: usize,
    remaining: usize,
}

type WriterSlot = Arc<Mutex<Option<CachedWriter>>>;

pub struct TcpTransport {
    node_id: NodeId,
    local_addr: SocketAddr,
    peers: RwLock<HashMap<NodeId, SocketAddr>>,
    writers: Mutex<HashMap<NodeId, WriterSlot>>,
    receiver: mpsc::Receiver<InboundMessage>,
    shutdown: watch::Sender<bool>,
    _accept_task: JoinHandle<()>,
    #[cfg(test)]
    inbound_budget: InboundBudget,
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
        let inbound_budget = InboundBudget::new(INBOUND_BYTE_BUDGET);
        let accept_task = tokio::spawn(accept_connections(
            listener,
            sender,
            shutdown_receiver,
            inbound_budget.clone(),
        ));

        Ok(TcpTransport {
            node_id,
            local_addr,
            peers: RwLock::new(peers),
            writers: Mutex::new(HashMap::new()),
            receiver,
            shutdown,
            _accept_task: accept_task,
            #[cfg(test)]
            inbound_budget,
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
        let inbound = self.receiver.recv().await?;
        let InboundMessage {
            source_id,
            message,
            _budget_permit,
        } = inbound;
        drop(_budget_permit);
        Some((source_id, message))
    }
}

impl InboundBudget {
    fn new(capacity: usize) -> InboundBudget {
        InboundBudget {
            semaphore: Arc::new(Semaphore::new(capacity)),
            capacity,
        }
    }

    async fn acquire(&self, payload_len: usize) -> crate::Result<OwnedSemaphorePermit> {
        let permit_count = payload_len.min(self.capacity);
        let permit_count = u32::try_from(permit_count).map_err(|_| {
            io_error(
                ErrorKind::InvalidInput,
                "inbound byte budget exceeds semaphore permit range",
            )
        })?;
        Arc::clone(&self.semaphore)
            .acquire_many_owned(permit_count)
            .await
            .map_err(|_| io_error(ErrorKind::BrokenPipe, "inbound byte budget is unavailable"))
    }

    #[cfg(test)]
    fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }
}

impl AcceptFailurePolicy {
    fn new() -> AcceptFailurePolicy {
        AcceptFailurePolicy {
            retries: 0,
            next_delay: INITIAL_ACCEPT_BACKOFF,
        }
    }

    fn next_delay(&mut self) -> Option<Duration> {
        if self.retries == MAX_ACCEPT_RETRIES {
            return None;
        }
        let delay = self.next_delay;
        self.retries += 1;
        self.next_delay = self.next_delay.saturating_mul(2).min(MAX_ACCEPT_BACKOFF);
        Some(delay)
    }

    fn reset(&mut self) {
        self.retries = 0;
        self.next_delay = INITIAL_ACCEPT_BACKOFF;
    }
}

impl<'a> ChunkReader<'a> {
    fn new(chunks: &'a [Vec<u8>], payload_len: usize) -> ChunkReader<'a> {
        ChunkReader {
            chunks,
            chunk_index: 0,
            chunk_offset: 0,
            remaining: payload_len,
        }
    }

    fn remaining(&self) -> usize {
        self.remaining
    }
}

impl Read for ChunkReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let mut written = 0;
        while written < buffer.len() && self.chunk_index < self.chunks.len() {
            let chunk = &self.chunks[self.chunk_index];
            let available = &chunk[self.chunk_offset..];
            if available.is_empty() {
                self.chunk_index += 1;
                self.chunk_offset = 0;
                continue;
            }
            let copy_len = available.len().min(buffer.len() - written);
            buffer[written..written + copy_len].copy_from_slice(&available[..copy_len]);
            written += copy_len;
            self.chunk_offset += copy_len;
            self.remaining -= copy_len;
        }
        Ok(written)
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
    sender: mpsc::Sender<InboundMessage>,
    mut shutdown: watch::Receiver<bool>,
    inbound_budget: InboundBudget,
) {
    let mut readers = JoinSet::new();
    let mut accept_failures = AcceptFailurePolicy::new();

    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            _ = readers.join_next(), if !readers.is_empty() => {}
            accepted = listener.accept(), if readers.len() < MAX_INBOUND_CONNECTIONS => {
                match accepted {
                    Ok((stream, _)) => {
                        accept_failures.reset();
                        let sender = sender.clone();
                        let inbound_budget = inbound_budget.clone();
                        readers.spawn(async move {
                            read_stream(stream, sender, inbound_budget).await
                        });
                    }
                    Err(_) => {
                        let Some(delay) = accept_failures.next_delay() else {
                            break;
                        };
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
    sender: mpsc::Sender<InboundMessage>,
    inbound_budget: InboundBudget,
) -> crate::Result<()> {
    let mut handshake = [0; 8];
    read_exact_with_idle_timeout(&mut stream, &mut handshake).await?;
    let source_id = NodeId::from_le_bytes(handshake);

    loop {
        let mut length = [0; 4];
        read_exact_with_idle_timeout(&mut stream, &mut length).await?;
        let payload_len = u32::from_le_bytes(length) as usize;
        let budget_permit = inbound_budget.acquire(payload_len).await?;
        let payload = read_payload(&mut stream, payload_len).await?;
        let message = {
            let mut payload_reader = ChunkReader::new(&payload, payload_len);
            let message = bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .reject_trailing_bytes()
                .with_limit(payload_len as u64)
                .deserialize_from(&mut payload_reader)
                .map_err(|error| {
                    crate::Error::Corruption(format!("message deserialization failed: {error}"))
                })?;
            if payload_reader.remaining() != 0 {
                return Err(crate::Error::Corruption(
                    "message deserialization left trailing payload bytes".to_string(),
                ));
            }
            message
        };
        drop(payload);
        sender
            .send(InboundMessage {
                source_id,
                message,
                _budget_permit: budget_permit,
            })
            .await
            .map_err(|_| {
                io_error(
                    ErrorKind::BrokenPipe,
                    "TCP transport inbound receiver is closed",
                )
            })?;
    }
}

async fn read_payload(stream: &mut TcpStream, payload_len: usize) -> crate::Result<Vec<Vec<u8>>> {
    let chunk_count = payload_len / INBOUND_READ_CHUNK_SIZE
        + usize::from(!payload_len.is_multiple_of(INBOUND_READ_CHUNK_SIZE));
    let mut chunks = Vec::new();
    chunks.try_reserve_exact(chunk_count).map_err(|error| {
        io_error(
            ErrorKind::OutOfMemory,
            format!("cannot allocate incoming frame chunk index: {error}"),
        )
    })?;

    let mut remaining = payload_len;
    while remaining != 0 {
        let chunk_len = remaining.min(INBOUND_READ_CHUNK_SIZE);
        let mut chunk = allocate_zeroed_chunk(chunk_len)?;
        read_exact_with_idle_timeout(stream, &mut chunk).await?;
        chunks.push(chunk);
        remaining -= chunk_len;
    }
    Ok(chunks)
}

fn allocate_zeroed_chunk(chunk_len: usize) -> crate::Result<Vec<u8>> {
    let mut chunk = Vec::new();
    chunk.try_reserve_exact(chunk_len).map_err(|error| {
        io_error(
            ErrorKind::OutOfMemory,
            format!("cannot allocate incoming frame chunk: {error}"),
        )
    })?;
    chunk.resize(chunk_len, 0);
    Ok(chunk)
}

async fn read_exact_with_idle_timeout(
    stream: &mut TcpStream,
    mut buffer: &mut [u8],
) -> crate::Result<()> {
    while !buffer.is_empty() {
        let bytes_read = tokio::time::timeout(INBOUND_IDLE_TIMEOUT, stream.read(buffer))
            .await
            .map_err(|_| {
                io_error(
                    ErrorKind::TimedOut,
                    "TCP inbound read made no progress before idle deadline",
                )
            })??;
        if bytes_read == 0 {
            return Err(io_error(
                ErrorKind::UnexpectedEof,
                "TCP inbound stream ended before the frame completed",
            ));
        }
        let (_, remaining) = buffer.split_at_mut(bytes_read);
        buffer = remaining;
    }
    Ok(())
}

fn io_error(kind: ErrorKind, message: impl Into<String>) -> crate::Error {
    std::io::Error::new(kind, message.into()).into()
}

#[cfg(test)]
mod tests {
    use super::{
        allocate_zeroed_chunk, AcceptFailurePolicy, InboundBudget, TcpTransport,
        INBOUND_BYTE_BUDGET,
    };
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

    #[tokio::test]
    async fn idle_partial_handshakes_release_reader_slots() {
        const EXPECTED_MAX_CONNECTIONS: usize = 32;

        let mut transport =
            TcpTransport::bind(2, SocketAddr::from(([127, 0, 0, 1], 0)), HashMap::new())
                .await
                .unwrap();
        let mut idle_clients = Vec::new();
        for _ in 0..EXPECTED_MAX_CONNECTIONS {
            let mut client = TcpStream::connect(transport.local_addr()).await.unwrap();
            client.write_all(&[0x01]).await.unwrap();
            idle_clients.push(client);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut valid = TcpStream::connect(transport.local_addr()).await.unwrap();
        valid.write_all(&77_u64.to_le_bytes()).await.unwrap();
        let expected = Message::InstallSnapshotResp(InstallSnapshotResp { term: 53 });
        write_raw_message(&mut valid, &expected).await;

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), transport.recv())
                .await
                .unwrap(),
            Some((77, expected))
        );
    }

    #[tokio::test]
    async fn byte_budget_bounds_normal_frames_and_exclusively_admits_oversized() {
        let budget = InboundBudget::new(10);
        let first = budget.acquire(6).await.unwrap();
        let waiting_budget = budget.clone();
        let mut second = tokio::spawn(async move { waiting_budget.acquire(5).await.unwrap() });
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut second)
            .await
            .is_err());
        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .unwrap()
            .unwrap();

        let oversized_budget = budget.clone();
        let mut oversized =
            tokio::spawn(async move { oversized_budget.acquire(11).await.unwrap() });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut oversized)
                .await
                .is_err()
        );
        drop(second);
        let oversized = tokio::time::timeout(Duration::from_secs(1), oversized)
            .await
            .unwrap()
            .unwrap();

        let normal_budget = budget.clone();
        let mut normal = tokio::spawn(async move { normal_budget.acquire(1).await.unwrap() });
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut normal)
            .await
            .is_err());
        drop(oversized);
        let _ = tokio::time::timeout(Duration::from_secs(1), normal)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn queued_message_holds_budget_until_recv() {
        let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let mut receiver = TcpTransport::bind(2, bind_addr, HashMap::new())
            .await
            .unwrap();
        let sender = TcpTransport::bind(1, bind_addr, HashMap::from([(2, receiver.local_addr())]))
            .await
            .unwrap();
        let expected = Message::InstallSnapshot(InstallSnapshotReq {
            term: 59,
            leader_id: 1,
            last_index: 113,
            last_term: 58,
            data: vec![0x2a; 4096],
        });
        let encoded_len = bincode::serialize(&expected).unwrap().len();

        sender.send(2, expected.clone()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if receiver.inbound_budget.available_permits() == INBOUND_BYTE_BUDGET - encoded_len
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        assert_eq!(receiver.recv().await, Some((1, expected)));
        assert_eq!(
            receiver.inbound_budget.available_permits(),
            INBOUND_BYTE_BUDGET
        );
    }

    #[test]
    fn payload_chunk_allocation_failure_is_recoverable() {
        assert!(allocate_zeroed_chunk(usize::MAX).is_err());
    }

    #[test]
    fn accept_failure_policy_terminates_after_bounded_retries() {
        let mut policy = AcceptFailurePolicy::new();
        let delays: Vec<_> = std::iter::from_fn(|| policy.next_delay()).collect();

        assert_eq!(
            delays,
            vec![
                Duration::from_millis(10),
                Duration::from_millis(20),
                Duration::from_millis(40),
                Duration::from_millis(80),
                Duration::from_millis(160),
                Duration::from_millis(320),
                Duration::from_millis(640),
                Duration::from_secs(1),
            ]
        );
        assert_eq!(policy.next_delay(), None);

        policy.reset();
        assert_eq!(policy.next_delay(), Some(Duration::from_millis(10)));
    }
}
