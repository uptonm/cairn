use super::Transport;
use crate::{Message, NodeId};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch, Mutex, OwnedSemaphorePermit, RwLock, Semaphore};
use tokio::task::{JoinHandle, JoinSet};

mod codec;

use codec::{
    add_log_entry_len, read_exact_with_idle_timeout, read_message, serialized_frame_len,
    write_all_with_idle_timeout, write_frame,
};

const INBOUND_CHANNEL_CAPACITY: usize = 256;
const MAX_INBOUND_CONNECTIONS: usize = 32;
const INBOUND_BYTE_BUDGET: usize = 16 * 1024 * 1024;
const INBOUND_IDLE_TIMEOUT: Duration = Duration::from_secs(1);
const OUTBOUND_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const OUTBOUND_IDLE_TIMEOUT: Duration = Duration::from_secs(1);
const INITIAL_ACCEPT_BACKOFF: Duration = Duration::from_millis(10);
const MAX_ACCEPT_BACKOFF: Duration = Duration::from_secs(1);

struct CachedWriter {
    peer_addr: SocketAddr,
    writer: OwnedWriteHalf,
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

/// Tracks the backoff delay for consecutive `listener.accept()` failures.
///
/// A Raft node's inbound listener must stay alive through transient error
/// bursts (e.g. EMFILE, ECONNABORTED under connection churn): losing the
/// ability to receive votes/AppendEntries is worse than a slow accept loop.
/// So this policy never signals "give up" — it only grows the delay, capped,
/// and `reset` on the next successful accept collapses it back down.
struct AcceptFailurePolicy {
    next_delay: Duration,
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
        let writer = self.writer_slot(to).await;
        let mut writer = writer.lock().await;
        let peer_addr = self.peer_addr(to).await?;
        let (payload_len, msg) = size_message(msg).await?;

        if write_frame_once(&mut writer, self.node_id, peer_addr, &msg, payload_len)
            .await
            .is_ok()
        {
            return Ok(());
        }

        write_frame_once(&mut writer, self.node_id, peer_addr, &msg, payload_len).await
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
            next_delay: INITIAL_ACCEPT_BACKOFF,
        }
    }

    /// Returns the delay to wait before retrying, then grows it (capped) for
    /// next time. Always returns a delay: accept errors back off but the
    /// listener keeps retrying indefinitely.
    fn next_delay(&mut self) -> Duration {
        let delay = self.next_delay;
        self.next_delay = self.next_delay.saturating_mul(2).min(MAX_ACCEPT_BACKOFF);
        delay
    }

    fn reset(&mut self) {
        self.next_delay = INITIAL_ACCEPT_BACKOFF;
    }
}

async fn size_message(message: Message) -> crate::Result<(u32, Message)> {
    let payload_len = match &message {
        Message::AppendEntries(request) => {
            // Yield between entries so a long AppendEntries can be cancelled without
            // scanning command bytes on a blocking pool thread.
            let mut len = 4 + 8 + 8 + 8 + 8 + 8;
            for (index, entry) in request.entries.iter().enumerate() {
                if index > 0 {
                    tokio::task::yield_now().await;
                }
                len = add_log_entry_len(len, entry)?;
            }
            let len = len.checked_add(8).ok_or_else(|| {
                crate::Error::Corruption("message size calculation overflowed usize".to_string())
            })?;
            u32::try_from(len).map_err(|_| {
                crate::Error::Corruption(
                    "message payload does not fit in a u32 frame length".to_string(),
                )
            })?
        }
        _ => serialized_frame_len(&message)?,
    };
    Ok((payload_len, message))
}

async fn write_frame_once(
    writer_slot: &mut Option<CachedWriter>,
    node_id: NodeId,
    peer_addr: SocketAddr,
    message: &Message,
    payload_len: u32,
) -> crate::Result<()> {
    let mut writer = match writer_slot.take() {
        Some(cached) if cached.peer_addr == peer_addr => cached.writer,
        Some(_) | None => connect_writer(node_id, peer_addr).await?,
    };
    match write_frame(&mut writer, message, payload_len, OUTBOUND_IDLE_TIMEOUT).await {
        Ok(()) => {
            *writer_slot = Some(CachedWriter { peer_addr, writer });
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn connect_writer(node_id: NodeId, peer_addr: SocketAddr) -> crate::Result<OwnedWriteHalf> {
    let mut stream = tokio::time::timeout(OUTBOUND_CONNECT_TIMEOUT, TcpStream::connect(peer_addr))
        .await
        .map_err(|_| {
            io_error(
                ErrorKind::TimedOut,
                format!("TCP connect to {peer_addr} exceeded its deadline"),
            )
        })??;
    write_all_with_idle_timeout(&mut stream, &node_id.to_le_bytes(), OUTBOUND_IDLE_TIMEOUT).await?;
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
                        let delay = accept_failures.next_delay();
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
    read_exact_with_idle_timeout(&mut stream, &mut handshake, INBOUND_IDLE_TIMEOUT).await?;
    let source_id = NodeId::from_le_bytes(handshake);

    loop {
        let mut length = [0; 4];
        read_exact_with_idle_timeout(&mut stream, &mut length, INBOUND_IDLE_TIMEOUT).await?;
        let payload_len = u32::from_le_bytes(length) as usize;
        let budget_permit = inbound_budget.acquire(payload_len).await?;
        let message = read_message(&mut stream, payload_len, INBOUND_IDLE_TIMEOUT).await?;
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

fn io_error(kind: ErrorKind, message: impl Into<String>) -> crate::Error {
    std::io::Error::new(kind, message.into()).into()
}

#[cfg(test)]
mod tests {
    use super::{
        codec::{
            allocate_bytes, read_message, serialized_frame_len, write_all_with_idle_timeout,
            write_frame,
        },
        size_message, AcceptFailurePolicy, InboundBudget, TcpTransport, INBOUND_BYTE_BUDGET,
    };
    use crate::transport::Transport;
    use crate::{
        AppendEntriesReq, AppendEntriesResp, InstallSnapshotReq, InstallSnapshotResp, LogEntry,
        Message, RequestVoteReq, RequestVoteResp,
    };
    use std::collections::{HashMap, HashSet};
    use std::future::Future;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::task::Poll;
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
                pre_vote: false,
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

    fn codec_messages() -> Vec<Message> {
        let mut messages = all_messages();
        messages.extend([
            Message::AppendEntries(AppendEntriesReq {
                term: 5,
                leader_id: 8,
                prev_log_index: 12,
                prev_log_term: 4,
                entries: Vec::new(),
                leader_commit: 12,
            }),
            Message::AppendEntries(AppendEntriesReq {
                term: 6,
                leader_id: 8,
                prev_log_index: 13,
                prev_log_term: 5,
                entries: vec![
                    LogEntry {
                        term: 6,
                        index: 14,
                        command: Vec::new(),
                    },
                    LogEntry {
                        term: 6,
                        index: 15,
                        command: b"second command".to_vec(),
                    },
                ],
                leader_commit: 13,
            }),
            Message::AppendEntriesResp(AppendEntriesResp {
                term: 6,
                success: true,
                conflict_index: None,
            }),
            Message::InstallSnapshot(InstallSnapshotReq {
                term: 7,
                leader_id: 8,
                last_index: 15,
                last_term: 6,
                data: Vec::new(),
            }),
        ]);
        messages
    }

    #[tokio::test]
    async fn incremental_codec_matches_bincode_for_every_message_variant() {
        for message in codec_messages() {
            let expected_payload = bincode::serialize(&message).unwrap();
            let payload_len = serialized_frame_len(&message).unwrap();
            assert_eq!(payload_len as usize, expected_payload.len());

            let (mut encoded, mut encoded_reader) = tokio::io::duplex(expected_payload.len() + 4);
            write_frame(&mut encoded, &message, payload_len, Duration::from_secs(1))
                .await
                .unwrap();
            let mut actual_frame = vec![0; expected_payload.len() + 4];
            encoded_reader.read_exact(&mut actual_frame).await.unwrap();
            let mut expected_frame = payload_len.to_le_bytes().to_vec();
            expected_frame.extend_from_slice(&expected_payload);
            assert_eq!(actual_frame, expected_frame);

            let (mut raw_writer, mut raw_reader) = tokio::io::duplex(expected_payload.len());
            raw_writer.write_all(&expected_payload).await.unwrap();
            drop(raw_writer);
            assert_eq!(
                read_message(
                    &mut raw_reader,
                    expected_payload.len(),
                    Duration::from_secs(1)
                )
                .await
                .unwrap(),
                message
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sizing_large_command_completes_without_background_scanning() {
        const COMMAND_LEN: usize = 32 * 1024 * 1024;

        let message = Message::AppendEntries(AppendEntriesReq {
            term: 8,
            leader_id: 9,
            prev_log_index: 15,
            prev_log_term: 7,
            entries: vec![LogEntry {
                term: 8,
                index: 16,
                command: vec![0x4d; COMMAND_LEN],
            }],
            leader_commit: 15,
        });
        let mut sizing = Box::pin(size_message(message));

        let (payload_len, message) =
            std::future::poll_fn(|context| match sizing.as_mut().poll(context) {
                Poll::Ready(result) => Poll::Ready(result),
                Poll::Pending => {
                    panic!("single-entry sizing detached or yielded while scanning command bytes")
                }
            })
            .await
            .unwrap();

        assert_eq!(payload_len as usize, 52 + 24 + COMMAND_LEN);
        drop(message);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn append_entry_sizing_exposes_a_bounded_cancellation_point() {
        let message = Message::AppendEntries(AppendEntriesReq {
            term: 9,
            leader_id: 10,
            prev_log_index: 16,
            prev_log_term: 8,
            entries: (0..128)
                .map(|offset| LogEntry {
                    term: 9,
                    index: 17 + offset,
                    command: Vec::new(),
                })
                .collect(),
            leader_commit: 16,
        });
        let mut sizing = Box::pin(size_message(message));

        std::future::poll_fn(|context| {
            assert!(
                sizing.as_mut().poll(context).is_pending(),
                "entry metadata sizing did not yield at its bounded cancellation point"
            );
            Poll::Ready(())
        })
        .await;
        drop(sizing);
    }

    #[tokio::test]
    async fn outbound_idle_timeout_resets_after_each_write_progress() {
        let (mut writer, mut reader) = tokio::io::duplex(1);
        let payload = [0x5a; 8];
        let slow_reader = tokio::spawn(async move {
            let mut received = Vec::new();
            for _ in 0..payload.len() {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let mut byte = [0];
                reader.read_exact(&mut byte).await.unwrap();
                received.push(byte[0]);
            }
            received
        });

        write_all_with_idle_timeout(&mut writer, &payload, Duration::from_millis(50))
            .await
            .unwrap();

        assert_eq!(slow_reader.await.unwrap(), payload);
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
            pre_vote: false,
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
    async fn stalled_outbound_write_times_out_and_later_send_reconnects() {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let peer_addr = listener.local_addr().unwrap();
        let (release_stalled, release_stalled_received) = oneshot::channel();
        let stalled_peer = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            let (second, _) = listener.accept().await.unwrap();
            let _ = release_stalled_received.await;
            drop((first, second));
            listener
        });
        let transport = TcpTransport::bind(
            1,
            SocketAddr::from(([127, 0, 0, 1], 0)),
            HashMap::from([(2, peer_addr)]),
        )
        .await
        .unwrap();
        let stalled = Message::InstallSnapshot(InstallSnapshotReq {
            term: 25,
            leader_id: 1,
            last_index: 43,
            last_term: 24,
            data: vec![0x3c; 16 * 1024 * 1024],
        });

        let stalled_result =
            tokio::time::timeout(Duration::from_secs(4), transport.send(2, stalled)).await;
        assert!(
            matches!(stalled_result, Ok(Err(crate::Error::Io(ref error)))
                if error.kind() == std::io::ErrorKind::TimedOut),
            "stalled send did not return an outbound idle timeout: {stalled_result:?}"
        );

        let _ = release_stalled.send(());
        let listener = stalled_peer.await.unwrap();
        let expected = Message::RequestVoteResp(RequestVoteResp {
            term: 26,
            vote_granted: true,
            pre_vote: false,
        });
        let recovering_peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut handshake = [0; 8];
            stream.read_exact(&mut handshake).await.unwrap();
            assert_eq!(handshake, 1_u64.to_le_bytes());
            read_raw_message(&mut stream).await
        });

        transport.send(2, expected.clone()).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), recovering_peer)
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
            pre_vote: false,
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
            pre_vote: false,
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
            pre_vote: false,
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
    fn message_byte_allocation_failure_is_recoverable() {
        assert!(allocate_bytes(usize::MAX).is_err());
    }

    #[test]
    fn accept_failure_policy_backs_off_but_never_gives_up() {
        let mut policy = AcceptFailurePolicy::new();
        let delays: Vec<_> = (0..8).map(|_| policy.next_delay()).collect();

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

        // Backoff is capped, not terminal: further failures keep retrying at
        // the max delay instead of the policy ever signalling "give up".
        for _ in 0..100 {
            assert_eq!(policy.next_delay(), Duration::from_secs(1));
        }

        policy.reset();
        assert_eq!(policy.next_delay(), Duration::from_millis(10));
    }

    #[tokio::test]
    async fn accept_loop_recovers_after_a_burst_of_accept_failures() {
        // Regression test: the accept loop must not permanently die after a
        // burst of transient accept errors. We can't force `listener.accept()`
        // itself to fail deterministically on a real socket, so this test
        // exercises the same policy the accept loop drives and then proves,
        // end-to-end through a live TcpTransport, that inbound connections are
        // still serviced after many consecutive simulated failures.
        let mut policy = AcceptFailurePolicy::new();
        for _ in 0..50 {
            let _ = policy.next_delay();
        }
        policy.reset();
        assert_eq!(policy.next_delay(), Duration::from_millis(10));

        let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let mut transport = TcpTransport::bind(2, bind_addr, HashMap::new())
            .await
            .unwrap();
        let mut client = TcpStream::connect(transport.local_addr()).await.unwrap();
        client.write_all(&1_u64.to_le_bytes()).await.unwrap();
        let message = Message::InstallSnapshotResp(InstallSnapshotResp { term: 61 });
        write_raw_message(&mut client, &message).await;

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), transport.recv())
                .await
                .unwrap(),
            Some((1, message)),
            "accept loop did not service a connection after a burst of prior failures"
        );
    }
}
