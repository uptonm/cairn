use super::Transport;
use crate::{Message, NodeId};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::io::ErrorKind;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

type Destination = mpsc::UnboundedSender<(NodeId, Message)>;

#[derive(Clone)]
pub struct InMemoryNetwork {
    faults: Arc<Mutex<FaultState>>,
}

pub struct InMemoryTransport {
    node_id: NodeId,
    faults: Arc<Mutex<FaultState>>,
    destinations: Arc<HashMap<NodeId, Destination>>,
    scheduler: mpsc::UnboundedSender<ScheduledDelivery>,
    receiver: mpsc::UnboundedReceiver<(NodeId, Message)>,
}

struct FaultState {
    node_ids: HashSet<NodeId>,
    partitions: HashSet<(NodeId, NodeId)>,
    drops: HashMap<(NodeId, NodeId), u64>,
    delays: HashMap<(NodeId, NodeId), Duration>,
    next_sequence: u128,
}

struct ScheduledDelivery {
    deadline: Instant,
    sequence: u128,
    from: NodeId,
    to: NodeId,
    message: Message,
    acknowledgement: oneshot::Sender<crate::Result<()>>,
}

impl PartialEq for ScheduledDelivery {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.sequence == other.sequence
    }
}

impl Eq for ScheduledDelivery {}

impl PartialOrd for ScheduledDelivery {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledDelivery {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .deadline
            .cmp(&self.deadline)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

impl InMemoryTransport {
    pub fn cluster(
        node_ids: impl IntoIterator<Item = NodeId>,
        seed: u64,
    ) -> crate::Result<(InMemoryNetwork, HashMap<NodeId, InMemoryTransport>)> {
        let mut ordered_node_ids = Vec::new();
        let mut members = HashSet::new();
        for node_id in node_ids {
            if !members.insert(node_id) {
                return Err(io_error(
                    ErrorKind::InvalidInput,
                    format!("duplicate node id {node_id}"),
                ));
            }
            ordered_node_ids.push(node_id);
        }

        let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
            io_error(
                ErrorKind::Other,
                format!("in-memory transport requires a Tokio runtime: {error}"),
            )
        })?;
        let faults = Arc::new(Mutex::new(FaultState {
            node_ids: members,
            partitions: HashSet::new(),
            drops: HashMap::new(),
            delays: HashMap::new(),
            next_sequence: u128::from(seed),
        }));
        let (scheduler, scheduler_receiver) = mpsc::unbounded_channel();
        let mut destinations = HashMap::new();
        let mut receivers = HashMap::new();

        for node_id in &ordered_node_ids {
            let (sender, receiver) = mpsc::unbounded_channel();
            destinations.insert(*node_id, sender);
            receivers.insert(*node_id, receiver);
        }

        let destinations = Arc::new(destinations);
        let mut endpoints = HashMap::new();
        for node_id in ordered_node_ids {
            let receiver = receivers.remove(&node_id).ok_or_else(|| {
                io_error(
                    ErrorKind::Other,
                    format!("receiver missing for node {node_id}"),
                )
            })?;
            endpoints.insert(
                node_id,
                InMemoryTransport {
                    node_id,
                    faults: Arc::clone(&faults),
                    destinations: Arc::clone(&destinations),
                    scheduler: scheduler.clone(),
                    receiver,
                },
            );
        }

        runtime.spawn(run_scheduler(scheduler_receiver, Arc::clone(&destinations)));

        Ok((InMemoryNetwork { faults }, endpoints))
    }
}

impl InMemoryNetwork {
    pub fn partition(&self, a: NodeId, b: NodeId) -> crate::Result<()> {
        let mut faults = lock_faults(&self.faults)?;
        validate_node(&faults, a)?;
        validate_node(&faults, b)?;
        faults.partitions.insert(partition_key(a, b));
        Ok(())
    }

    pub fn heal(&self) -> crate::Result<()> {
        lock_faults(&self.faults)?.partitions.clear();
        Ok(())
    }

    pub fn drop_next(&self, from: NodeId, to: NodeId) -> crate::Result<()> {
        let mut faults = lock_faults(&self.faults)?;
        validate_node(&faults, from)?;
        validate_node(&faults, to)?;
        let count = faults.drops.entry((from, to)).or_default();
        *count = count.checked_add(1).ok_or_else(|| {
            io_error(
                ErrorKind::Other,
                format!("drop counter exhausted for {from} -> {to}"),
            )
        })?;
        Ok(())
    }

    pub fn set_delay(&self, from: NodeId, to: NodeId, delay: Duration) -> crate::Result<()> {
        let mut faults = lock_faults(&self.faults)?;
        validate_node(&faults, from)?;
        validate_node(&faults, to)?;
        if delay.is_zero() {
            faults.delays.remove(&(from, to));
        } else {
            faults.delays.insert((from, to), delay);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Transport for InMemoryTransport {
    async fn send(&self, to: NodeId, msg: Message) -> crate::Result<()> {
        let acknowledgement = {
            let mut faults = lock_faults(&self.faults)?;
            validate_node(&faults, to)?;

            let destination = self.destinations.get(&to).ok_or_else(|| {
                io_error(
                    ErrorKind::InvalidInput,
                    format!("unknown destination node {to}"),
                )
            })?;
            if destination.is_closed() {
                return Err(io_error(
                    ErrorKind::BrokenPipe,
                    format!("destination node {to} is closed"),
                ));
            }

            if faults.partitions.contains(&partition_key(self.node_id, to)) {
                return Ok(());
            }

            let direction = (self.node_id, to);
            if let Some(remaining) = faults.drops.get_mut(&direction) {
                *remaining -= 1;
                if *remaining == 0 {
                    faults.drops.remove(&direction);
                }
                return Ok(());
            }

            let delay = faults.delays.get(&direction).copied().unwrap_or_default();
            let deadline = Instant::now().checked_add(delay).ok_or_else(|| {
                io_error(
                    ErrorKind::InvalidInput,
                    format!("delay is too large for {} -> {to}", self.node_id),
                )
            })?;
            let sequence = faults.next_sequence;
            faults.next_sequence = sequence.checked_add(1).ok_or_else(|| {
                io_error(ErrorKind::Other, "in-memory transport sequence exhausted")
            })?;
            let (acknowledgement, acknowledgement_receiver) = oneshot::channel();
            self.scheduler
                .send(ScheduledDelivery {
                    deadline,
                    sequence,
                    from: self.node_id,
                    to,
                    message: msg,
                    acknowledgement,
                })
                .map_err(|_| {
                    io_error(
                        ErrorKind::BrokenPipe,
                        "in-memory transport scheduler is closed",
                    )
                })?;
            acknowledgement_receiver
        };

        acknowledgement.await.map_err(|_| {
            io_error(
                ErrorKind::BrokenPipe,
                "in-memory transport scheduler stopped before delivery",
            )
        })?
    }

    async fn recv(&mut self) -> Option<(NodeId, Message)> {
        self.receiver.recv().await
    }
}

async fn run_scheduler(
    mut incoming: mpsc::UnboundedReceiver<ScheduledDelivery>,
    destinations: Arc<HashMap<NodeId, Destination>>,
) {
    const MAX_INCOMING_BATCH: usize = 64;

    let mut pending = BinaryHeap::new();

    loop {
        if incoming.is_closed() {
            fail_queued_deliveries(&mut incoming, &mut pending);
            return;
        }

        if pending
            .peek()
            .is_some_and(|delivery| delivery.deadline <= Instant::now())
        {
            if let Some(delivery) = pending.pop() {
                deliver(delivery, &destinations);
            }
            tokio::task::yield_now().await;
            continue;
        }

        let mut batch_size = 0;
        while batch_size < MAX_INCOMING_BATCH {
            match incoming.try_recv() {
                Ok(delivery) => {
                    pending.push(delivery);
                    batch_size += 1;
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    fail_queued_deliveries(&mut incoming, &mut pending);
                    return;
                }
            }
        }

        if batch_size == MAX_INCOMING_BATCH {
            tokio::task::yield_now().await;
            continue;
        }

        let Some(next_deadline) = pending.peek().map(|delivery| delivery.deadline) else {
            match incoming.recv().await {
                Some(delivery) => pending.push(delivery),
                None => {
                    fail_queued_deliveries(&mut incoming, &mut pending);
                    return;
                }
            }
            continue;
        };

        tokio::select! {
            delivery = incoming.recv() => {
                match delivery {
                    Some(delivery) => pending.push(delivery),
                    None => {
                        fail_queued_deliveries(&mut incoming, &mut pending);
                        return;
                    }
                }
            }
            _ = tokio::time::sleep_until(next_deadline) => {}
        }
    }
}

fn fail_queued_deliveries(
    incoming: &mut mpsc::UnboundedReceiver<ScheduledDelivery>,
    pending: &mut BinaryHeap<ScheduledDelivery>,
) {
    while let Ok(delivery) = incoming.try_recv() {
        fail_delivery(delivery);
    }
    while let Some(delivery) = pending.pop() {
        fail_delivery(delivery);
    }
}

fn fail_delivery(delivery: ScheduledDelivery) {
    let _ = delivery.acknowledgement.send(Err(io_error(
        ErrorKind::BrokenPipe,
        "in-memory transport scheduler input is closed",
    )));
}

fn deliver(delivery: ScheduledDelivery, destinations: &HashMap<NodeId, Destination>) {
    let result = destinations
        .get(&delivery.to)
        .ok_or_else(|| {
            io_error(
                ErrorKind::InvalidInput,
                format!("unknown destination node {}", delivery.to),
            )
        })
        .and_then(|destination| {
            destination
                .send((delivery.from, delivery.message))
                .map_err(|_| {
                    io_error(
                        ErrorKind::BrokenPipe,
                        format!("destination node {} is closed", delivery.to),
                    )
                })
        });
    let _ = delivery.acknowledgement.send(result);
}

fn validate_node(faults: &FaultState, node_id: NodeId) -> crate::Result<()> {
    if faults.node_ids.contains(&node_id) {
        Ok(())
    } else {
        Err(io_error(
            ErrorKind::InvalidInput,
            format!("unknown node id {node_id}"),
        ))
    }
}

fn partition_key(a: NodeId, b: NodeId) -> (NodeId, NodeId) {
    if a <= b {
        (a, b)
    } else {
        (b, a)
    }
}

fn lock_faults(faults: &Mutex<FaultState>) -> crate::Result<MutexGuard<'_, FaultState>> {
    faults
        .lock()
        .map_err(|_| io_error(ErrorKind::Other, "in-memory transport state is poisoned"))
}

fn io_error(kind: ErrorKind, message: impl Into<String>) -> crate::Error {
    std::io::Error::new(kind, message.into()).into()
}

#[cfg(test)]
mod tests {
    use super::{run_scheduler, InMemoryTransport, ScheduledDelivery};
    use crate::rpc::{Message, RequestVoteResp};
    use crate::transport::Transport;
    use crate::Error;
    use std::collections::HashMap;
    use std::future::Future;
    use std::sync::Arc;
    use std::task::Poll;
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};
    use tokio::time::Instant;

    fn message(term: u64) -> Message {
        Message::RequestVoteResp(RequestVoteResp {
            term,
            vote_granted: true,
        })
    }

    #[tokio::test]
    async fn delivers_messages_between_three_nodes() {
        let (_network, mut endpoints) = InMemoryTransport::cluster([1, 2, 3], 17).unwrap();
        let one = endpoints.remove(&1).unwrap();
        let mut two = endpoints.remove(&2).unwrap();
        let mut three = endpoints.remove(&3).unwrap();

        one.send(2, message(12)).await.unwrap();
        one.send(3, message(13)).await.unwrap();

        assert_eq!(two.recv().await, Some((1, message(12))));
        assert_eq!(three.recv().await, Some((1, message(13))));
    }

    #[tokio::test]
    async fn partition_blocks_delivery_until_healed() {
        let (network, mut endpoints) = InMemoryTransport::cluster([1, 2], 23).unwrap();
        let one = endpoints.remove(&1).unwrap();
        let mut two = endpoints.remove(&2).unwrap();

        network.partition(1, 2).unwrap();
        one.send(2, message(21)).await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(10), two.recv())
            .await
            .is_err());

        network.heal().unwrap();
        one.send(2, message(22)).await.unwrap();
        assert_eq!(two.recv().await, Some((1, message(22))));
    }

    #[tokio::test]
    async fn drops_exactly_one_message_in_the_configured_direction() {
        let (network, mut endpoints) = InMemoryTransport::cluster([1, 2], 29).unwrap();
        let mut one = endpoints.remove(&1).unwrap();
        let mut two = endpoints.remove(&2).unwrap();

        network.drop_next(1, 2).unwrap();
        one.send(2, message(31)).await.unwrap();
        one.send(2, message(32)).await.unwrap();
        two.send(1, message(33)).await.unwrap();

        assert_eq!(two.recv().await, Some((1, message(32))));
        assert_eq!(one.recv().await, Some((2, message(33))));
    }

    #[tokio::test]
    async fn applies_directional_delay_before_delivery() {
        let (network, mut endpoints) = InMemoryTransport::cluster([1, 2], 37).unwrap();
        let one = endpoints.remove(&1).unwrap();
        let mut two = endpoints.remove(&2).unwrap();

        network.set_delay(1, 2, Duration::from_millis(30)).unwrap();
        let send = tokio::spawn(async move { one.send(2, message(41)).await });

        assert!(tokio::time::timeout(Duration::from_millis(5), two.recv())
            .await
            .is_err());
        send.await.unwrap().unwrap();
        assert_eq!(two.recv().await, Some((1, message(41))));
    }

    #[tokio::test]
    async fn rejects_duplicate_and_unknown_node_ids() {
        assert!(matches!(
            InMemoryTransport::cluster([1, 1], 43),
            Err(Error::Io(_))
        ));

        let (network, mut endpoints) = InMemoryTransport::cluster([1, 2], 47).unwrap();
        let one = endpoints.remove(&1).unwrap();

        assert!(matches!(network.partition(1, 3), Err(Error::Io(_))));
        assert!(matches!(network.drop_next(3, 1), Err(Error::Io(_))));
        assert!(matches!(
            network.set_delay(1, 3, Duration::from_millis(1)),
            Err(Error::Io(_))
        ));
        assert!(matches!(one.send(3, message(51)).await, Err(Error::Io(_))));
    }

    #[tokio::test]
    async fn sending_to_a_closed_destination_returns_an_io_error() {
        let (_network, mut endpoints) = InMemoryTransport::cluster([1, 2], 53).unwrap();
        let one = endpoints.remove(&1).unwrap();
        drop(endpoints.remove(&2));

        assert!(matches!(one.send(2, message(61)).await, Err(Error::Io(_))));
    }

    #[tokio::test]
    async fn scheduler_yields_between_batches_on_current_thread() {
        let (incoming, incoming_receiver) = mpsc::unbounded_channel();
        let (destination, mut destination_receiver) = mpsc::unbounded_channel();
        let destinations = Arc::new(HashMap::from([(2, destination)]));
        let (due_acknowledgement, mut due_result) = oneshot::channel();
        let now = Instant::now();

        incoming
            .send(ScheduledDelivery {
                deadline: now,
                sequence: 0,
                from: 1,
                to: 2,
                message: message(71),
                acknowledgement: due_acknowledgement,
            })
            .unwrap();
        for sequence in 1..=1_024 {
            let (acknowledgement, result) = oneshot::channel();
            drop(result);
            incoming
                .send(ScheduledDelivery {
                    deadline: now + Duration::from_secs(60),
                    sequence,
                    from: 1,
                    to: 2,
                    message: message(72),
                    acknowledgement,
                })
                .unwrap();
        }

        let mut scheduler = Box::pin(run_scheduler(incoming_receiver, destinations));
        std::future::poll_fn(|context| {
            assert!(scheduler.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;

        assert!(
            matches!(
                due_result.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ),
            "scheduler did not yield before servicing the full input backlog"
        );

        let scheduler = tokio::spawn(scheduler);
        let result = tokio::time::timeout(Duration::from_secs(1), &mut due_result).await;
        scheduler.abort();
        drop(incoming);
        let _ = scheduler.await;

        assert!(
            result.is_ok(),
            "due delivery was starved while scheduler drained incoming work"
        );
        assert!(result.unwrap().unwrap().is_ok());
        assert_eq!(destination_receiver.recv().await, Some((1, message(71))));
    }

    #[tokio::test]
    async fn pending_deliveries_fail_when_scheduler_input_closes() {
        let (incoming, incoming_receiver) = mpsc::unbounded_channel();
        let (destination, _destination_receiver) = mpsc::unbounded_channel();
        let destinations = Arc::new(HashMap::from([(2, destination)]));
        let (acknowledgement, result) = oneshot::channel();

        incoming
            .send(ScheduledDelivery {
                deadline: Instant::now() + Duration::from_secs(60),
                sequence: 0,
                from: 1,
                to: 2,
                message: message(81),
                acknowledgement,
            })
            .unwrap();
        drop(incoming);

        let mut scheduler = tokio::spawn(run_scheduler(incoming_receiver, destinations));
        let acknowledgement = tokio::time::timeout(Duration::from_millis(50), result).await;
        let exit = tokio::time::timeout(Duration::from_millis(50), &mut scheduler).await;
        if exit.is_err() {
            scheduler.abort();
            let _ = scheduler.await;
        }

        assert!(matches!(acknowledgement, Ok(Ok(Err(Error::Io(_))))));
        assert!(exit.is_ok(), "scheduler did not exit when input closed");
    }
}
