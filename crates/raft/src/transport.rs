use crate::{Message, NodeId};

pub mod in_memory;

#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, to: NodeId, msg: Message) -> crate::Result<()>;
    async fn recv(&mut self) -> Option<(NodeId, Message)>;
}
