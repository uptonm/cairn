use serde_derive::{Deserialize, Serialize};

pub type NodeId = u64;
pub type Term = u64;
pub type LogIndex = u64;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub command: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct HardState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct SnapshotMeta {
    pub last_index: LogIndex,
    pub last_term: Term,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hard_state_default_is_term0_no_vote() {
        let hs = HardState::default();
        assert_eq!(hs.current_term, 0);
        assert_eq!(hs.voted_for, None);
    }

    #[test]
    fn log_entry_holds_command_bytes() {
        let e = LogEntry {
            term: 2,
            index: 5,
            command: b"set x".to_vec(),
        };
        assert_eq!(e.term, 2);
        assert_eq!(e.index, 5);
        assert_eq!(e.command, b"set x");
    }
}
