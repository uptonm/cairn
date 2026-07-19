use serde_derive::{Deserialize, Serialize};

pub type NodeId = u64;
pub type Term = u64;
pub type LogIndex = u64;

/// What a `LogEntry` represents: a normal state-machine command, or a
/// cluster membership change. Distinguishing the two lets the apply layer
/// route entries without inspecting `command` bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub enum EntryType {
    #[default]
    Normal,
    ConfigChange,
}

impl EntryType {
    /// The 1-byte wire tag used by both hand-rolled binary codecs
    /// (`oplog.rs`, `transport/tcp/codec.rs`). Kept in one place so the two
    /// crash-safety-critical encoders can't drift on the mapping.
    pub(crate) fn to_byte(self) -> u8 {
        match self {
            EntryType::Normal => 0,
            EntryType::ConfigChange => 1,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub command: Vec<u8>,
    pub entry_type: EntryType,
}

impl LogEntry {
    pub fn normal(term: Term, index: LogIndex, command: Vec<u8>) -> Self {
        Self {
            term,
            index,
            command,
            entry_type: EntryType::Normal,
        }
    }

    pub fn config_change(term: Term, index: LogIndex, command: Vec<u8>) -> Self {
        Self {
            term,
            index,
            command,
            entry_type: EntryType::ConfigChange,
        }
    }
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
        let e = LogEntry::normal(2, 5, b"set x".to_vec());
        assert_eq!(e.term, 2);
        assert_eq!(e.index, 5);
        assert_eq!(e.command, b"set x");
        assert_eq!(e.entry_type, EntryType::Normal);
    }

    #[test]
    fn entry_type_default_is_normal() {
        assert_eq!(EntryType::default(), EntryType::Normal);
    }

    #[test]
    fn constructors_set_the_matching_entry_type() {
        let normal = LogEntry::normal(1, 1, vec![]);
        let config_change = LogEntry::config_change(1, 2, vec![]);
        assert_eq!(normal.entry_type, EntryType::Normal);
        assert_eq!(config_change.entry_type, EntryType::ConfigChange);
    }
}
