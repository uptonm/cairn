use crate::types::{LogEntry, LogIndex, NodeId, Term};
use serde_derive::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RequestVoteReq {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
    pub pre_vote: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RequestVoteResp {
    pub term: Term,
    pub vote_granted: bool,
    /// Discriminates a pre-vote grant from a real-vote grant. Term alone
    /// cannot: a pre-vote responder echoes ITS OWN current_term, which can
    /// coincide with a candidate's real-election term (e.g. a peer already
    /// at T+1 grants a pre-vote carrying term T+1, wire-identical to a real
    /// vote at that term). `handle_vote_resp` routes strictly by this flag
    /// per role rather than by term, so a stray pre-vote grant can never be
    /// miscounted as a real vote (or vice versa).
    pub pre_vote: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AppendEntriesReq {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AppendEntriesResp {
    pub term: Term,
    pub success: bool,
    pub conflict_index: Option<LogIndex>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InstallSnapshotReq {
    pub term: Term,
    pub leader_id: NodeId,
    pub last_index: LogIndex,
    pub last_term: Term,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InstallSnapshotResp {
    pub term: Term,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Message {
    RequestVote(RequestVoteReq),
    RequestVoteResp(RequestVoteResp),
    AppendEntries(AppendEntriesReq),
    AppendEntriesResp(AppendEntriesResp),
    InstallSnapshot(InstallSnapshotReq),
    InstallSnapshotResp(InstallSnapshotResp),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LogEntry;

    #[test]
    fn all_message_variants_roundtrip_with_bincode() {
        let messages = vec![
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
        ];

        for message in messages {
            let encoded = bincode::serialize(&message).unwrap();
            let decoded = bincode::deserialize::<Message>(&encoded).unwrap();
            assert_eq!(decoded, message);
        }
    }
}
