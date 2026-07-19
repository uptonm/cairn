use cairn_raft::{LogEntry, RaftLog, SnapshotMeta};
use proptest::prelude::*;
use tempfile::tempdir;

#[derive(Debug, Clone)]
enum Op {
    Append,
    Truncate(u64),
    Compact(u64),
    Reopen,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        Just(Op::Append),
        (0u64..8).prop_map(Op::Truncate),
        (0u64..8).prop_map(Op::Compact),
        Just(Op::Reopen),
    ]
}

proptest! {
    #[test]
    fn raftlog_matches_reference(ops in proptest::collection::vec(op_strategy(), 1..120)) {
        let dir = tempdir().unwrap();
        let mut log = RaftLog::open(dir.path()).unwrap();
        let mut model: Vec<LogEntry> = Vec::new();
        let mut snap = SnapshotMeta::default();
        let mut next_term = 1u64;

        for op in ops {
            match op {
                Op::Append => {
                    let index = model.last().map_or(snap.last_index, |e| e.index) + 1;
                    let entry = LogEntry::normal(next_term, index, vec![index as u8]);
                    log.append(std::slice::from_ref(&entry)).unwrap();
                    model.push(entry);
                    next_term += 1;
                }
                Op::Truncate(rel) => {
                    let from = snap.last_index + 1 + rel;
                    let last = model.last().map_or(snap.last_index, |e| e.index);
                    if from > snap.last_index && from <= last {
                        log.truncate_suffix(from).unwrap();
                        model.retain(|e| e.index < from);
                    }
                }
                Op::Compact(rel) => {
                    let up_to = snap.last_index + rel;
                    let last = model.last().map_or(snap.last_index, |e| e.index);
                    if up_to > snap.last_index && up_to <= last {
                        let term = model.iter().find(|e| e.index == up_to).map(|e| e.term).unwrap_or(snap.last_term);
                        let meta = SnapshotMeta { last_index: up_to, last_term: term };
                        log.compact_prefix(up_to, meta).unwrap();
                        model.retain(|e| e.index > up_to);
                        snap = meta;
                    }
                }
                Op::Reopen => {
                    drop(log);
                    log = RaftLog::open(dir.path()).unwrap();
                }
            }

            let model_last = model.last().map_or(snap.last_index, |e| e.index);
            let model_last_term = model.last().map_or(snap.last_term, |e| e.term);
            prop_assert_eq!(log.last_index(), model_last);
            prop_assert_eq!(log.last_term(), model_last_term);
            prop_assert_eq!(log.snapshot_meta(), snap);
            for e in &model {
                prop_assert_eq!(log.entry(e.index), Some(e));
            }
        }
    }
}
