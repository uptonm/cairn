use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Op {
    Put { key: Vec<u8>, val: Vec<u8> },
    Get { key: Vec<u8> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Ret {
    PutOk,
    GetVal(Option<Vec<u8>>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Event {
    pub process: u64,
    pub op: Op,
    pub ret: Ret,
    pub invoke: u64,
    pub complete: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Report {
    pub linearizable: bool,
    pub witness: Option<String>,
}

struct ProjectedEvent<'a> {
    history_index: usize,
    event: &'a Event,
}

struct Failure {
    depth: usize,
    history_index: usize,
    reason: String,
}

pub fn check(history: &[Event]) -> Report {
    let mut by_key = BTreeMap::<Vec<u8>, Vec<ProjectedEvent<'_>>>::new();

    for (history_index, event) in history.iter().enumerate() {
        let key = operation_key(&event.op);
        if event.invoke >= event.complete {
            return Report {
                linearizable: false,
                witness: Some(format!(
                    "key {} is not linearizable: operation #{} ({}) has invalid interval \
                     invoke={} complete={}",
                    format_bytes(key),
                    history_index,
                    describe_operation(event),
                    event.invoke,
                    event.complete
                )),
            };
        }

        by_key
            .entry(key.to_vec())
            .or_default()
            .push(ProjectedEvent {
                history_index,
                event,
            });
    }

    for (key, events) in by_key {
        let mut linearized = vec![false; events.len()];
        if let Err(failure) = search(&events, &mut linearized, &None, 0) {
            let event = history
                .get(failure.history_index)
                .expect("projected events originate in history");
            return Report {
                linearizable: false,
                witness: Some(format!(
                    "key {} is not linearizable: operation #{} ({}) reached a contradiction: {}",
                    format_bytes(&key),
                    failure.history_index,
                    describe_operation(event),
                    failure.reason
                )),
            };
        }
    }

    Report {
        linearizable: true,
        witness: None,
    }
}

fn operation_key(op: &Op) -> &[u8] {
    match op {
        Op::Put { key, .. } | Op::Get { key } => key,
    }
}

fn search(
    events: &[ProjectedEvent<'_>],
    linearized: &mut [bool],
    state: &Option<Vec<u8>>,
    depth: usize,
) -> Result<(), Failure> {
    if depth == events.len() {
        return Ok(());
    }

    let frontier = events
        .iter()
        .enumerate()
        .filter(|(index, _)| !linearized[*index])
        .map(|(_, projected)| projected.event.complete)
        .min()
        .expect("unfinished search has remaining events");
    let mut best_failure = None;

    for (index, projected) in events.iter().enumerate() {
        if linearized[index] || projected.event.invoke > frontier {
            continue;
        }

        match apply(projected.event, state) {
            Ok(next_state) => {
                linearized[index] = true;
                let result = search(events, linearized, &next_state, depth + 1);
                linearized[index] = false;

                match result {
                    Ok(()) => return Ok(()),
                    Err(failure) => record_deepest(&mut best_failure, failure),
                }
            }
            Err(reason) => record_deepest(
                &mut best_failure,
                Failure {
                    depth,
                    history_index: projected.history_index,
                    reason,
                },
            ),
        }
    }

    Err(best_failure.unwrap_or_else(|| {
        let projected = events
            .iter()
            .enumerate()
            .filter(|(index, _)| !linearized[*index])
            .min_by_key(|(_, projected)| projected.event.complete)
            .map(|(_, projected)| projected)
            .expect("unfinished search has remaining events");
        Failure {
            depth,
            history_index: projected.history_index,
            reason: format!("no operation is eligible at frontier {frontier}"),
        }
    }))
}

fn record_deepest(best: &mut Option<Failure>, candidate: Failure) {
    if best
        .as_ref()
        .is_none_or(|current| candidate.depth > current.depth)
    {
        *best = Some(candidate);
    }
}

fn apply(event: &Event, state: &Option<Vec<u8>>) -> Result<Option<Vec<u8>>, String> {
    match (&event.op, &event.ret) {
        (Op::Put { val, .. }, Ret::PutOk) => Ok(Some(val.clone())),
        (Op::Put { .. }, Ret::GetVal(value)) => Err(format!(
            "Put returned {}, expected PutOk",
            format_value(value)
        )),
        (Op::Get { .. }, Ret::PutOk) => Err("Get returned PutOk, expected GetVal".to_owned()),
        (Op::Get { .. }, Ret::GetVal(value)) if value == state => Ok(state.clone()),
        (Op::Get { .. }, Ret::GetVal(value)) => Err(format!(
            "Get returned {}, but the register contains {}",
            format_value(value),
            format_value(state)
        )),
    }
}

fn describe_operation(event: &Event) -> String {
    match (&event.op, &event.ret) {
        (Op::Put { val, .. }, ret) => format!(
            "process {} Put({}) -> {ret:?}, invoke={}, complete={}",
            event.process,
            format_bytes(val),
            event.invoke,
            event.complete
        ),
        (Op::Get { .. }, ret) => format!(
            "process {} Get -> {ret:?}, invoke={}, complete={}",
            event.process, event.invoke, event.complete
        ),
    }
}

fn format_value(value: &Option<Vec<u8>>) -> String {
    match value {
        Some(bytes) => format!("Some({})", format_bytes(bytes)),
        None => "None".to_owned(),
    }
}

fn format_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut formatted = String::with_capacity(2 + bytes.len() * 2);
    formatted.push_str("0x");
    for byte in bytes {
        formatted.push(char::from(HEX[usize::from(byte >> 4)]));
        formatted.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    formatted
}

#[cfg(test)]
mod tests {
    use super::{check, Event, Op, Ret};
    use proptest::prelude::*;

    fn put(process: u64, key: &str, val: &str, invoke: u64, complete: u64) -> Event {
        Event {
            process,
            op: Op::Put {
                key: key.as_bytes().to_vec(),
                val: val.as_bytes().to_vec(),
            },
            ret: Ret::PutOk,
            invoke,
            complete,
        }
    }

    fn get(process: u64, key: &str, val: Option<&str>, invoke: u64, complete: u64) -> Event {
        Event {
            process,
            op: Op::Get {
                key: key.as_bytes().to_vec(),
            },
            ret: Ret::GetVal(val.map(|value| value.as_bytes().to_vec())),
            invoke,
            complete,
        }
    }

    #[test]
    fn simple_sequential_history_is_linearizable() {
        let history = vec![
            get(1, "key", None, 1, 2),
            put(1, "key", "value", 3, 4),
            get(1, "key", Some("value"), 5, 6),
        ];

        let report = check(&history);

        assert!(report.linearizable);
        assert!(report.witness.is_none());
    }

    #[test]
    fn stale_read_without_a_valid_linearization_is_rejected() {
        let history = vec![
            put(1, "key", "old", 1, 2),
            put(1, "key", "new", 3, 4),
            get(1, "key", Some("old"), 5, 6),
        ];

        let report = check(&history);

        assert!(!report.linearizable);
        let witness = report.witness.expect("failure should include a witness");
        assert!(witness.contains("key 0x6b6579"));
        assert!(witness.contains("Get"));
    }

    #[test]
    fn overlapping_puts_allow_either_value_for_a_later_get() {
        for expected in ["first", "second"] {
            let history = vec![
                put(1, "key", "first", 1, 5),
                put(2, "key", "second", 2, 6),
                get(3, "key", Some(expected), 7, 8),
            ];

            assert!(
                check(&history).linearizable,
                "later Get returning {expected:?} should be linearizable"
            );
        }
    }

    #[test]
    fn value_cannot_resurrect_after_a_linearized_overwrite() {
        let history = vec![
            put(1, "key", "old", 1, 2),
            put(2, "key", "new", 3, 8),
            get(3, "key", Some("new"), 4, 5),
            get(4, "key", Some("old"), 9, 10),
        ];

        let report = check(&history);

        assert!(!report.linearizable);
        assert!(report.witness.is_some());
    }

    #[test]
    fn defect_on_one_key_is_caught_when_other_keys_are_valid() {
        let history = vec![
            put(1, "good", "one", 1, 2),
            get(1, "good", Some("one"), 3, 4),
            put(2, "bad", "old", 5, 6),
            put(2, "bad", "new", 7, 8),
            get(2, "bad", Some("old"), 9, 10),
            get(1, "good", Some("one"), 11, 12),
        ];

        let report = check(&history);

        assert!(!report.linearizable);
        let witness = report.witness.expect("failure should include a witness");
        assert!(witness.contains("key 0x626164"));
    }

    #[test]
    fn moderately_concurrent_generated_history_is_linearizable() {
        let mut history = Vec::new();
        let mut invocation = 1;

        for round in 0..16_u8 {
            for key_id in 0..5_u8 {
                let key = vec![key_id];
                let val = vec![round, key_id];

                history.push(Event {
                    process: invocation,
                    op: Op::Put {
                        key: key.clone(),
                        val: val.clone(),
                    },
                    ret: Ret::PutOk,
                    invoke: invocation,
                    complete: invocation + 10_000,
                });
                invocation += 1;
                history.push(Event {
                    process: invocation,
                    op: Op::Get { key },
                    ret: Ret::GetVal(Some(val)),
                    invoke: invocation,
                    complete: invocation + 10_000,
                });
                invocation += 1;
            }
        }

        assert!(check(&history).linearizable);
    }

    proptest! {
        #[test]
        fn generated_sequential_histories_are_linearizable(
            values in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..8), 0..24),
        ) {
            let key = b"property-key".to_vec();
            let mut history = Vec::new();

            for (index, value) in values.into_iter().enumerate() {
                let invoke = index as u64 * 4 + 1;
                history.push(Event {
                    process: 1,
                    op: Op::Put {
                        key: key.clone(),
                        val: value.clone(),
                    },
                    ret: Ret::PutOk,
                    invoke,
                    complete: invoke + 1,
                });
                history.push(Event {
                    process: 1,
                    op: Op::Get { key: key.clone() },
                    ret: Ret::GetVal(Some(value)),
                    invoke: invoke + 2,
                    complete: invoke + 3,
                });
            }

            prop_assert!(check(&history).linearizable);
        }
    }
}
