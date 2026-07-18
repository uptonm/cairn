use cairn_storage::Engine;
use proptest::prelude::*;
use std::collections::BTreeMap;
use tempfile::tempdir;

#[derive(Debug, Clone)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Get(Vec<u8>),
    Flush,
    Compact,
}

fn small_key() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(0u8..4, 1..3)
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (small_key(), proptest::collection::vec(any::<u8>(), 0..4))
            .prop_map(|(k, v)| Op::Put(k, v)),
        small_key().prop_map(Op::Delete),
        small_key().prop_map(Op::Get),
        Just(Op::Flush),
        Just(Op::Compact),
    ]
}

proptest! {
    #[test]
    fn engine_matches_btreemap(ops in proptest::collection::vec(op_strategy(), 1..200)) {
        let dir = tempdir().unwrap();
        let mut engine = Engine::open(dir.path()).unwrap();
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        for op in ops {
            match op {
                Op::Put(k, v) => { engine.put(&k, &v).unwrap(); model.insert(k, v); }
                Op::Delete(k) => { engine.delete(&k).unwrap(); model.remove(&k); }
                Op::Get(k) => {
                    prop_assert_eq!(engine.get(&k).unwrap(), model.get(&k).cloned());
                }
                Op::Flush => engine.flush().unwrap(),
                Op::Compact => engine.compact().unwrap(),
            }
        }
    }
}
