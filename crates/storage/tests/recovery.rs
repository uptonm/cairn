use cairn_storage::Engine;
use tempfile::tempdir;

#[test]
fn unflushed_writes_survive_reopen() {
    let dir = tempdir().unwrap();
    {
        let mut e = Engine::open(dir.path()).unwrap();
        e.put(b"durable", b"yes").unwrap();
        // no flush: durability must come from the WAL alone
    }
    let e = Engine::open(dir.path()).unwrap();
    assert_eq!(e.get(b"durable").unwrap(), Some(b"yes".to_vec()));
}

#[test]
fn mixed_flushed_and_wal_writes_survive_reopen() {
    let dir = tempdir().unwrap();
    {
        let mut e = Engine::open(dir.path()).unwrap();
        e.put(b"a", b"1").unwrap();
        e.flush().unwrap();
        e.put(b"b", b"2").unwrap(); // WAL only
    }
    let e = Engine::open(dir.path()).unwrap();
    assert_eq!(e.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(e.get(b"b").unwrap(), Some(b"2".to_vec()));
}
