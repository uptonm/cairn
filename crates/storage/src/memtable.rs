use crate::types::InternalKey;
use std::collections::BTreeMap;

pub struct Memtable {
    map: BTreeMap<InternalKey, Option<Vec<u8>>>,
    size: usize,
}

impl Memtable {
    pub fn new() -> Self {
        Memtable {
            map: BTreeMap::new(),
            size: 0,
        }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Option<Vec<u8>>, seqno: u64) {
        self.size += key.len() + value.as_ref().map_or(0, |v| v.len()) + 16;
        self.map.insert(
            InternalKey {
                user_key: key,
                seqno,
            },
            value,
        );
    }

    pub fn get(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        // Newest version sorts first for a given user_key.
        self.map
            .range(
                InternalKey {
                    user_key: key.to_vec(),
                    seqno: u64::MAX,
                }..,
            )
            .next()
            .filter(|(ik, _)| ik.user_key == key)
            .map(|(_, v)| v.clone())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&InternalKey, &Option<Vec<u8>>)> {
        self.map.iter()
    }

    pub fn approx_size_bytes(&self) -> usize {
        self.size
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_returns_newest_version() {
        let mut m = Memtable::new();
        m.put(b"k".to_vec(), Some(b"v1".to_vec()), 1);
        m.put(b"k".to_vec(), Some(b"v2".to_vec()), 2);
        assert_eq!(m.get(b"k"), Some(Some(b"v2".to_vec())));
    }

    #[test]
    fn tombstone_shadows_older_value() {
        let mut m = Memtable::new();
        m.put(b"k".to_vec(), Some(b"v".to_vec()), 1);
        m.put(b"k".to_vec(), None, 2);
        assert_eq!(m.get(b"k"), Some(None));
    }

    #[test]
    fn absent_key_returns_none() {
        let m = Memtable::new();
        assert_eq!(m.get(b"missing"), None);
    }

    #[test]
    fn iter_is_sorted_newest_first_within_key() {
        let mut m = Memtable::new();
        m.put(b"b".to_vec(), Some(b"x".to_vec()), 1);
        m.put(b"a".to_vec(), Some(b"y".to_vec()), 2);
        m.put(b"a".to_vec(), Some(b"z".to_vec()), 5);
        let keys: Vec<_> = m
            .iter()
            .map(|(k, _)| (k.user_key.clone(), k.seqno))
            .collect();
        assert_eq!(
            keys,
            vec![(b"a".to_vec(), 5), (b"a".to_vec(), 2), (b"b".to_vec(), 1),]
        );
    }
}
