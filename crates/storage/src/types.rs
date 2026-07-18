pub type Seqno = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InternalKey {
    pub user_key: Vec<u8>,
    pub seqno: Seqno,
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.user_key
            .cmp(&other.user_key)
            .then(other.seqno.cmp(&self.seqno)) // newer seqno sorts first
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newer_version_of_same_key_sorts_first() {
        let older = InternalKey {
            user_key: b"a".to_vec(),
            seqno: 1,
        };
        let newer = InternalKey {
            user_key: b"a".to_vec(),
            seqno: 2,
        };
        assert!(newer < older);
    }

    #[test]
    fn different_keys_sort_by_user_key() {
        let a = InternalKey {
            user_key: b"a".to_vec(),
            seqno: 9,
        };
        let b = InternalKey {
            user_key: b"b".to_vec(),
            seqno: 1,
        };
        assert!(a < b);
    }
}
