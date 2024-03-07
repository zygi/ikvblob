use siphasher::sip::SipHasher;

use crate::fileformat_write::StaticSizeSerializable;

#[derive(Clone, Debug)]
pub struct SipHasherFactory {
    p: u64,
}

impl SipHasherFactory {
    pub fn new(param: u64) -> Self { SipHasherFactory { p: param } }

    // IMPORTANT: we want the hashes here to be platform-independent. Unfortunately, this means we
    // can't use the Hash trait. So we define a function on keys that are StaticSizeSerializable
    // and hash the bytes directly.
    pub fn hash<K: StaticSizeSerializable>(&self, key: &K) -> u64 {
        let hasher: SipHasher = SipHasher::new_with_keys(self.p, 0);
        hasher.hash(&key.to_bytes().unwrap())
    }
}
