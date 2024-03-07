//! Multihash type
//!
//! We intend to use these multihashes as keys, and so we want them to be serializable to a statically sized byte array.
//! IPFS's multihash unfortunately uses varints both for the length and the code, and won't work for us here.
//! So we define our own type.

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub struct Multihash<const N: usize> {
    code: u8,
    digest: [u8; N],
}

impl<const N: usize> Multihash<N> {
    pub fn wrap(code: u8, digest: [u8; N]) -> Multihash<N> { Multihash { code, digest } }
    pub fn code(&self) -> u8 { self.code }
    pub fn digest(&self) -> &[u8; N] { &self.digest }
}

// Implement Hash manually for clarity
impl<const N: usize> std::hash::Hash for Multihash<N> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.code.hash(state);
        self.digest.hash(state);
    }
}
