use std::ops::Range;
// use maybe_async::maybe_async;
use memmap2::Mmap;

// #[maybe_async::maybe_async(AFIT)]
pub trait Memory {
    // TODO add error handling
    fn read_slice(&self, range: Range<usize>) -> impl std::future::Future<Output = Vec<u8>>;
    fn len(&self) -> impl std::future::Future<Output = usize>;
}

// #[sync_impl]
impl Memory for Vec<u8> {
    async fn read_slice(&self, range: Range<usize>) -> Vec<u8> { (&self[range]).to_vec() }
    async fn len(&self) -> usize { (*self).len() }
}

pub struct MmapMemory {
    pub mmap: Mmap,
}

// #[sync_impl]
impl Memory for MmapMemory {
    async fn read_slice(&self, range: Range<usize>) -> Vec<u8> { (&self.mmap[range]).to_vec() }
    async fn len(&self) -> usize { self.mmap.len() }
}
