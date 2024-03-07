// Sequential for now, it shouldn't be too hard to extend it
// later by requiring a "merge" method
pub trait IndexBuilder<I, V> {
    fn push(&mut self, entry: &V) -> I;
}

pub struct EntryIndexBuilder {
    current_index: usize,
}

impl EntryIndexBuilder {
    pub fn new() -> Self { EntryIndexBuilder { current_index: 0 } }
}

impl<V> IndexBuilder<usize, V> for EntryIndexBuilder {
    fn push(&mut self, _entry: &V) -> usize {
        let idx = self.current_index;
        self.current_index += 1;
        idx
    }
}

pub struct OffsetIndexBuilder {
    current_offset: u64,
}

impl OffsetIndexBuilder {
    pub fn new() -> Self { OffsetIndexBuilder { current_offset: 0 } }
}

impl<V> IndexBuilder<(u64, u32), V> for OffsetIndexBuilder
where
    V: AsRef<[u8]>,
{
    fn push(&mut self, _entry: &V) -> (u64, u32) {
        let v_bytes = _entry.as_ref();
        let len = v_bytes.len() as u32;
        let res = (self.current_offset, len);
        self.current_offset += len as u64;
        res
    }
}
