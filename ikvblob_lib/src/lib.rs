#![feature(array_chunks)]
#![feature(iter_array_chunks)]
#![feature(array_try_from_fn)]

pub mod utils;
pub mod construction;
pub mod cuckoo;
pub mod parametrized_hasher;
pub mod index;
pub mod fileformat_write;
pub mod fileformat_read;
pub mod memory_view;
pub mod multihash;