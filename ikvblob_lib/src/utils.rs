use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufRead, Read},
    sync::mpsc,
};

use base64::prelude::*;
use log::info;
use rand::Rng;
use zstd::Decoder;

pub fn parsed_signature_tuple_iterator(
    filename: &str,
) -> Result<
    std::iter::Map<
        std::io::Lines<
            std::io::BufReader<Decoder<'_, std::io::BufReader<std::io::BufReader<File>>>>,
        >,
        impl FnMut(Result<String, std::io::Error>) -> (Vec<u8>, Vec<u8>),
    >,
    std::io::Error,
> {
    let file = File::open(filename)?;
    let decoder = Decoder::new(std::io::BufReader::new(file))?;
    let bufreader = std::io::BufReader::new(decoder).lines();

    Ok(bufreader.map(|l| {
        let line = l.unwrap();
        let parts = line.split(',').collect::<Vec<&str>>();
        let key = hex::decode(parts[0]).unwrap();
        let value = BASE64_STANDARD.decode(parts[1].as_bytes()).unwrap();
        (key, value)
    }))
}

const SZ: usize = std::mem::size_of::<u64>() + std::mem::size_of::<u32>();
pub fn index_file_iterator(
    filename: &str,
) -> Result<impl Iterator<Item = (u64, u32)>, Box<(dyn std::error::Error)>> {
    let mut bufreader = std::io::BufReader::new(File::open(filename)?);

    Ok(std::iter::from_fn(move || {
        let mut buf = [0u8; SZ];
        match bufreader.read_exact(&mut buf) {
            Ok(()) => {
                let a = u64::from_le_bytes(buf[0..8].try_into().unwrap());
                let b = u32::from_le_bytes(buf[8..12].try_into().unwrap());
                Some((a, b))
            }
            Err(_) => None,
        }
    }))
}


pub struct CRC32Writer<W: io::Write> {
    inner: W,
    crc: crc32fast::Hasher,
}

impl <W: io::Write> CRC32Writer<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            crc: crc32fast::Hasher::new(),
        }
    }

    pub fn current_crc(&mut self) -> u32 {
        self.crc.clone().finalize()
    }
}

impl <W: io::Write> io::Write for CRC32Writer<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.crc.update(buf);
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

// The following two function are from https://github.com/npryce/reservoir-rs . License: Apache 2.0 / MIT
pub fn sample<I, RNG>(rng: &mut RNG, sample_size: usize, iter: I) -> Vec<I::Item>
where
    I: Iterator,
    RNG: Rng,
{
    let mut samples = Vec::<I::Item>::new();
    sample_into(&mut samples, rng, sample_size, iter);
    samples
}

pub fn sample_into<I, RNG>(samples: &mut Vec<I::Item>, rng: &mut RNG, sample_size: usize, iter: I)
where
    I: Iterator,
    RNG: Rng,
{
    let original_length = samples.len();
    let mut count: usize = 0;
    for element in iter {
        count += 1;

        if count <= sample_size {
            samples.push(element);
        } else {
            let index = rng.gen_range(0..count);
            if index < sample_size {
                samples[original_length + index] = element;
            }
        }
    }
}


pub fn recover_order<T>(rx: mpsc::Receiver<(usize, T)>, mut op: impl FnMut(T)) {
    let mut next_index: usize = 0;
    let mut buffer: HashMap<usize, T> = HashMap::new();
    for (i, value) in rx {
        if i == next_index {
            op(value);
            next_index += 1;
            while let Some((_, value)) = buffer.remove_entry(&next_index) {
                op(value);
                next_index += 1;
            }
        } else {
            // Item is out of order
            buffer.insert(i, value);
        }
    }

    assert!(buffer.is_empty(), "channel closed with missing items");

    info!("Buffer capacity used: {}", buffer.capacity());
}
