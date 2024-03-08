use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use ciborium::cbor;
// use multihash::Multihash;
use std::{
    array,
    error::Error,
    io::{self, Write},
};

use crate::{cuckoo::StaticCuckooTable, multihash::Multihash, utils::CRC32Writer};

// TODO document
pub trait StaticSizeSerializable: Sized {
    fn write<W>(&self, write: &mut W) -> Result<(), io::Error>
    where
        W: io::Write;

    fn read<R>(read: &mut R) -> Result<Self, io::Error>
    where
        R: io::Read;
    const SER_SIZE: usize;

    // TODO make this output [u8; Self::SER_SIZE]
    fn to_bytes(&self) -> Result<Vec<u8>, io::Error> {
        let mut buf = Vec::new();
        self.write(&mut buf)?;
        Ok(buf)
    }
}

const FILE_FORMAT_VERSION: u64 = 1;

const MAGIC: &[u8; 8] = b"\0Ikvblob";
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct IkvblobHeader {
    pub version: u64,

    pub dynamic_metadata_offset: u64,
    pub dynamic_metadata_size: u64,

    pub cuckoo_table_offset: u64,
    pub cuckoo_table_size: u64,
    pub cuckoo_entry_size: u64,
    pub cuckoo_table_elems_per_bucket: u64,
    pub cuckoo_table_num_hashers: u64,

    pub value_blob_offset: u64,
    pub value_blob_size: u64,
}

impl IkvblobHeader {
    pub fn cuckoo_table_num_buckets(&self) -> usize {
        (self.cuckoo_table_size / (self.cuckoo_table_elems_per_bucket * self.cuckoo_entry_size))
            as usize
    }

    pub fn total_size(&self) -> usize {
        (self.value_blob_offset + self.value_blob_size + 4) as usize
    }

    pub fn invariant_check(&self) {
        let md_range = self.dynamic_metadata_offset
            ..(self.dynamic_metadata_offset + self.dynamic_metadata_size);
        let ct_range =
            self.cuckoo_table_offset..(self.cuckoo_table_offset + self.cuckoo_table_size);
        let vb_range = self.value_blob_offset..(self.value_blob_offset + self.value_blob_size);

        // assert that the ranges don't overlap
        for a in [&md_range, &ct_range, &vb_range].into_iter() {
            for b in [&md_range, &ct_range, &vb_range].into_iter() {
                if a != b {
                    assert!(
                        a.end <= b.start || b.end <= a.start,
                        "Invariant error: ranges overlap"
                    );
                }
            }
        }
    }
}

// TODO replace with serde or something
impl StaticSizeSerializable for IkvblobHeader {
    fn write<W>(&self, write: &mut W) -> Result<(), io::Error>
    where
        W: io::Write,
    {
        self.invariant_check();
        write.write_all(MAGIC)?;
        write.write_u64::<LittleEndian>(self.version)?;
        write.write_u64::<LittleEndian>(self.dynamic_metadata_offset as u64)?;
        write.write_u64::<LittleEndian>(self.dynamic_metadata_size as u64)?;

        write.write_u64::<LittleEndian>(self.cuckoo_table_offset as u64)?;
        write.write_u64::<LittleEndian>(self.cuckoo_table_size as u64)?;
        write.write_u64::<LittleEndian>(self.cuckoo_entry_size as u64)?;
        write.write_u64::<LittleEndian>(self.cuckoo_table_elems_per_bucket as u64)?;
        write.write_u64::<LittleEndian>(self.cuckoo_table_num_hashers as u64)?;

        write.write_u64::<LittleEndian>(self.value_blob_offset as u64)?;
        write.write_u64::<LittleEndian>(self.value_blob_size as u64)?;
        Ok(())
    }

    fn read<R>(read: &mut R) -> Result<Self, io::Error>
    where
        R: io::Read,
    {
        let mut magic = [0u8; 8];
        read.read_exact(&mut magic)?;
        if magic != *MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Invalid magic number in header",
            ));
        }
        let version = read.read_u64::<LittleEndian>()?;

        if version > FILE_FORMAT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Unsupported IkvBlob format version: file has version {}, library supports versions <= {}", version, FILE_FORMAT_VERSION),
            ));
        }

        let dynamic_metadata_offset = read.read_u64::<LittleEndian>()?;
        let dynamic_metadata_size = read.read_u64::<LittleEndian>()?;
        let cuckoo_table_offset = read.read_u64::<LittleEndian>()?;
        let cuckoo_table_size = read.read_u64::<LittleEndian>()?;
        let cuckoo_entry_size = read.read_u64::<LittleEndian>()?;
        let cuckoo_table_num_buckets = read.read_u64::<LittleEndian>()?;
        let cuckoo_table_num_hashers = read.read_u64::<LittleEndian>()?;
        let value_blob_offset = read.read_u64::<LittleEndian>()?;
        let value_blob_size = read.read_u64::<LittleEndian>()?;

        let res = IkvblobHeader {
            version,
            dynamic_metadata_offset,
            dynamic_metadata_size,
            cuckoo_table_offset,
            cuckoo_table_size,
            cuckoo_entry_size,
            cuckoo_table_elems_per_bucket: cuckoo_table_num_buckets,
            cuckoo_table_num_hashers,
            value_blob_offset,
            value_blob_size,
        };
        res.invariant_check();
        Ok(res)
    }

    const SER_SIZE: usize = MAGIC.len() + 10 * std::mem::size_of::<u64>();
}

type KEY = Multihash<32>;

impl<K: StaticSizeSerializable, const N: usize> StaticSizeSerializable for [K; N] {
    fn write<W>(&self, write: &mut W) -> io::Result<()>
    where
        W: io::Write,
    {
        for k in self.iter() {
            k.write(write)?;
        }
        Ok(())
    }

    fn read<R>(read: &mut R) -> io::Result<Self>
    where
        R: io::Read,
    {
        array::try_from_fn(|_| K::read(read)).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    const SER_SIZE: usize = K::SER_SIZE * N;
}

impl StaticSizeSerializable for u64 {
    fn write<W>(&self, write: &mut W) -> io::Result<()>
    where
        W: io::Write,
    {
        write.write_u64::<LittleEndian>(*self)
    }

    fn read<R>(read: &mut R) -> io::Result<Self>
    where
        R: io::Read,
    {
        read.read_u64::<LittleEndian>()
    }

    const SER_SIZE: usize = std::mem::size_of::<u64>();
}

impl StaticSizeSerializable for Multihash<32> {
    fn write<W>(&self, write: &mut W) -> io::Result<()>
    where
        W: io::Write,
    {
        write.write_all(self.digest())?;
        write.write_u8(self.code())
    }

    fn read<R>(read: &mut R) -> io::Result<Self>
    where
        R: io::Read,
    {
        // let key = Multihash::<32>::read(&mut *read)
        //     .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let digest = {
            let mut digest = [0u8; 32];
            read.read_exact(&mut digest)?;
            digest
        };
        let code = read.read_u8()?;
        Ok(Multihash::<32>::wrap(code, digest))
    }

    const SER_SIZE: usize = 32 + 1;
}

type IndexTableEntry = Option<(KEY, (u64, u64))>;

impl StaticSizeSerializable for IndexTableEntry {
    fn write<W>(&self, write: &mut W) -> io::Result<()>
    where
        W: io::Write,
    {
        match self {
            Some((key, (offset, size))) => {
                // key.write(&mut *write)
                //     .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                write.write_all(key.digest())?;

                let highest_byte_mask_u64 = 0xFF00000000000000;
                let highest_byte_inv_mask_u64 = 0x00FFFFFFFFFFFFFF;
                if offset & highest_byte_mask_u64 != 0 {
                    return Err(io::Error::new(io::ErrorKind::Other, "Offset too large"));
                }

                let packed = (offset & highest_byte_inv_mask_u64) | ((key.code() as u64) << 56);
                write.write_u64::<LittleEndian>(packed)?;
                write.write_u64::<LittleEndian>(*size)?;
            }
            None => {
                write.write_all(&[0u8; 32])?;
                write.write_u64::<LittleEndian>(0)?;
                write.write_u64::<LittleEndian>(0)?;
            }
        }
        Ok(())
    }

    fn read<R>(read: &mut R) -> io::Result<Self>
    where
        R: io::Read,
    {
        let digest = {
            let mut digest = [0u8; 32];
            read.read_exact(&mut digest)?;
            digest
        };

        let highest_byte_inv_mask_u64 = 0x00FFFFFFFFFFFFFF;
        let packed = read.read_u64::<LittleEndian>()?;
        let size = read.read_u64::<LittleEndian>()?;

        let hash_type = (packed >> 56) as u8;
        let offset = packed & highest_byte_inv_mask_u64;
        if hash_type == 0 {
            Ok(None)
        } else {
            Ok(Some((
                Multihash::<32>::wrap(hash_type, digest),
                (offset, size),
            )))
        }
    }

    const SER_SIZE: usize = 32 + 2 * std::mem::size_of::<u64>();
}

pub fn write_combined_file<const BS: usize, const HS: usize, K, V, R: io::Read, W: io::Write>(
    map_table: &StaticCuckooTable<BS, HS, K, V>,
    compression_dict: &[u8],
    mut result_read: R,
    result_byte_len: usize,
    mut base_desination: W,
) -> Result<(), Box<dyn Error>>
where
    K: Eq + Copy + StaticSizeSerializable,
    Option<(K, V)>: StaticSizeSerializable,
{
    let mut dest = CRC32Writer::new(&mut base_desination);

    let cuckoo_entry_size = Option::<(K, V)>::SER_SIZE as u64;
    let cuckoo_table_size = (map_table.table.len() * Option::<(K, V)>::SER_SIZE * BS) as u64;

    let md = if compression_dict.len() > 0 {
        cbor!({
            "compression_type" => "zstd",
            "compression_dict" => ciborium::Value::Bytes(compression_dict.to_vec())
        })
    } else {
        cbor!({})
    };

    let mut md_bytes = Vec::<u8>::new();
    ciborium::into_writer(&md?, &mut md_bytes)?;
    let md_size = md_bytes.len() as u64;
    let aligned_md_size = (md_bytes.len() + 7) & !7;
    md_bytes.resize(aligned_md_size, 0);

    let header = IkvblobHeader {
        version: FILE_FORMAT_VERSION,
        dynamic_metadata_offset: IkvblobHeader::SER_SIZE as u64,
        dynamic_metadata_size: md_size as u64,
        cuckoo_table_offset: IkvblobHeader::SER_SIZE as u64 + aligned_md_size as u64,
        cuckoo_table_size,
        cuckoo_entry_size,
        cuckoo_table_elems_per_bucket: BS as u64,
        cuckoo_table_num_hashers: HS as u64,
        value_blob_offset: IkvblobHeader::SER_SIZE as u64
            + aligned_md_size as u64
            + cuckoo_table_size as u64,
        value_blob_size: result_byte_len as u64,
    };

    header.write(&mut dest)?;
    dest.write_all(&md_bytes)?;
    for bucket in map_table.table.iter() {
        bucket.write(&mut dest)?;
    }

    io::copy(&mut result_read, &mut dest)?;

    // Finally, write checksum
    let cs = dest.current_crc();
    dest.write_u32::<LittleEndian>(cs)?;

    Ok(())
}

// Tests
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_header_ser_deser() {
        let header = IkvblobHeader {
            version: 1,
            dynamic_metadata_offset: 2,
            dynamic_metadata_size: 3,
            cuckoo_table_offset: 4,
            cuckoo_table_size: 5,
            cuckoo_entry_size: 6,
            cuckoo_table_elems_per_bucket: 7,
            cuckoo_table_num_hashers: 8,
            value_blob_offset: 9,
            value_blob_size: 10,
        };

        let mut buf = Vec::new();
        header.write(&mut buf).unwrap();
        let header2 = IkvblobHeader::read(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(header, header2);
    }

    #[test]
    fn test_index_ser_deser() {
        let index1 = Some((Multihash::<32>::wrap(1, [2; 32]), (3, 4)));
        let mut buf = Vec::new();
        index1.write(&mut buf).unwrap();
        let index2 = IndexTableEntry::read(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(index1, index2);
    }
}
