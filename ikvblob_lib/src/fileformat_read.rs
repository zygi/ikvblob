use std::{collections::HashMap, error, hash::Hash};

use ciborium::Value;

use crate::{
    fileformat_write::{IkvblobHeader, StaticSizeSerializable},
    memory_view::Memory,
    parametrized_hasher::SipHasherFactory,
};

pub struct IkvblobView<'a, M: Memory, K, Idx>
where
    K: Hash + Copy + Eq,
    Option<(K, Idx)>: StaticSizeSerializable,
{
    pub header: IkvblobHeader,
    source_memory: M,
    compression_dict: Option<Box<zstd::dict::DecoderDictionary<'a>>>,
    hashers: Vec<SipHasherFactory>,
    phantom_key: std::marker::PhantomData<K>,
    phantom_idx: std::marker::PhantomData<Idx>,
    phantom_lifetime: std::marker::PhantomData<&'a ()>,
}

// DecoderDictionary doesn't implement Debug so we implement it manually
impl<'a, M: Memory + std::fmt::Debug, K, Idx> std::fmt::Debug for IkvblobView<'a, M, K, Idx>
where
    K: Hash + Copy + Eq,
    Option<(K, Idx)>: StaticSizeSerializable,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IkvblobView")
            .field("header", &self.header)
            // .field("source_memory", &self.source_memory)
            // .field(
            //     "compression_dict",
            //     &self.compression_dict.as_ref().map(|_| ()),
            // )
            .field("hashers", &self.hashers)
            .finish()
    }
}

#[derive(Debug)]
pub enum IkvblobError {
    Other(String),
}
impl std::fmt::Display for IkvblobError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            IkvblobError::Other(e) => write!(f, "IkvblobError: {}", e),
        }
    }
}
impl std::error::Error for IkvblobError {}

impl<'a, M: Memory, K, Idx> IkvblobView<'a, M, K, Idx>
where
    K: Hash + Copy + Eq,
    Option<(K, Idx)>: StaticSizeSerializable,
    K: StaticSizeSerializable,
{
    // #[maybe_async::maybe_async]
    pub async fn wrap(source_memory: M) -> Result<Self, Box<dyn error::Error>> {
        let len = source_memory.len().await;
        assert!(len >= IkvblobHeader::SER_SIZE);

        let header_bytes = source_memory.read_slice(0..IkvblobHeader::SER_SIZE).await;
        let header = IkvblobHeader::read(&mut &header_bytes[..]).unwrap();

        if header.cuckoo_entry_size != Option::<(K, Idx)>::SER_SIZE as u64 {
            return Err(Box::new(IkvblobError::Other(format!(
                "Cuckoo entry size mismatch: expected {}, got {}",
                Option::<(K, Idx)>::SER_SIZE,
                header.cuckoo_entry_size
            ))));
        }
        assert_eq!(header.total_size(), len);

        let hashers = (0..header.cuckoo_table_num_hashers)
            .map(|i| SipHasherFactory::new(i))
            .collect();

        let md = source_memory
            .read_slice(
                header.dynamic_metadata_offset as usize
                    ..(header.dynamic_metadata_offset + header.dynamic_metadata_size) as usize,
            )
            .await;
        let md_cbor = ciborium::from_reader::<ciborium::Value, _>(&md[..])?;
        if !md_cbor.is_map() {
            return Err(Box::new(IkvblobError::Other(
                "Metadata is not a map".to_string(),
            )));
        }
        let md_map: HashMap<String, &Value> = md_cbor
            .as_map()
            .unwrap()
            .into_iter()
            .map(|(k, v)| {
                if k.is_text() {
                    Ok((k.as_text().unwrap().to_string(), v))
                } else {
                    Err(Box::new(IkvblobError::Other(
                        "Metadata key is not a string".to_string(),
                    )))
                }
            })
            .collect::<Result<Vec<(String, &Value)>, _>>()?
            .into_iter()
            .collect();

        // let md_bson = bson::Document::from_reader(&md[..])?;

        // "compression_type": "zstd",
        // "compression_dict": bson::Binary {subtype: bson::spec::BinarySubtype::Generic, bytes: compression_dict.to_vec() },
        // let compression_dict = ;

        Self::new_with_compression_dict(header, source_memory, hashers, md_map)
    }

    fn new_with_compression_dict(
        header: IkvblobHeader,
        source_memory: M,
        hashers: Vec<SipHasherFactory>,
        md_map: HashMap<String, &Value>,
    ) -> Result<Self, Box<dyn error::Error>> {
        let compression_dict =
            if md_map.contains_key("compression_type") && md_map.contains_key("compression_dict") {
                let compression_type =
                    md_map["compression_type"]
                        .as_text()
                        .ok_or(Box::new(IkvblobError::Other(
                            "compression_type is not a string".to_string(),
                        )))?;
                if compression_type != "zstd" {
                    return Err(Box::new(IkvblobError::Other(format!(
                        "Unsupported compression type: {}",
                        compression_type
                    ))));
                }
                let compression_dict_bytes =
                    md_map["compression_dict"]
                        .as_bytes()
                        .ok_or(Box::new(IkvblobError::Other(
                            "compression_dict is not a binary".to_string(),
                        )))?;
                Some(Box::new(zstd::dict::DecoderDictionary::copy(
                    &compression_dict_bytes,
                )))
            } else {
                None
            };
        Ok(IkvblobView {
            header,
            compression_dict,
            source_memory,
            hashers,
            phantom_key: std::marker::PhantomData,
            phantom_idx: std::marker::PhantomData,
            phantom_lifetime: std::marker::PhantomData,
        })
    }

    // #[maybe_async::maybe_async]
    async fn get_hashmap_bucket(&self, idx: usize) -> Result<Vec<Option<(K, Idx)>>, IkvblobError> {
        log::debug!("{}", idx);

        // fn get_hashmap_bucket(&self, idx: usize) -> Result<impl Iterator<Item = Option<(K, Idx)> >, IkvblobError> {
        let start = (self.header.cuckoo_table_offset
            + (idx
                * Option::<(K, Idx)>::SER_SIZE
                * self.header.cuckoo_table_elems_per_bucket as usize) as u64)
            as usize;
        let end = start
            + (Option::<(K, Idx)>::SER_SIZE * self.header.cuckoo_table_elems_per_bucket as usize)
                as usize;
        let slice = self.source_memory.read_slice(start..end).await;
        let mut reader = &slice[..];

        // (0..self.header.cuckoo_table_elems_per_bucket)
        //     .map(|_| Option::<(K, Idx)>::read(&mut reader).unwrap())
        //     .map(|x| x.map_err(|e| IkvblobError::Other(e.to_string())))
        //     .collect::<Result<Vec<_>, _>>()
        //     .map(|x| x.into_iter())
        let mut result = Vec::new();
        for _ in 0..self.header.cuckoo_table_elems_per_bucket {
            result.push(
                Option::<(K, Idx)>::read(&mut reader)
                    .map_err(|e| IkvblobError::Other(e.to_string()))?,
            );
        }
        Ok(result)
    }

    // #[maybe_async::maybe_async]
    async fn lookup_value_address(&self, key: &K) -> Option<Idx> {
        for hasher in &self.hashers {
            let idx = (hasher.hash(key) % (self.header.cuckoo_table_num_buckets() as u64)) as usize;
            log::debug!("hash: {}", hasher.hash(key));
            log::debug!("num_buckets: {}", self.header.cuckoo_table_num_buckets());
            log::debug!("idx: {}", idx);
            let bucket = self.get_hashmap_bucket(idx).await.unwrap();
            for entry in bucket {
                match entry {
                    Some((k, v)) if &k == key => return Some(v),
                    _ => continue,
                }
            }
        }
        None
    }

    // #[maybe_async::maybe_async]
    async fn _read_debug(&self) -> (IkvblobHeader, Vec<Vec<Option<(K, Idx)>>>, Vec<u8>) {
        let mut cuckoo_table = Vec::new();
        for i in 0..self.header.cuckoo_table_num_buckets() {
            let bucket = self.get_hashmap_bucket(i as usize).await.unwrap();
            cuckoo_table.push(bucket);
        }

        let mut value_blob = Vec::new();
        let start = self.header.value_blob_offset as usize;
        let end = start + self.header.value_blob_size as usize;
        value_blob.extend_from_slice(&self.source_memory.read_slice(start..end).await);

        (self.header.clone(), cuckoo_table, value_blob)
    }
}

impl<'a, M: Memory, K: std::fmt::Debug> IkvblobView<'a, M, K, (u64, u64)>
where
    K: Hash + Copy + Eq,
    Option<(K, (u64, u64))>: StaticSizeSerializable,
    K: StaticSizeSerializable,
{
    fn try_decompress(&self, bytes: &[u8]) -> Result<Vec<u8>, IkvblobError> {
        match &self.compression_dict {
            None => Ok(bytes.into()),
            Some(dict) => {
                let mut decoder =
                    zstd::bulk::Decompressor::with_prepared_dictionary(&dict).unwrap();

                // try decoding into an ever higher capacity buffer until it works
                let starting_buffer = 4 * 1024;
                for i in 0..6 {
                    let mut result = Vec::with_capacity(starting_buffer * (2 << i));
                    match decoder.decompress_to_buffer(&bytes, &mut result) {
                        Ok(_) => return Ok(result),
                        Err(_) => continue,
                    }
                }

                Err(IkvblobError::Other("Decompression failed".to_string()))
            }
        }
    }

    pub async fn lookup(&self, key: &K) -> Result<Option<Vec<u8>>, IkvblobError> {
        log::debug!("{:?}", key);
        let (offset, size) = match self.lookup_value_address(key).await {
            Some(v) => v,
            None => return Ok(None),
        };

        let start = (self.header.value_blob_offset + offset) as usize;
        let end = start + size as usize;

        let raw_bytes = self.source_memory.read_slice(start..end).await;
        self.try_decompress(&raw_bytes).map(Some)
        // match &self.compression_dict {
        //     None => Ok(Some(raw_bytes.into())),
        //     Some(dict) => {
        //         let mut decoder =
        //             zstd::bulk::Decompressor::with_prepared_dictionary(&dict).unwrap();

        //         // try decoding into an ever higher capacity buffer until it works
        //         let starting_buffer = 4 * 1024;
        //         for i in 0..6 {
        //             let mut result = Vec::with_capacity(starting_buffer * (2 << i));
        //             match decoder.decompress_to_buffer(&raw_bytes, &mut result) {
        //                 Ok(_) => return Ok(Some(result)),
        //                 Err(_) => continue,
        //             }
        //         }

        //         Err(IkvblobError::Other("Decompression failed".to_string()))
        //     }
        // }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        cuckoo::StaticCuckooTable, fileformat_write::write_combined_file, multihash::Multihash,
    };

    use super::*;

    #[tokio::test]
    async fn test_full_ser_deser() {
        let test_size = 100;
        let kvs = (0..test_size).map(|i| {
            (
                Multihash::<32>::wrap(2, [i as u8; 32]),
                (i as u64, 1 as u64),
            )
        });
        let table = StaticCuckooTable::<8, 2, _, _>::from_iter(kvs, 1.2);

        let data = (0..test_size).map(|x| x as u8).collect::<Vec<u8>>();

        let mut buf = Vec::new();
        write_combined_file(&table, &Vec::new(), &data[..], data.len(), &mut buf).unwrap();

        let view = IkvblobView::wrap(buf).await.unwrap();

        let (_, cuckoo_reconstr, _) = view._read_debug().await;
        let reconstr_tpd = cuckoo_reconstr
            .into_iter()
            .map(|x| TryInto::<[Option<(Multihash<32>, (u64, u64))>; 8]>::try_into(x).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(reconstr_tpd, table.table);

        // dbg!(reconstr_tpd);
        // assert_eq!(table.table)

        // dbg!(&view);

        for i in 0..test_size {
            let key = Multihash::<32>::wrap(2, [i as u8; 32]);
            let value = view.lookup(&key).await.unwrap().unwrap();
            assert_eq!(value, vec![i as u8]);
        }
    }
}
