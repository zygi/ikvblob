# IkvBlob

**IkvBlob** is an archive file format designed to provide an indexed **read-only** key-value store that can be efficiently searched by doing a few random reads of the archive file.

This means that IkvBlob key-value databases don't have to be hosted by running a traditional web service. Anything that provides a random-access file API is enough.
In particular, an IkvBlob database can be hosted on Amazon S3, IPFS, or a remote mounted drive, without needing to run an extra web server.

IkvBlob is inspired by projects like (sqlite-s3vfs)[https://github.com/uktrade/sqlite-s3vfs] which implement a Virtual Filesystem layer for SQLite, allowing it 
to efficiently access SQLite database blobs that are hosted remotely. Such virtual file system approaches are very powerful and general, but also 1) potentially slower than necessary, and 2) quite complicated to compile and run. The goal of IkvBlob is to be as simple as possible, while also providing excellent key lookup performance.

**Pros**
- Can be "hosted" on S3, IPFS etc.
- Fast key lookups: in the intended scenario, latency is twice the file seek latency of your file host.
- Simple design: the implementation is small and simple. Except for the (optional) ZSTD compression of values, IkvBlob lookup code can be reimplemented without much effort.

**Cons**
- No access control: if anyone can look up a record, they can also download the whole blob.
- Just a key-value store: no SQL capabilities or range queries.
- (As of now) no sharding: while in principle IkvBlob archives work fine even at the size of TBs, manipulating individual files of that size can get awkward.

## Instructions

TODO

## Performance

### Lookup

Each key lookup consists of:
1) Reading archive metadata if it's not cached
    - (if compression is involved, this can be in the order of MBs)
2) Reading two places in the index concurrently (hundreds of bytes)
3) Reading the value bytes (size of the value)
4) Optionally decompressing the data

In the indended setting (a client library querying a small number of keys from a several-GB IkvBlob file hosted on a remote filesystem), the CPU use on the client side is negligible, except potentially for decompression. For small values expected latency should be `2 * file_seek_latency`.

### File Construction

*TL;DR The set of keys must fit in memory. Values can be streamed.*

Preprocessing the values can be done in a streaming way and isn't limited by RAM. If value compression is turned off, the preprocessing is very quick and time spent is dominated by reading the values from disk. If value compression is on, RAM becomes important (since the sample of values used to learn the dictionary must fit in RAM), but but the compression dict sample size is configurable. Compression can also take a significant amount of time. 

Constructing the index involves building a cuckoo hashmap of the keys and value-offsets, and as of now requires that your set of keys fits into RAM. A naive estimate is that 16GB of RAM is enough to construct an IkvBlob with 200M keys. Index construction usually takes seconds to minutes.



## Structure
The values are stored in one large concatenated blob. We keep the `(pointer; size)` addresses that we can use to access an individual value from the blob.

The index is a bucketed Cuckoo hash table mapping the keys to their value addresses. We use 2 hash functions,  bucket size of 8, and load factor of 5/6.

The dynamic metadata section is represented as a CBOR-encoded object.



### File format
```
IkvBlob := magic header dynamic_metadata index_table values checksum
magic := '\0ikvbblob' : u8[8]
header := 
    version: u64
    dynamic_metadata_offset: u64
    dynamic_metadata_size: u64
    cuckoo_table_offset: u64
    cuckoo_table_size: u64
    cuckoo_entry_size: u64
    cuckoo_table_elems_per_bucket: u64
    cuckoo_table_num_hashers: u64
    value_blob_offset: u64
    value_blob_size: u64

// dynamic_metadata is a serialized CBOR object. Some values in it are used
// by the implementation, see below. Users are free to put other values in as well. 
dynamic_metadata := CBOR_encoded : u8[dynamic_metadata_size]

index_table := index_entry* // a vector of statically sized entries
index_entry := (key index) : u8[cuckoo_entry_size] 
// Here, in principle we want to say
// key := bytes : u8[key_size]  
// index := u64 u64 // a tuple of (offset_into_values_buffer, entry_size_in_bytes)
// 
// since in principle IkvBlob supports arbitrary constant-sized keys. But the current implementation is specialized
// for keys in the form of (hash: u8[16|32|64], hash_code: u8), so to keep keys aligned we steal a byte from the index (since it's u64 anyway) 
// and use it to store the code of the hash. See `ikvblob_lib/src/fileformat_write.rs:215` for the specifics


values := value* : u8[value_blob_size]
value := u8[varlength] // values are just concatenated arbitrarily-sized binary blobs

checksum := bytes : u8[4] // crc32 checksum of the preceeding data
```

### Dynamic metadata
Each IkvBlob contains a dynamic metadata object, encoded as [CBOR](https://cbor.io/). Some keys in it are reserved since they are used by the implementation. Other than that, users are free to put arbitrary values there, e.g. the description of the archive or auxiliary data needed to interpret the binary values. 

Reserved metadata keys as of Version 1:
- *compression_type*: optional. One of `["zstd"]`.
- *compression_dict*: must exist if *compression_type* exists. Contains bytes of the dictionary that was used to compress
the value entries.




## Assorted Q&A

### Why Cuckoo Hashing?
Because it gives us constant-time access. Looking up *any* key will take no more than 2 looks into the table. Many other hashing schemes only have expected O(1) access, and for some keys might require many more than 1 or 2 looks.

### Why not perfect hashing?
As amazing as perfect hashing is, there are some information-theoretic bounds that limit its usefulness. Representing a perfect hashing function [requires storage proportional to the number of keys](https://en.wikipedia.org/wiki/Perfect_hash_function#Performance_of_perfect_hash_functions) - for a minimal perfect hash function, `~1.44` bits per element. A client trying to query an IkvBlob would first need to download the hash function's representation, which could easily become tens or even hundreds of MBs. In exchange, we could replace two concurrent index reads by one read. In the scenario where seek times dominate, this wouldn't save us much.

Perfect hashing could be useful for smaller IkvBlobs though. We might explore that direction later.

### You advertise it as "simple", but your JS library is a Rust-wasm-bindgen abomination.
Sadly yes. We wanted to ship ZSTD-value-compression as a core feature, and packaging its wasm build with a separate, hand written js-native implementation would be quite hard, whereas with Rust it was trivial. An implementation that doesn't support compression should be easy to implement in JS and we'd welcome contributions for it.