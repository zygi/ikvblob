// #![feature(iter_map_windows)]

// extern crate rayon;
// extern crate rkyv;
// extern crate thread_local;
// extern crate umash;

// use memmap2::Mmap;
// // use memmap::MmapOptions;
// // use mmap_allocator::MmapAllocator;
// use rayon::prelude::*;
// use rkyv::Deserialize;
// use std::{
//     collections::HashMap,
//     default,
//     fs::{self, File},
//     hash::{Hash, Hasher},
//     io::{self, Read, Write},
//     iter::FromIterator,
//     mem,
// };
// use ikvblob::utils::{index_file_iterator, parsed_signature_tuple_iterator};
// // use thread_local::ThreadLocal;
// use umash::Params;

// fn simple_cuckoo_test<I, const N: usize>(elems: Vec<I>, hashers: &[Params; N]) -> ()
// where
//     I: Ord + Eq + Hash + Copy + Send,
//     //   H: Hasher
// {
//     let archived = elems;
//     let arch_len = archived.len() as u64;

//     let num_elems = 3 * arch_len as usize;
//     let mut table = vec![-1i64; num_elems];
//     let mut hash_idx_used = vec![0; num_elems];
//     let mut collisions = 0;
//     let looping_limit = 10000;

//     for (val, key) in archived.iter().enumerate() {
//         let mut key = key;
//         let mut val = val as i64;
//         let mut i = 0;

//         let mut replace_with_which = 0;
//         let mut hash_start_pos = 2;

//         'outer: loop {
//             let mut htorep = 0usize;
//             for mut j in 0..hashers.len() {
//                 // j = (j + hash_start_pos + 1) % hashers.len();
//                 let h = (hashers[j].hash(key) % (table.len() as u64)) as usize;

//                 if j == replace_with_which {
//                     htorep = h;
//                 }

//                 if table[h] == -1 {
//                     table[h] = val;
//                     hash_idx_used[h] = j as u8;
//                     break 'outer;
//                 }
//             }

//             // let h = (hashers[replace_with_which].hash(key) % (table.len() as u64)) as usize;
//             replace_with_which = (replace_with_which + 1) % 3;
//             // let h2 = h2;
//             let old = table[htorep];
//             table[htorep] = val;
//             val = old;
//             key = &archived[val as usize];
//             hash_start_pos = hash_idx_used[htorep] as usize;
//             i += 1;
//             if i > looping_limit {
//                 collisions += 1;
//                 // dbg!("LOOP at ", val);
//                 break 'outer;
//             }
//         }
//     }

//     dbg!(collisions);
// }

// // const BUCKET_SIZE: usize = 2;

// fn view_based_lookup<'a, K, V, const BS: usize, const HS: usize>(
//     table: &'a [[Option<(K, V)>; BS]],
//     key: &K,
//     hashers: &[Params; HS],
// ) -> Option<&'a V>
// where
//     K: Hash + Eq + Copy,
// {
//     for h in hashers {
//         let h = (h.hash(key) % (table.len() as u64)) as usize;
//         let bucket = &table[h];
//         for i in 0..BS {
//             match &bucket[i] {
//                 Some((k, v)) if k == key => return Some(v),
//                 _ => continue,
//             }
//         }
//     }
//     None
// }


// struct StaticCuckooTable<K: Hash + Eq + Copy, V, const BS: usize, const HS: usize> {
//     table: Vec<[Option<(K, V)>; BS]>,
//     hashers: [Params; HS],
// }

// // impl<K, V, const BS: usize, const HS: usize> From<Vec<(K, V)>> for StaticCuckooTable<K, V, BS, HS>
// // where
// //     K: Hash + Eq + Copy,
// // {

// // }

// impl<K, V, const BS: usize, const HS: usize> StaticCuckooTable<K, V, BS, HS>
// where
//     K: Hash + Eq + Copy,
// {
//     fn lookup(&self, key: &K) -> Option<&V> {
//         view_based_lookup(&self.table, key, &self.hashers)
//     }

//     fn from_iter(elems: Vec<(K, V)>, ratio: f32) -> Self {
//         let arch_len = elems.len() as u64;
//         let outer_size = (ratio * (arch_len as f32) / BS as f32).ceil() as usize;

//         dbg!("Started initing");

//         // let mut table: Vec<[Option<(K, V)>; BS]> = vec![[None; BS]; outer_size];
//         // This uglier initialization is necessary because V isn't Copy so Option<(K, V)> isn't either
//         let mut table = Vec::<[Option<(K, V)>; BS]>::new();
//         table.resize_with(outer_size, || [(); BS].map(|_| None));

//         dbg!("Ended initing");

//         let hashers = {
//             let mut ctr = 0;
//             [(); HS].map(|_| {
//                 ctr += 1;
//                 Params::derive(ctr, &[ctr as u8; 32])
//             })
//         };

//         let mut collisions = 0;

//         for (key, val) in elems {
//             let mut key = key;
//             let mut val = val;
//             let mut i = 0;

//             let mut replace_with_which_outer = 0;
//             let mut replace_with_which_inner = 0;
//             // let mut hash_start_pos = 2;

//             'outer: loop {
//                 let mut htorep = 0usize;
//                 for j in 0..hashers.len() {
//                     let h = (hashers[j].hash(key) % (table.len() as u64)) as usize;

//                     if j == replace_with_which_outer {
//                         htorep = h;
//                     }

//                     for k in 0..BS {
//                         if table[h][k].is_none() {
//                             table[h][k] = Some((key, val));
//                             break 'outer;
//                         }
//                     }
//                 }
//                 let postorep = replace_with_which_inner;

//                 replace_with_which_outer = (replace_with_which_outer + 1) % hashers.len();
//                 replace_with_which_inner = (replace_with_which_outer + 1) % BS;
//                 // let old = table[htorep][postorep].unwrap();
//                 // table[htorep][postorep] = Some((key, val));

//                 let current = Some((key, val));
//                 let old = mem::replace(&mut table[htorep][postorep], current).unwrap();

//                 val = old.1;
//                 key = old.0;
//                 i += 1;
//                 if i > 1000 {
//                     // Probably stuck in a loop
//                     collisions += 1;
//                     break 'outer;
//                 }
//             }
//         }

//         if collisions > 0 {
//             panic!("Collisions: {}", collisions);
//         }

//         Self { table, hashers }
//     }
// }

// // type T = (u64, u32);
// const SZ: usize = mem::size_of::<u64>() + mem::size_of::<u32>();
// const HS: usize = 2;

// fn main() -> Result<(), Box<dyn std::error::Error>> {

//     let hashers = {
//         let mut ctr = 0;
//         [(); HS].map(|_| {
//             ctr += 1;
//             Params::derive(ctr, &[ctr as u8; 32])
//         })
//     };

//     dbg!("Starting");
//     let memmap = unsafe { Mmap::map(&File::open("cuckoo_table.bin")?)? };
//     let deser = unsafe { rkyv::archived_root::<Vec<[Option<([u8; 16], (u64, u32))>; 2]>>(&memmap) };
//     // let memmap = unsafe { Mmap::map(&File::open("testout.cstm.index")?)? };
//     // let deser = rkyv::check_archived_root::<Vec<[Option<([u8; 16], (u64, u32))>; 2]>>(File::open("testout.cstm.index"))?;


//     let signature_memmap = unsafe { Mmap::map(&File::open("testout.cstm")?)? };
//     let zstd_dictionary = fs::read("testout.cstm.comprdict")?;
//     let mut decoder = zstd::bulk::Decompressor::with_dictionary(&zstd_dictionary).unwrap();



//     println!("Done! {} {:?}", deser.len(), deser[0]);


//     let bucket_fn = |key: usize| -> [Option<([u8; 16], (u64, u32))>; 2] {
//         deser[key].deserialize(&mut rkyv::Infallible).unwrap()
//     };
        
//     let indices = index_file_iterator("testout.cstm.index")?;
//     // dbg!(indices.count());

//     let elem_iter =
//         parsed_signature_tuple_iterator("../rust-opentimestamps/shrunk_timestamps_scimag.csv.zst")?
//             .map(|(k, v)| (TryInto::<[u8; 16]>::try_into(k).unwrap(), v));

//     let mut collected = elem_iter
//         .zip(indices)
//         .take(100000)
//         // ;
//         .collect::<Vec<_>>();

//     collected.sort_by(|((x, _), _), ((y, _), _)| x.cmp(y));
//     collected.dedup_by(|((x, _), _), ((y, _), _)| x == y);

//     dbg!(deser.len());

//     let mut count_mistakes = 0;
//     let mut count_total = 0;
//     for ((key, sign), val) in collected.into_iter() {
//         // let val = val as i64;
//         // let key = &key;
//         let found: Option<(u64, u32)> = viewkv::utils::bucket_fn_based_lookup::<[u8; 16], (u64, u32), 2, HS>(&bucket_fn, deser.len(), &key, &hashers);
//         // let found = view_based_lookup(&table, key, &hashers);
//         // let found = tbl.lookup(key);
//         count_total += 1;
//         match found {
//             Some(v) => {

//                 // look up the compressed signature
//                 let compr_sig = signature_memmap[v.0 as usize..(v.0 + v.1 as u64) as usize].to_vec();
//                 let mut decompressed = decoder.decompress(&compr_sig, 5000)?;
            
//                 if decompressed != sign {
//                     count_mistakes += 1;
//                     println!("Mistake at {}", count_total);
//                     // dbg!(decompressed, sign);
//                 }

//                 // if val != v {
//                 //     count_mistakes += 1;
//                 //     // dbg!(val, v);
//                 // }
//                 // assert_eq!(val, v);
//             }
//             None => {
//                 panic!("Not found")
//             }
//         }
//     }

//     dbg!(count_mistakes, count_total);


//     // let indices = index_file_iterator("testout.cstm.index")?;
//     // // let archivedv = {
//     // //     let bytes = std::fs::read("../rust-opentimestamps/keys.bin")?;
//     // //     rkyv::from_bytes::<Vec<[u8; 16]>>(&bytes).unwrap()
//     // // };

//     // let elem_iter =
//     //     parsed_signature_tuple_iterator("../rust-opentimestamps/shrunk_timestamps_scimag.csv.zst")?
//     //         .map(|(k, _)| TryInto::<[u8; 16]>::try_into(k).unwrap());

//     // let mut collected = elem_iter
//     //     .zip(indices)
//     //     // .take(10000)
//     //     .collect::<Vec<_>>();
//     // collected.sort_by(|(x, _), (y, _)| x.cmp(y));
//     // collected.dedup_by(|(x, _), (y, _)| x == y);

//     // dbg!(collected.len());

//     // // let zipped = indices.zip(archivedv.into_iter());

//     // // dbg!(zipped.count());

//     // // let archived = archivedv;
//     // // // let archived: Vec<_> = archivedv[0..10000].to_vec();

//     // // dbg!(archived.len());
//     // dbg!("Starting construction");

//     // let tbl: StaticCuckooTable<[u8; 16], (u64, u32), 2, 2> =
//     //     StaticCuckooTable::from_iter(collected, 1.5f32);

//     // dbg!("Done construction");
//     // // dbg!("Expected size: ", tbl.table.len() * 2 * (16 + 8 + 4));
//     // dbg!(
//     //     "Expected size: ",
//     //     tbl.table.len() * std::mem::size_of::<[Option<([u8; 16], (u64, u32))>; 2]>()
//     // );

//     // // serialize the table

//     // let mut file = io::BufWriter::new(File::create("cuckoo_table.bin")?);
//     // file.write_all(&rkyv::to_bytes::<_, 1024>(&tbl.table)?)?;

//     // for (val, key) in archived.into_iter().enumerate() {
//     //     let val = val as i64;
//     //     let key = &key;
//     //     // let found = bucket_fn_based_lookup(&bucket_fn, table.len(), key, &hashers);
//     //     // let found = view_based_lookup(&table, key, &hashers);
//     //     let found = tbl.lookup(key);
//     //     match found {
//     //         Some(v) => {
//     //             assert_eq!(val, *v);
//     //         }
//     //         None => {
//     //             dbg!(val);
//     //         }
//     //     }
//     // }
//     Ok(())
// }
