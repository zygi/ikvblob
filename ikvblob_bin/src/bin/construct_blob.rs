// // extern crate source;

// use std::{cell::RefCell, collections::HashMap, fmt::write, io::Write, sync::mpsc};

// use clap::Parser;
// use indicatif::ParallelProgressIterator;
// use rand::Rng;
// use rayon::prelude::*;
// use ikvblob::{
//     construction,
//     utils::{self, recover_order, sample},
// };
// // use rng::pre

// /// Simple program to greet a person
// #[derive(Parser, Debug)]
// #[command(version, about, long_about = None)]
// struct Args {
//     input: String,
//     output: String,
// }

// fn main() -> Result<(), Box<dyn std::error::Error>> {
//     let args = Args::parse();
//     let get_iterator =
//         || viewkv::utils::parsed_signature_tuple_iterator(&args.input)
//         // .map(|x| x.take(1_000_000))
//         ;

//     let sample_count = 1_000_000;
//     let max_dict_size_in_bytes = 16 * 1024 * 1024;
//     let dict_buffer = construction::build_zstd_dictionary_from_sample(
//         get_iterator()?.map(|(_, v)| v),
//         max_dict_size_in_bytes,
//         sample_count,
//     )?;
//     dbg!("Done computing dict.");

//     // Now we stream again and compress with the dictionary
//     let output_filename = args.output;
//     let mut bufwr = std::io::BufWriter::new(std::fs::File::create(output_filename.clone())?);
//     let mut index_bufwr =
//         std::io::BufWriter::new(std::fs::File::create(output_filename.clone() + ".index")?);
//     let mut compr_dict_bufwr =
//         std::io::BufWriter::new(std::fs::File::create(output_filename + ".comprdict")?);

//     let stream = get_iterator()?;

//     let tl = thread_local::ThreadLocal::<RefCell<zstd::bulk::Compressor>>::new();
//     let build_enc =
//         || RefCell::new(zstd::bulk::Compressor::with_dictionary(15, &dict_buffer).unwrap());

//     let compressed = utils::ordered_par_map(stream, |i, (_, v)| {
//         let encoder = tl.get_or(build_enc);
//         let compr = encoder.borrow_mut().compress(&v).unwrap();
//         // let compr = zstd::stream::encode_all(&v, 15)?;
//         compr
//     });

//     let mut index_pos_counter: usize = 0;
//     let writer_fn = |v: Vec<u8>| {
//         bufwr.write_all(&v).unwrap();
//         index_bufwr
//             .write_all(&index_pos_counter.to_le_bytes())
//             .unwrap();

//         let len = v.len();
//         assert!(len < std::u32::MAX as usize);
//         index_bufwr.write_all(&(len as u32).to_le_bytes()).unwrap();
//         index_pos_counter += len;
//     };

//     compressed.for_each(writer_fn);
//     compr_dict_bufwr.write_all(&dict_buffer)?;

//     Ok(())
// }

// // fn main() -> Result<(), Box<dyn std::error::Error>> {
// //     let args = Args::parse();

// //     let mut approx_count = 0;
// //     let get_iterator =
// //         || viewkv::utils::parsed_signature_tuple_iterator(&args.input)
// //         // .map(|x| x.take(1_000_000))
// //         ;

// //     // let sample_ratio = 1;
// //     let sample_count = 1_000_000;
// //     let max_dict_size_in_bytes = 16 * 1024 * 1024;

// //     let dict_buffer: Vec<u8> = {
// //         let stream = get_iterator()?;
// //         let vs = stream.map(|(_, v)| {
// //             approx_count += 1;
// //             v
// //         });
// //         let samples = sample(&mut rand::thread_rng(), sample_count, vs);

// //         dbg!("Done collecting samples");
// //         zstd::dict::from_samples(&samples, max_dict_size_in_bytes)
// //     }?;

// //     dbg!("Done computing dict. Approx count ", approx_count);

// //     // Now we stream again and compress with the dictionary
// //     let output_filename = args.output;
// //     let mut bufwr = std::io::BufWriter::new(std::fs::File::create(output_filename.clone())?);
// //     let mut index_bufwr = std::io::BufWriter::new(std::fs::File::create(output_filename.clone() + ".index")?);
// //     let mut compr_dict_bufwr = std::io::BufWriter::new(std::fs::File::create(output_filename + ".comprdict")?);

// //     let stream = get_iterator()?;

// //     let tl = thread_local::ThreadLocal::<RefCell<zstd::bulk::Compressor>>::new();
// //     let build_enc =
// //         || RefCell::new(zstd::bulk::Compressor::with_dictionary(15, &dict_buffer).unwrap());

// //     let (tx, rx) = mpsc::sync_channel(std::thread::available_parallelism().map_or(8, |n| n.get()));

// //     let mut index_pos_counter: usize = 0;
// //     let writer_fn = |v: Vec<u8>| {
// //         bufwr.write_all(&v).unwrap();
// //         index_bufwr.write_all(&index_pos_counter.to_le_bytes()).unwrap();

// //         let len = v.len();
// //         assert!(len < std::u32::MAX as usize);
// //         index_bufwr.write_all(&(len as u32).to_le_bytes()).unwrap();
// //         index_pos_counter += len;
// //     };

// //     rayon::scope(|s| {
// //         s.spawn(move |_| {
// //             stream
// //                 .enumerate()
// //                 .par_bridge()
// //                 .progress_count(approx_count)
// //                 .map(|(i, (_, v))| {
// //                     // pretend to do some work
// //                     let encoder = tl.get_or(build_enc);
// //                     let compr = encoder.borrow_mut().compress(&v).unwrap();
// //                     (i, compr)
// //                 })
// //                 .for_each_with(tx, |tx, pair| {
// //                     let _ = tx.send(pair);
// //                 });
// //         });

// //         recover_order(rx, writer_fn);
// //     });

// //     compr_dict_bufwr.write_all(&dict_buffer)?;

// //     Ok(())

// // }
