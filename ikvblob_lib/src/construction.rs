use std::{io, sync::mpsc};

use log::info;
use rand::seq::SliceRandom;

use rayon::prelude::*;

use crate::utils::{self, sample};

// TODO: potential performance improvements:
// reservoir sampling can certainly be parallelized with rayon
// the dictionary construction can maybe be parallelized too, not sure
pub fn build_zstd_dictionary_from_sample<V, I>(
    it: I,
    max_dict_size_in_bytes: usize,
    sample_count: usize,
) -> Result<Vec<u8>, io::Error>
where
    I: Iterator<Item = V>,
    V: AsRef<[u8]>,
{
    // let max_dict_size_in_bytes = 16 * 1024 * 1024;

    let dict_buffer: Vec<u8> = {
        let vs = it;
        let samples = sample(&mut rand::thread_rng(), sample_count, vs);

        info!("Done collecting samples");
        zstd::dict::from_samples(&samples, max_dict_size_in_bytes)
    }?;

    // dbg!("Done computing dict. Approx count ", approx_count);

    Ok(dict_buffer)
}

const COMPRESSION_LEVEL: i32 = 5;

pub fn construct_simple2<V, FNV>(
    data: Vec<V>,
    mut value_cb: FNV,
    max_dict_size_in_bytes: usize,
    sample_count: usize,
) -> Result<Vec<u8>, Box<dyn std::error::Error>>
where
    V: AsRef<[u8]> + Send + Clone,
    FNV: FnMut(Vec<u8>) -> () + Send + Sync,
{
    let rng = &mut rand::thread_rng();
    let sample = &data.choose_multiple(rng, sample_count).collect::<Vec<_>>();
    let dict_buffer = zstd::dict::from_samples(sample, max_dict_size_in_bytes)?;
    let compression_dict = zstd::dict::EncoderDictionary::copy(&dict_buffer, COMPRESSION_LEVEL);
    let build_enc = || zstd::bulk::Compressor::with_prepared_dictionary(&compression_dict).unwrap();
    let mut enc = build_enc();

    data.into_iter()
        .map(|v| enc.compress(v.as_ref()))
        .for_each(|v| value_cb(v.unwrap()));
    Ok(dict_buffer)
}

pub fn construct_simple<V, FNV>(
    data: Vec<V>,
    mut value_cb: FNV,
    max_dict_size_in_bytes: usize,
    sample_count: usize,
) -> Result<Vec<u8>, Box<dyn std::error::Error>>
where
    V: AsRef<[u8]> + Send + Clone,
    FNV: FnMut(Vec<u8>) -> () + Send + Sync,
{
    let rng = &mut rand::thread_rng();
    let sample = &data.choose_multiple(rng, sample_count).collect::<Vec<_>>();
    let dict_buffer = zstd::dict::from_samples(sample, max_dict_size_in_bytes)?;
    let compression_dict = zstd::dict::EncoderDictionary::copy(&dict_buffer, COMPRESSION_LEVEL);
    let build_enc = || zstd::bulk::Compressor::with_prepared_dictionary(&compression_dict).unwrap();

    // let mut encoded = pariter::scope(|scope| data
    //     .into_iter()
    //     .enumerate()
    //     .parallel_map_scoped(scope, move |(i, v)| {
    //         let compr = build_enc().compress(v.as_ref()).unwrap();
    //         (i, compr)
    //     })
    //     .collect::<Vec<_>>()).unwrap();

    let mut encoded = data
        .into_iter()
        .enumerate()
        // .parallel_map_scoped(scope, move |(i, v)| {
        //     let compr = build_enc().compress(v.as_ref()).unwrap();
        //     (i, compr)
        // })
        .collect::<Vec<_>>()
        .into_par_iter()
        // .chunk
        .chunks(4 * 1024)
        .flat_map_iter(|chunk| {
            let mut enc = build_enc();
            chunk.into_iter().map(move |(i, v)| {
                let compr = enc.compress(v.as_ref()).unwrap();
                (i, compr)
            })
        })
        // .map_init(build_enc, |enc, els| {
        //     els.into_iter().map(|(i, v)| {
        //         let compr = enc.compress(v.as_ref()).unwrap();
        //         (i, compr)
        //     }).collect::<Vec<_>>()
        //     // let compr = enc.compress(v.as_ref()).unwrap();
        // (i, compr)
        .collect::<Vec<_>>();

    encoded.par_sort_by_key(|(i, _)| *i);
    encoded.into_iter().for_each(|(_, v)| value_cb(v));

    // let mut compressor = zstd::bulk::Compressor::with_dictionary(COMPRESSION_LEVEL, &dict_buffer)?;
    // data.into_iter().map(|v| compressor.compress(v.as_ref())).for_each(|v| value_cb(v.unwrap()));
    Ok(dict_buffer)
}

pub fn construct<V, F, IT, FNV>(
    get_iterator: F,
    mut value_cb: FNV,
    max_dict_size_in_bytes: usize,
    sample_count: usize,
) -> Result<Vec<u8>, Box<dyn std::error::Error>>
// pub fn construct<K, V, F, IT, W1, W2, W3>(get_iterator: F, index_output_writer: &mut W1, value_output_writer: &mut W2, dict_output_writer: &mut W3) -> Result<(), Box<dyn std::error::Error>>
where
    V: AsRef<[u8]> + Send,
    F: Fn() -> IT,
    IT: Iterator<Item = V> + Send,
    FNV: FnMut(Vec<u8>) -> () + Send + Sync,
{
    // let sample_count = 1_000_000;
    // let max_dict_size_in_bytes = 16 * 1024 * 1024;
    let dict_buffer =
        build_zstd_dictionary_from_sample(get_iterator(), max_dict_size_in_bytes, sample_count)?;
    info!("Done computing dict.");

    let stream = get_iterator();
    let compression_dict = zstd::dict::EncoderDictionary::copy(&dict_buffer, COMPRESSION_LEVEL);

    let build_enc = || zstd::bulk::Compressor::with_prepared_dictionary(&compression_dict).unwrap();

    let writer_fn = |v: Vec<u8>| {
        value_cb(v);
    };

    let (tx, rx) = mpsc::sync_channel(std::thread::available_parallelism().map_or(8, |n| n.get()));

    // let enc = zstd::bulk::Compressor::with_dictionary(COMPRESSION_LEVEL, &dict_buffer).unwrap();

    // Run the compression in parallel using rayon, but preserve sequentiality.
    // Idea taken from https://stackoverflow.com/a/76963325 .
    // The way this works is: we enumerate our input iterator, preserve the idx when doing the computation in parallel, send all results to a single channel.
    // The result collector then stores the index of the tip, and if index `i` hasn't been received yet, holds all results for `j > i` in a buffer.
    // This will cause the buffer to grow large if tasks take very variable amounts of time or the consumer is slow. Luckily here that's not the case.
    rayon::scope(|s: &rayon::Scope<'_>| {
        s.spawn(move |_| {
            stream
                .into_iter()
                .enumerate()
                .array_chunks::<4096>()
                // .chunks(16000)
                .par_bridge()
                // .progress_count(approx_count)
                .map_init(build_enc, |enc, xs| {
                    xs.map(|(i, v)| {
                        // let encoder: &RefCell<zstd::bulk::Compressor<'_>> = tl.get_or(build_enc);
                        let compr = enc.compress(v.as_ref()).unwrap();
                        // let compr = zstd::stream::encode_all(&v, COMPRESSION_LEVEL)?;
                        (i, compr)
                    })
                })
                .for_each_with(tx, |tx, pairs| {
                    for pair in pairs {
                        let _ = tx.send(pair);
                    }
                });
        });

        s.spawn(move |_| utils::recover_order(rx, writer_fn));
    });

    Ok(dict_buffer)
}
