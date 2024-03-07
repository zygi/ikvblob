use std::{fs, io::BufWriter};

use rand::{RngCore, SeedableRng};
use ikvblob::{
    construction, cuckoo, fileformat_read::IkvblobView, fileformat_write::write_combined_file, index::{IndexBuilder, OffsetIndexBuilder}, memory_view::MmapMemory, multihash::Multihash
};

fn mk_test_iter(seed: u64) -> impl Iterator<Item = ([u8; 32], [u8; 16])> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    std::iter::from_fn(move || {
        let mut buf = [0u8; 32];
        rng.fill_bytes(&mut buf);
        let mut buf2 = [0u8; 16];
        rng.fill_bytes(&mut buf2);
        Some((buf, buf2))
    })
}

const CODE_TABLE_EMPTY: u8 = 0;
const CODE_TABLE_MD5: u8 = 1;

fn md5_to_multihash(md5: [u8; 32]) -> Multihash<32> {
    let mut multihash = [0u8; 32];
    multihash[0..32].copy_from_slice(&md5);
    Multihash::<32>::wrap(CODE_TABLE_MD5, multihash)
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mk_fin_iter = || mk_test_iter(2).take(5000);

    let mut value_vect = Vec::<Vec<u8>>::new();
    let mut index_vect = Vec::new();

    let mut index_builder = OffsetIndexBuilder::new();
    // let mut index_builder = EntryIndexBuilder::new();
    let value_cb = |v: Vec<u8>| {
        let idx = index_builder.push(&v);
        value_vect.push(v);
        index_vect.push(idx);
    };

    // let dict_vect = construction::construct(
    //     || mk_fin_iter().map(|(_, v)| v),
    //     value_cb,
    //     16 * 1024 * 1024,
    //     100_000,
    // )?;

    let dict_vect = construction::construct_simple(
        mk_fin_iter().map(|(_, v)| v).collect(),
        value_cb,
        16 * 1024 * 1024,
        100_000,
    )?;

    let hashtable_iter = mk_fin_iter()
        .map(|(k, _)| md5_to_multihash(k))
        .zip(index_vect.clone().into_iter())
        .collect::<Vec<_>>();
    let cuckoo =
        cuckoo::StaticCuckooTable::<2, 2, _, _>::from_iter(hashtable_iter.into_iter(), 1.5);

    let value_len = value_vect.iter().map(|v| v.len()).sum::<usize>();
    let value_reader = std::io::BufReader::new(std::io::Cursor::new(value_vect.concat()));

    // let mut dest_vec = Vec::new();
    let mut dest_file = std::fs::File::create("test.ikvblob")?;

    write_combined_file(&cuckoo, &dict_vect, value_reader, value_len, BufWriter::new(&mut dest_file))?;
    // write_combined_file(&cuckoo, &dict_vect, value_reader, value_len, BufWriter::new(&mut dest_vec))?;
    // dbg!(dest_vec.len());
    let mmap = unsafe { memmap2::Mmap::map(&fs::File::open("test.ikvblob")?)? };



    let view = IkvblobView::wrap(MmapMemory {mmap}).await?;
    // let view = IkvblobView::new(&dest_vec);
    for (idx, ((k, v), _)) in mk_fin_iter().zip(index_vect.into_iter()).enumerate() {
        let k_mod = md5_to_multihash(k);
        if idx == 0 {
            dbg!(&k_mod);
        }
        // if idx % 1000 == 0 {
        // dbg!(idx);
        // }
        // dbg!(idx);
        let val = view.lookup(&k_mod).await.unwrap().unwrap();
        assert_eq!(v.to_vec(), val);
    }

    Ok(())
}
