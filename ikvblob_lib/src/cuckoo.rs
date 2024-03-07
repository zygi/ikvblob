// use umash::Params;
use std::mem;

use crate::{
    fileformat_write::StaticSizeSerializable, parametrized_hasher::SipHasherFactory as SHF,
};

fn view_based_lookup<'a, K, V, const BS: usize, const HS: usize>(
    table: &'a [[Option<(K, V)>; BS]],
    key: &K,
    hashers: &[SHF; HS],
) -> Option<&'a V>
where
    K: Eq + Copy + StaticSizeSerializable,
{
    for h in hashers {
        let h = (h.hash(key) % (table.len() as u64)) as usize;
        let bucket = &table[h];
        for i in 0..BS {
            match &bucket[i] {
                Some((k, v)) if k == key => return Some(v),
                _ => continue,
            }
        }
    }
    None
}

pub struct StaticCuckooTable<
    const BS: usize,
    const HS: usize,
    K: Eq + Copy + StaticSizeSerializable,
    V,
> {
    pub table: Vec<[Option<(K, V)>; BS]>,
    pub hashers: [SHF; HS],
}

impl<const BS: usize, const HS: usize, K, V> StaticCuckooTable<BS, HS, K, V>
where
    K: Eq + Copy + StaticSizeSerializable,
{
    pub fn lookup(&self, key: &K) -> Option<&V> {
        view_based_lookup(&self.table, key, &self.hashers)
    }

    pub fn from_iter<IT>(elems: IT, ratio: f32) -> Self
    where
        IT: ExactSizeIterator<Item = (K, V)>,
    {
        let arch_len = elems.len() as u64;
        let outer_size = (ratio * (arch_len as f32) / BS as f32).ceil() as usize;

        // let mut table: Vec<[Option<(K, V)>; BS]> = vec![[None; BS]; outer_size];
        // This uglier initialization is necessary because V isn't Copy so Option<(K, V)> isn't either
        let mut table = Vec::<[Option<(K, V)>; BS]>::new();
        table.resize_with(outer_size, || [(); BS].map(|_| None));

        let hashers = {
            let mut ctr = 0;
            [(); HS].map(|_| {
                let h = SHF::new(ctr);
                ctr += 1;
                h
            })
        };

        let mut collisions = 0;

        for (key, val) in elems {
            let mut key = key;
            let mut val = val;
            let mut i = 0;

            let mut replace_with_which_outer = 0;
            let mut replace_with_which_inner = 0;
            // let mut hash_start_pos = 2;

            'outer: loop {
                let mut htorep = 0usize;
                for j in 0..hashers.len() {
                    let h = (hashers[j].hash(&key) % (table.len() as u64)) as usize;

                    if j == replace_with_which_outer {
                        htorep = h;
                    }

                    for k in 0..BS {
                        match table[h][k] {
                            None => {
                                table[h][k] = Some((key, val));
                                break 'outer;
                            }
                            Some((entry_key, _)) if entry_key == key => {
                                // Handle duplicate values by inserting the latest one
                                table[h][k] = Some((key, val));
                                break 'outer;
                            }
                            _ => {}
                        }
                        // if table[h][k].is_none() {
                        //     table[h][k] = Some((key, val));
                        //     break 'outer;
                        // }
                    }
                }
                let postorep = replace_with_which_inner;

                replace_with_which_outer = (replace_with_which_outer + 1) % hashers.len();
                replace_with_which_inner = (replace_with_which_outer + 1) % BS;
                // let old = table[htorep][postorep].unwrap();
                // table[htorep][postorep] = Some((key, val));

                let current = Some((key, val));
                let old = mem::replace(&mut table[htorep][postorep], current).unwrap();

                val = old.1;
                key = old.0;
                i += 1;
                if i > 1000 {
                    // Probably stuck in a loop
                    collisions += 1;
                    break 'outer;
                }
            }
        }

        if collisions > 0 {
            panic!("Collisions: {}", collisions);
        }

        Self { table, hashers }
    }
}

// pub struct StaticCuckooTableDyn<K: Hash + Eq + Copy, V> {
//     pub table: Vec<Vec<Option<(K, V)>>>,
//     pub hashers: Vec<SHF>,
//     pub BS: usize,
//     pub HS: usize,
// }

// impl<K, V> StaticCuckooTableDyn<K, V>
// where
//     K: Hash + Eq + Copy,
// {
//     pub fn lookup(&self, key: &K) -> Option<&V> {
//         for h in &self.hashers {
//             let h = (h.hash(key) % (self.table.len() as u64)) as usize;
//             let bucket = &self.table[h];
//             for i in 0..self.BS {
//                 match &bucket[i] {
//                     Some((k, v)) if k == key => return Some(v),
//                     _ => continue,
//                 }
//             }
//         }
//         None
//     }

//     pub fn from_iter<IT>(elems: IT, ratio: f32, HS: usize, BS: usize) -> Self
//     where IT: ExactSizeIterator<Item = (K, V)>
//     {
//         let arch_len = elems.len() as u64;
//         let outer_size = (ratio * (arch_len as f32) / BS as f32).ceil() as usize;

//         dbg!("Started initing");

//         // let mut table: Vec<[Option<(K, V)>; BS]> = vec![[None; BS]; outer_size];
//         // This uglier initialization is necessary because V isn't Copy so Option<(K, V)> isn't either
//         let mut table = Vec::<Vec<Option<(K, V)>>>::new();
//         table.resize_with(outer_size, || Vec::with_capacity(2));

//         dbg!("Ended initing");

//         let hashers = {
//             let mut ctr = 0;
//             (0..HS).map(|_| {
//                 ctr += 1;
//                 SHF::new(ctr)
//             }).collect::<Vec<_>>()
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

//         Self { table, hashers, BS, HS }
//     }
// }

// Tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_static_cuckoo_table() {
        let inputs = (0..1000u64).map(|x| (x, x * 2)).collect::<Vec<_>>();

        let table = StaticCuckooTable::<2, 2, u64, u64>::from_iter(inputs.clone().into_iter(), 1.5);

        for (k, v) in inputs {
            assert_eq!(table.lookup(&k), Some(&v));
        }
    }
}
