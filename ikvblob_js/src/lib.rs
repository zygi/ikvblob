#![allow(dead_code)]

use js_sys::Promise;
use ikvblob::{fileformat_read::IkvblobView, memory_view::Memory, multihash::Multihash};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(typescript_type = "(start: BigInt, end: BigInt) => Promise<Uint8Array>")]
    pub type ReadCallbackType;

    #[wasm_bindgen(typescript_type = "() => Promise<BigInt>")]
    pub type LenCallbackType;

    #[wasm_bindgen(typescript_type = "Uint8Array")]
    pub type Uint8ArrayType;
}

struct JsCallbackMemory {
    read_callback: js_sys::Function,
    len_callback: js_sys::Function,
}

impl JsCallbackMemory {
    fn new(read_callback: js_sys::Function, len_callback: js_sys::Function) -> Self {
        JsCallbackMemory {
            read_callback,
            len_callback,
        }
    }
}

impl Memory for JsCallbackMemory {
    async fn read_slice(&self, range: std::ops::Range<usize>) -> Vec<u8> {
        let this = JsValue::null();
        let start = range.start as i64;
        let end = range.end as i64;
        let promise_jsvalue = self
            .read_callback
            .call2(&this, &JsValue::from(start), &JsValue::from(end))
            .unwrap();
        let result = wasm_bindgen_futures::JsFuture::from(Promise::from(promise_jsvalue))
            .await
            .unwrap();
        let bytes = js_sys::Uint8Array::new(&result).to_vec();
        bytes
    }

    async fn len(&self) -> usize {
        let this = JsValue::null();
        let promise_jsvalue = self.len_callback.call0(&this).unwrap();
        let result = wasm_bindgen_futures::JsFuture::from(Promise::from(promise_jsvalue))
            .await
            .unwrap();
        let sz = result.as_f64().map(|f| f as usize).unwrap();
        sz
    }
}

#[wasm_bindgen(js_name = IkvblobReader)]
struct IkvblobReader {
    view: IkvblobView<'static, JsCallbackMemory, Multihash<32>, (u64, u32)>,
}

#[wasm_bindgen(js_class = IkvblobReader)]
impl IkvblobReader {
    pub async fn new(
        read_callback: ReadCallbackType,
        len_callback: LenCallbackType,
    ) -> Result<IkvblobReader, JsError> {
        let read_cb = JsValue::from(read_callback);
        let len_cb = JsValue::from(len_callback);
        if read_cb.is_undefined() || len_cb.is_undefined() {
            return Err(JsError::new("read_callback or len_callback is undefined"));
        }
        if !read_cb.is_function() || !len_cb.is_function() {
            return Err(JsError::new(
                "read_callback or len_callback is not a function",
            ));
        }

        let mem = JsCallbackMemory::new(read_cb.into(), len_cb.into());
        let view = IkvblobView::<_, Multihash<32>, (u64, u32)>::wrap(mem).await
            .map_err(|e| JsError::new(&format!("Error: {}", e)))?;
        Ok(IkvblobReader { view })
    }

    pub async fn lookup_key(&self, key: &[u8]) -> Result<Option<Uint8ArrayType>, JsError> {
        let hash = Multihash::wrap(1, key.try_into().unwrap());
        let res = self.view.lookup(&hash).await;
        let res_vec = res.map_err(|e| JsError::new(&format!("Error: {}", e)))?;
        match res_vec {
            Some(res_vec) => {
                let arr = js_sys::Uint8Array::from(res_vec.as_ref());
                Ok(Some(JsValue::from(arr).into()))
            }
            None => Ok(None)
        }
    }
}

// Implement custom error

// #[derive(Debug)]
// enum ViewkvJsError {
//     JsError(JsError),
//     OtherError(String)
// }

// impl From<JsError> for ViewkvJsError {
//     fn from(e: JsError) -> Self {
//         ViewkvJsError::JsError(e)
//     }
// }
// impl std::fmt::Display for ViewkvJsError {
//     fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
//         match self {
//             ViewkvJsError::JsError(e) => write!(f, "JsError: {:?}", e),
//             ViewkvJsError::OtherError(e) => write!(f, "OtherError: {}", e)
//         }
//     }
// }
// impl Into<JsError> for ViewkvJsError {
//     fn into(self) -> JsError {
//         match self {
//             ViewkvJsError::JsError(e) => e,
//             ViewkvJsError::OtherError(e) => JsError::from_str(&e)
//         }
//     }
// }
// impl std::error::Error for ViewkvJsError {}


// #[wasm_bindgen]
// extern "C" {
//     // Use `js_namespace` here to bind `console.log(..)` instead of just
//     // `log(..)`
//     #[wasm_bindgen(js_namespace = console)]
//     fn log(s: &str);

//     #[wasm_bindgen(js_namespace = console, js_name = log)]
//     fn log_s(s: String);

//     // The `console.log` is quite polymorphic, so we can bind it with multiple
//     // signatures. Note that we need to use `js_name` to ensure we always call
//     // `log` in JS.
//     #[wasm_bindgen(js_namespace = console, js_name = log)]
//     fn log_u32(a: u32);

// }


// use wasm_bindgen::prelude::*;
// use wasm_bindgen_futures::JsFuture;
// use wasm_bindgen_test::wasm_bindgen_test;

// #[wasm_bindgen(module = "/js/read_bin_file.cjs")]
// extern "C" {
//     fn readBinFile(filename: &str) -> js_sys::Promise;
// }

// #[wasm_bindgen_test]
// async fn my_async_test() {
//     wasm_logger::init(wasm_logger::Config::default());
//     // let file_contents = fs::read("../test.ikvblob").unwrap();

//     let data = readBinFile("../test.ikvblob");
//     let file_contents_jsvalue = JsFuture::from(data).await.unwrap();
//     let file_contents = js_sys::Uint8Array::new(&file_contents_jsvalue).to_vec();

//     let ikvblob = IkvblobView::wrap(file_contents).await.unwrap();

//     let key = Multihash::wrap(1, [
//         31,
//         190,
//         200,
//         20,
//         177,
//         139,
//         29,
//         76,
//         62,
//         170,
//         124,
//         236,
//         65,
//         0,
//         126,
//         4,
//         191,
//         10,
//         152,
//         69,
//         59,
//         6,
//         236,
//         117,
//         130,
//         170,
//         41,
//         136,
//         44,
//         82,
//         235,
//         126,
//     ]);

//     let res = ikvblob.lookup(&key).await.unwrap().unwrap();

//     for i in res {
//         log_u32(i as u32);
//     }

//     // log_u32(ikvblob.header.cuckoo_table_num_hashers as u32);

//     // log(ikvblob.type_id().hash(state));

//     // dbg!(ikvblob.type_id());

//     // Convert that promise into a future and make the test wait on it.
//     // let x = JsFuture::from(promise).await.unwrap();
//     // assert_eq!(x, 42);
// }

// type HASH = SipHasher;
// // type HASH = hashers::jenkins::OAATHasher;

// #[wasm_bindgen_test]
// fn siphasher_test() {
//     // let sh = SipHasherFactory::new(1);
//     // let mut sh = SHA;
//     // let mut sh = SipHasher::new_with_keys(1, 0);
//     // let mut sh = hashers::fx_hash::FxHasher::default();
//     let mut sh = HASH::default();
//     b"abcd".hash(&mut sh);
//     let res = sh.finish();
//     let le_bytes = res.to_le_bytes();
//     // let res_str = format!("{:x}", res);
//     for b in le_bytes {
//         log_u32(b as u32);
//     }

//     // assert!(false);
// }

// #[test]
// fn siphasher_test2() {
//     // let sh = SipHasherFactory::new(1);
//     // let mut sh = DefaultHasher::new();
//     // let mut sh = SipHasher::new_with_keys(1, 0);
//     let mut sh = HASH::default();
//     b"abcd".hash(&mut sh);
//     let res = sh.finish();
//     let le_bytes = res.to_le_bytes();
//     for b in le_bytes {
//         println!("{}", b as u32);
//     }
//     // let res_str = format!("{:x}", res);
//     // dbg!(res_str);
// }

// #[wasm_bindgen]
// pub async fn lookup_key(key: &[u8], _lookup_fn: &js_sys::Function
// ) -> Result<JsValue, JsValue> {
//     let hashers = {
//         let mut ctr = 0;
//         [(); HS].map(|_| {
//             ctr += 1;
//             SipHasherFactory::new(ctr)
//         })
//     };

//     let wrapped_fn = async move |_i: usize| {
//         // let this = JsValue::null();
//         // let promise_jsvalue = lookup_fn.call1(&this, &JsValue::from(i)).unwrap();
//         // let result = wasm_bindgen_futures::JsFuture::from(Promise::from(promise_jsvalue)).await.unwrap();
//         // let bytes = js_sys::Uint8Array::new(&result).to_vec();
//         let parsed = [None; 2];
//         // let parsed = rkyv::from_bytes::<[Option<([u8; 16], (u64, u32))>; 2]>(&bytes).unwrap();
//         parsed
//     };

//     let _decompressor = zstd::bulk::decompress(key,32).unwrap();

//     let keyt = TryInto::<[u8; 16]>::try_into(_decompressor.as_ref()).map_err(|e| JsError::new(&format!("Error converting key to [u8; 16]: {}", e)))?;
//     let res = bucket_fn_based_lookup::<[u8; 16], (u64, u32), 2, 2, _, _>(
//         wrapped_fn,
//         63281128,
//         &keyt,
//         &hashers
//     ).await;

//     match res {
//         Some((a, b)) => {
//             // do it the dumb way
//             // let val = JsValue::E
//             let ab = js_sys::Array::new();
//             ab.push(&JsValue::from(a));
//             ab.push(&JsValue::from(b));
//             Ok(ab.into())
//             // Ok(vec![a, b as u64].into_boxed_slice())

//         },
//         None => Err(JsError::new("Key not found").into())
//     }
// }
