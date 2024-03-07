import { lookup_key, IkvblobReader } from './pkg';
import {open} from 'node:fs/promises';

let bytes = new Uint8Array([
    31,
    190,
    200,
    20,
    177,
    139,
    29,
    76,
    62,
    170,
    124,
    236,
    65,
    0,
    126,
    4,
    191,
    10,
    152,
    69,
    59,
    6,
    236,
    117,
    130,
    170,
    41,
    136,
    44,
    82,
    235,
    126,
]);

let handle = await open("../test.ikvblob", "r");

/**
 * 
 * @param {BigInt} start_bi
 * @param {BigInt} end_bi
 * @returns 
 */
async function read_cb(start_bi, end_bi) {
    // console.log(start, end, typeof start, typeof end);
    let start = Number(start_bi);
    let end = Number(end_bi);
    console.log(start, end, typeof start, typeof end);
    let buffer = new Uint8Array(end - start);
    let res = await handle.read(buffer, 0, end - start, start);
    console.log(buffer);
    return buffer;
}

let length_cb = async () => {
    let stats = await handle.stat();
    return stats.size;
}


let manager = await IkvblobReader.new(read_cb, length_cb);
let result = await manager.lookup_key(bytes);
console.log(result);



// console.log(await lookup_key(bytes, read_cb, length_cb));
