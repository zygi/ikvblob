// import {readFile} from 'node:fs/promises';

const fs = require('fs/promises');

async function readBinFile(filePath) {
    const data = await fs.readFile(filePath);
    return new Uint8Array(data);
}

module.exports = { readBinFile };