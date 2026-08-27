'use strict';
/**
 * Regenerate data/slot-table.json.
 *
 * You should essentially never need to run this. The table is a pure function of
 * the Redis Cluster spec -- CRC16-XMODEM and the fixed 16384 slot count -- both
 * of which are frozen. It is committed precisely so it cannot drift. This script
 * exists so the table is reproducible rather than magic.
 *
 *   node scripts/gen-slot-table.js [--check]
 *
 * --check regenerates in memory and diffs against the committed file without
 * writing, which is what CI should run.
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { NUM_SLOTS, crc16, slotFor } = require('../src/slot');

const OUT = path.join(__dirname, '..', 'data', 'slot-table.json');
const check = process.argv.includes('--check');

// Guard the primitive before trusting it to build 16384 entries.
if (crc16('123456789') !== 0x31c3) {
  throw new Error(`crc16 self-test failed: got 0x${crc16('123456789').toString(16)}`);
}
for (const [key, want] of [['foo', 12182], ['bar', 5061], ['hello', 866], ['', 0]]) {
  if (slotFor(key) !== want) {
    throw new Error(`slot vector failed: ${JSON.stringify(key)} -> ${slotFor(key)}, want ${want}`);
  }
}

/** @type {string[]} */
const tags = new Array(NUM_SLOTS);
let found = 0;
let n = 0;
while (found < NUM_SLOTS) {
  n++;
  const s = slotFor(String(n));
  if (tags[s] === undefined) {
    tags[s] = String(n);
    found++;
  }
}

// Invariants: total, unique, and every tag round-trips.
if (tags.some((t) => t === undefined)) throw new Error('coverage gap');
if (new Set(tags).size !== NUM_SLOTS) throw new Error('duplicate tag');
for (let s = 0; s < NUM_SLOTS; s++) {
  if (slotFor(tags[s]) !== s) throw new Error(`tag ${tags[s]} does not map to slot ${s}`);
}

const sha256 = crypto.createHash('sha256').update(tags.join('\n')).digest('hex');
const payload = { algorithm: 'crc16-xmodem', num_slots: NUM_SLOTS, sha256, tags };
const json = JSON.stringify(payload);

const lengths = {};
for (const t of tags) lengths[t.length] = (lengths[t.length] || 0) + 1;

console.log(`slots covered   : ${found} / ${NUM_SLOTS}`);
console.log(`integers scanned: ${n}`);
console.log(`largest tag     : ${Math.max(...tags.map(Number))}`);
console.log(`tag length dist : ${JSON.stringify(lengths)}`);
console.log(`sha256          : ${sha256}`);

if (check) {
  const existing = fs.readFileSync(OUT, 'utf8');
  if (existing !== json) {
    console.error('\nFAIL: committed data/slot-table.json differs from regenerated output');
    process.exit(1);
  }
  console.log('\ncommitted table matches regenerated output');
} else {
  fs.writeFileSync(OUT, json);
  console.log(`\nwrote ${path.relative(process.cwd(), OUT)}`);
}
