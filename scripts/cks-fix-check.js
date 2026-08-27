'use strict';
/**
 * Candidate fix for cluster-key-slot's empty-hash-tag bug, validated against a
 * live Redis via CLUSTER KEYSLOT.
 *
 * Bug: Redis's keyHashSlot() treats an empty tag `{}` as "no tag" and hashes the
 * whole key, stopping the search. cluster-key-slot's single-pass scanner does
 * not return in that case but also does not clear `start`, so it keeps scanning
 * and latches onto the NEXT `}`, hashing whatever sits between.
 *
 *   "a{}b}c"  Redis: hash("a{}b}c") = 2041     cluster-key-slot: hash("b") = 3300
 *
 * Fix: lock in the whole-key fallback when the empty-tag case is seen.
 * One extra boolean, no extra pass, no change to the hot path for normal keys.
 */
const Redis = require('ioredis');
const upstream = require('cluster-key-slot');
const lookup = require('cluster-key-slot/lib/index.js'); // for parity of table
const { slotFor } = require('../src/slot');

// --- verbatim copy of upstream, with the two-line fix marked ---------------
const CRC_TABLE = (() => {
  const t = new Uint16Array(256);
  for (let i = 0; i < 256; i++) {
    let crc = i << 8;
    for (let b = 0; b < 8; b++) crc = crc & 0x8000 ? ((crc << 1) ^ 0x1021) & 0xffff : (crc << 1) & 0xffff;
    t[i] = crc;
  }
  return t;
})();

function toUTF8Array(str) {
  return Array.from(Buffer.from(str, 'utf8'));
}

function generateFixed(str) {
  let char;
  let i = 0;
  let start = -1;
  let done = false;                        // <-- FIX: whole-key fallback latched
  let result = 0;
  let resultHash = 0;
  const utf8 = typeof str === 'string' ? toUTF8Array(str) : str;
  const len = utf8.length;

  while (i < len) {
    char = utf8[i++];
    if (done || start === -1) {            // <-- FIX: skip tag scan once latched
      if (!done && char === 0x7b) {
        start = i;
      }
    } else if (char !== 0x7d) {
      resultHash = CRC_TABLE[(char ^ (resultHash >> 8)) & 0xff] ^ ((resultHash << 8) & 0xffff);
    } else if (i - 1 !== start) {
      return resultHash & 0x3fff;
    } else {
      done = true;                         // <-- FIX: empty tag => hash whole key
    }

    result = CRC_TABLE[(char ^ (result >> 8)) & 0xff] ^ ((result << 8) & 0xffff);
  }

  return result & 0x3fff;
}

// --- validate all three against the server --------------------------------
(async () => {
  const r = new Redis({ host: '127.0.0.1', port: 7001 });

  const NAMED = [
    'a{}b}c', '{}a}b', ':{}1:-0c9--0}_9c8-{b', '{}', '{}foo', 'user:{}:1',
    'a{b}c', '{a}', 'foo', 'a{b{c}d', '}{a}', '{{a}}', 'x{}}y', '{}{a}',
    'тест{}ы}z', '{}🔥}a',
  ];

  let s = 424242;
  const rnd = () => (s = (s * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff;
  const alphabet = 'ab{}:9-';
  const corpus = [...NAMED];
  for (let i = 0; i < 8000; i++) {
    let k = '';
    const len = Math.floor(rnd() * 20);
    for (let j = 0; j < len; j++) k += alphabet[Math.floor(rnd() * alphabet.length)];
    corpus.push(k);
  }

  let badUpstream = 0, badFixed = 0, badOurs = 0;
  const examples = [];
  for (const k of corpus) {
    const truth = await r.cluster('KEYSLOT', k);
    const u = upstream(k);
    const f = generateFixed(k);
    const o = slotFor(k);
    if (u !== truth) {
      badUpstream++;
      if (examples.length < 6) examples.push({ k, truth, u, f });
    }
    if (f !== truth) badFixed++;
    if (o !== truth) badOurs++;
  }

  console.log(`corpus: ${corpus.length} keys, checked against live CLUSTER KEYSLOT\n`);
  console.log(`  cluster-key-slot@${require('cluster-key-slot/package.json').version} wrong : ${badUpstream}`);
  console.log(`  patched generate()                wrong : ${badFixed}`);
  console.log(`  intraslot src/slot.js             wrong : ${badOurs}`);

  console.log('\nrepresentative failures (upstream vs redis, and what the patch gives):');
  for (const e of examples) {
    console.log(`  ${JSON.stringify(e.k).padEnd(26)} redis=${String(e.truth).padStart(5)}  ` +
                `upstream=${String(e.u).padStart(5)}  patched=${String(e.f).padStart(5)}`);
  }

  // --- downstream impact via generateMulti, which ioredis uses to decide
  //     whether a multi-key command is legal. It fails in BOTH directions.
  const { TAGS } = require('../src/table');
  console.log(`\ngenerateMulti impact (ioredis gates multi-key commands on this):`);

  // (1) FALSE ACCEPT: upstream says same slot, Redis disagrees.
  //     ioredis builds the command; the server rejects it with CROSSSLOT.
  const fa = ['x{}}y', 'z{}}w'];
  const faTruth = await Promise.all(fa.map((k) => r.cluster('KEYSLOT', k)));
  console.log(`  false accept:`);
  console.log(`    redis    ${fa[0]} -> ${faTruth[0]},  ${fa[1]} -> ${faTruth[1]}  ` +
              `(same slot: ${faTruth[0] === faTruth[1]})`);
  console.log(`    upstream generateMulti = ${upstream.generateMulti(fa)} ` +
              `(not -1 => allowed through, server will reject)`);

  // (2) FALSE REJECT: Redis says same slot, upstream disagrees.
  //     ioredis refuses a command the server would have accepted.
  const emptyTagKey = 'a{}b}c';                    // redis hashes the whole key
  const truth = await r.cluster('KEYSLOT', emptyTagKey);
  const plainPeer = TAGS[truth];                    // plain key in that same slot
  const peerTruth = await r.cluster('KEYSLOT', plainPeer);
  console.log(`  false reject:`);
  console.log(`    redis    ${emptyTagKey} -> ${truth},  ${plainPeer} -> ${peerTruth}  ` +
              `(same slot: ${truth === peerTruth})`);
  console.log(`    upstream generateMulti = ${upstream.generateMulti([emptyTagKey, plainPeer])} ` +
              `(-1 => wrongly refused)`);
  console.log(`    patched  would give    = ` +
              `${generateFixed(emptyTagKey) === generateFixed(plainPeer) ? generateFixed(emptyTagKey) : -1}`);

  await r.quit();
  process.exit(badFixed === 0 ? 0 : 1);
})().catch((e) => { console.error(e); process.exit(1); });
