'use strict';
/**
 * Slot math and table integrity. No Redis required.
 *
 * Our CRC16 and hash-tag parsing are checked against two independent references:
 * a direct transcription of Redis's own `keyHashSlot()` from cluster.c, and the
 * `cluster-key-slot` package that ioredis uses for routing.
 *
 * Those two references do NOT always agree. cluster-key-slot mishandles empty
 * hash tags: given `{}`, Redis hashes the whole key, while cluster-key-slot
 * keeps scanning for a later `}`. Verified against a live server's CLUSTER
 * KEYSLOT (scripts/probe-hashtag.js): on 3000 random brace-heavy keys, ours
 * disagreed with Redis 0 times and cluster-key-slot disagreed 45 times.
 *
 * We follow Redis. Where the two references diverge, cluster-key-slot is
 * excluded and the C transcription is authoritative.
 */

const test = require('node:test');
const assert = require('node:assert');
const calculateSlot = require('cluster-key-slot');

const { crc16, hashTag, slotFor, NUM_SLOTS } = require('../src/slot');
const { TAGS, tagForSlot, verifyTable, digest, meta } = require('../src/table');

/**
 * Direct transcription of keyHashSlot() from Redis src/cluster.c.
 * Deliberately written in C style, not idiomatic JS, so it can be diffed
 * against the original by eye.
 * @param {string} key
 * @returns {number}
 */
function redisKeyHashSlot(key) {
  const keylen = key.length;
  let s, e;
  for (s = 0; s < keylen; s++) if (key[s] === '{') break;
  if (s === keylen) return crc16(key) % NUM_SLOTS;
  for (e = s + 1; e < keylen; e++) if (key[e] === '}') break;
  if (e === keylen || e === s + 1) return crc16(key) % NUM_SLOTS;
  return crc16(key.slice(s + 1, e)) % NUM_SLOTS;
}

/** True when a key hits the empty-tag case cluster-key-slot gets wrong. */
function hasEmptyTag(key) {
  const open = key.indexOf('{');
  return open !== -1 && key[open + 1] === '}';
}

/** Keys chosen to exercise every branch of Redis's hash-tag rule. */
const EDGE_KEYS = [
  'foo', 'bar', 'hello', '', '{}', '{', '}', '{}foo', 'a{}b',
  'user:{123}', 'user:{123}:profile', '{a}{b}', 'a{b{c}d', 'a{b}c}d',
  '{{a}}', '}{a}', 'x'.repeat(500), 'user:{}:1', ':', '::', '{ }',
  'canceled:{1234567890}', 'a{b}', '{a}b', 'тест', 'emoji:{🔥}',
  'subs:{1234567890}', 'active:{1405}',
];

function randomKeys(n, seed = 1234) {
  // Deterministic LCG so failures are reproducible.
  let s = seed;
  const rnd = () => (s = (s * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff;
  const alphabet = 'abc{}:0189_-';
  const out = [];
  for (let i = 0; i < n; i++) {
    const len = Math.floor(rnd() * 24);
    let k = '';
    for (let j = 0; j < len; j++) k += alphabet[Math.floor(rnd() * alphabet.length)];
    out.push(k);
  }
  return out;
}

test('crc16 matches the published XMODEM check value', () => {
  assert.strictEqual(crc16('123456789'), 0x31c3);
});

test('hash slots match published Redis vectors', () => {
  assert.strictEqual(slotFor('foo'), 12182);
  assert.strictEqual(slotFor('bar'), 5061);
  assert.strictEqual(slotFor('hello'), 866);
  assert.strictEqual(slotFor(''), 0);
});

test('hashTag follows Redis brace rules', () => {
  assert.strictEqual(hashTag('user:{123}:profile'), '123');
  assert.strictEqual(hashTag('no braces'), 'no braces');
  assert.strictEqual(hashTag('{}'), '{}', 'empty tag hashes the whole key');
  assert.strictEqual(hashTag('a{b'), 'a{b', 'unterminated brace hashes whole key');
  assert.strictEqual(hashTag('a{b{c}d'), 'b{c', 'first close after first open');
  assert.strictEqual(hashTag('}{a}'), 'a');
});

test('slotFor matches Redis keyHashSlot() on edge cases', () => {
  for (const k of EDGE_KEYS) {
    assert.strictEqual(slotFor(k), redisKeyHashSlot(k), `mismatch on ${JSON.stringify(k)}`);
  }
});

test('slotFor matches Redis keyHashSlot() on 20k random keys', () => {
  for (const k of randomKeys(20000)) {
    assert.strictEqual(slotFor(k), redisKeyHashSlot(k), `mismatch on ${JSON.stringify(k)}`);
  }
});

test('slotFor agrees with cluster-key-slot except on empty hash tags', () => {
  let diverged = 0;
  for (const k of randomKeys(20000)) {
    if (slotFor(k) === calculateSlot(k)) continue;
    diverged++;
    assert.ok(
      hasEmptyTag(k),
      `unexpected divergence from cluster-key-slot on ${JSON.stringify(k)} ` +
        `(not an empty-tag key, so this is a real bug in our implementation)`
    );
  }
  // Sanity: the corpus must actually exercise the divergent case, or this
  // test would pass vacuously if our implementation silently changed.
  assert.ok(diverged > 0, 'corpus should contain empty-tag keys');
});

test('empty hash tag hashes the whole key, per Redis', () => {
  // The specific shape cluster-key-slot gets wrong. Values confirmed against a
  // live server with CLUSTER KEYSLOT.
  assert.strictEqual(slotFor('{}a}b'), 5168);
  assert.strictEqual(slotFor('a{}b}c'), 2041);
  assert.strictEqual(slotFor(':{}1:-0c9--0}_9c8-{b'), 6535);
  for (const k of ['{}a}b', 'a{}b}c', ':{}1:-0c9--0}_9c8-{b']) {
    assert.strictEqual(hashTag(k), k, 'empty tag should fall back to whole key');
  }
});

test('table has one tag per slot, all distinct', () => {
  assert.strictEqual(TAGS.length, NUM_SLOTS);
  assert.strictEqual(new Set(TAGS).size, NUM_SLOTS);
});

test('every tag round-trips to its own slot', () => {
  for (let s = 0; s < NUM_SLOTS; s++) {
    assert.strictEqual(slotFor(TAGS[s]), s, `tag ${TAGS[s]} should be slot ${s}`);
    // Tags are plain integers with no braces, so both references must agree.
    assert.strictEqual(calculateSlot(TAGS[s]), s, `tag ${TAGS[s]} per cluster-key-slot`);
    assert.strictEqual(redisKeyHashSlot(TAGS[s]), s, `tag ${TAGS[s]} per keyHashSlot()`);
  }
});

test('keyFor output is co-slotted with its source key', () => {
  const { IntraSlot } = require('../src/index');
  const fan = Object.create(IntraSlot.prototype);
  fan.name = 'active';
  fan.keys = TAGS.map((t) => `active:{${t}}`);
  for (const k of [...EDGE_KEYS, ...randomKeys(5000, 77)]) {
    assert.strictEqual(
      slotFor(fan.keyFor(k)),
      slotFor(k),
      `keyFor(${JSON.stringify(k)}) not co-slotted`
    );
  }
});

test('shipped digest matches the shipped tags', () => {
  assert.strictEqual(digest(), meta.sha256);
  assert.doesNotThrow(() => verifyTable({ deep: true }));
});

test('tagForSlot rejects out-of-range slots', () => {
  assert.throws(() => tagForSlot(-1), RangeError);
  assert.throws(() => tagForSlot(NUM_SLOTS), RangeError);
  assert.strictEqual(tagForSlot(0), TAGS[0]);
});

test('table is the minimal table: no smaller tag exists for any slot', () => {
  // Walk the integers once and confirm the first hit for each slot is what we shipped.
  const firstSeen = new Map();
  for (let n = 1; firstSeen.size < NUM_SLOTS; n++) {
    const s = calculateSlot(String(n));
    if (!firstSeen.has(s)) firstSeen.set(s, String(n));
  }
  for (let s = 0; s < NUM_SLOTS; s++) {
    assert.strictEqual(TAGS[s], firstSeen.get(s), `slot ${s} is not minimal`);
  }
});
