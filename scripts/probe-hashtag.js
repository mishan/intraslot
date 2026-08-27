'use strict';
/**
 * Ground-truth check: ours vs cluster-key-slot vs Redis's own CLUSTER KEYSLOT.
 *
 * When two implementations disagree, the server is the only vote that counts.
 * Requires a cluster (or any cluster-enabled node) on 127.0.0.1:7001.
 */
const Redis = require('ioredis');
const calculateSlot = require('cluster-key-slot');
const { slotFor, hashTag } = require('../src/slot');

const CASES = [
  ':{}1:-0c9--0}_9c8-{b',   // the disagreement the test surfaced
  '{}',
  '{}foo',
  'a{}b',
  '{}a}b',
  'a{}b}c',
  'user:{}:1',
  '{ }',
  'a{b{c}d',
  'a{b}c}d',
  '}{a}',
  '{{a}}',
  '{',
  '}',
  'foo',
];

(async () => {
  const r = new Redis({ host: '127.0.0.1', port: 7001 });
  let disagreements = 0;

  console.log(
    'key'.padEnd(24) + 'ours'.padStart(7) + 'ckslot'.padStart(8) +
    'REDIS'.padStart(8) + '  ours-tag'
  );
  console.log('-'.repeat(70));

  for (const k of CASES) {
    const ours = slotFor(k);
    const cks = calculateSlot(k);
    const redis = await r.cluster('KEYSLOT', k);
    const flag = ours === redis ? (cks === redis ? '' : '  <- ckslot wrong')
                                : '  <- OURS WRONG';
    if (ours !== redis || cks !== redis) disagreements++;
    console.log(
      JSON.stringify(k).padEnd(24) +
        String(ours).padStart(7) + String(cks).padStart(8) +
        String(redis).padStart(8) + '  ' + JSON.stringify(hashTag(k)) + flag
    );
  }

  // Broad random sweep against the server.
  let s = 99;
  const rnd = () => (s = (s * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff;
  const alphabet = 'abc{}:0189_-';
  let oursBad = 0, cksBad = 0, checked = 0;
  for (let i = 0; i < 3000; i++) {
    let k = '';
    const len = Math.floor(rnd() * 24);
    for (let j = 0; j < len; j++) k += alphabet[Math.floor(rnd() * alphabet.length)];
    const redis = await r.cluster('KEYSLOT', k);
    if (slotFor(k) !== redis) oursBad++;
    if (calculateSlot(k) !== redis) cksBad++;
    checked++;
  }
  console.log(`\nrandom sweep vs server: ${checked} keys`);
  console.log(`  ours   disagreed with Redis: ${oursBad}`);
  console.log(`  ckslot disagreed with Redis: ${cksBad}`);

  await r.quit();
  process.exit(disagreements && oursBad ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
