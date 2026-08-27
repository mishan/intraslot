'use strict';
/**
 * Probe: how does ioredis split a 16384-command cluster pipeline?
 *
 * The publish path fans out to all 16384 slots. Whether that is one pipeline
 * per node (fast) or something closer to serial round trips (unusable) decides
 * the whole design, so measure it rather than assume.
 */
const Redis = require('ioredis');
const calculateSlot = require('cluster-key-slot');
const table = require('../data/slot-table.json');

const NUM_SLOTS = 16384;
const KEYS = table.tags.map((t) => `probe:{${t}}`);

function ms(t0) {
  return (Number(process.hrtime.bigint() - t0) / 1e6).toFixed(1);
}

(async () => {
  const cluster = new Redis.Cluster([{ host: '127.0.0.1', port: 7001 }]);
  await new Promise((r) => cluster.once('ready', r));

  // --- what does ioredis expose about slot ownership? ---------------------
  const masters = cluster.nodes('master');
  console.log(`masters: ${masters.map((n) => `${n.options.host}:${n.options.port}`).join(', ')}`);
  console.log(`cluster.slots is ${Array.isArray(cluster.slots) ? `an array of ${cluster.slots.length}` : typeof cluster.slots}`);
  if (Array.isArray(cluster.slots)) {
    console.log(`  slot 0 -> ${JSON.stringify(cluster.slots[0])}`);
    console.log(`  slot 8283 -> ${JSON.stringify(cluster.slots[8283])}`);
  }

  // --- A: one cluster-wide pipeline ---------------------------------------
  let t0 = process.hrtime.bigint();
  let tA = null;
  try {
    const p = cluster.pipeline();
    for (const k of KEYS) p.sadd(k, 'a', 'b', 'c');
    const resA = await p.exec();
    tA = ms(t0);
    console.log(`\nA. cluster.pipeline(), ${NUM_SLOTS} SADD : ${tA} ms ` +
                `(errors: ${resA.filter(([e]) => e).length})`);
  } catch (e) {
    console.log(`\nA. cluster.pipeline(), ${NUM_SLOTS} SADD : REJECTED -- ${e.message}`);
    console.log('   => ioredis will not span nodes in one pipeline; publish must');
    console.log('      group by owning node itself. This is the load-bearing');
    console.log('      difference from redis-py, which splits transparently.');
  }

  // --- B: manual per-node pipelines, issued concurrently ------------------
  const byNode = new Map();
  for (const k of KEYS) {
    const slot = calculateSlot(k);
    const owner = cluster.slots[slot][0]; // "host:port" of the master
    if (!byNode.has(owner)) byNode.set(owner, []);
    byNode.get(owner).push(k);
  }
  console.log(`   grouped: ${[...byNode].map(([n, v]) => `${n}=${v.length}`).join(', ')}`);

  t0 = process.hrtime.bigint();
  const results = await Promise.all(
    [...byNode.entries()].map(([addr, keys]) => {
      const [host, port] = addr.split(':');
      const node = masters.find(
        (n) => n.options.host === host && String(n.options.port) === port
      );
      const np = node.pipeline();
      for (const k of keys) np.sadd(k, 'a', 'b', 'c');
      return np.exec();
    })
  );
  const tB = ms(t0);
  const errB = results.flat().filter(([e]) => e).length;
  console.log(`B. per-node pipelines (parallel)      : ${tB} ms  (errors: ${errB})`);

  // --- C: no pipeline at all, for scale ------------------------------------
  t0 = process.hrtime.bigint();
  await Promise.all(KEYS.slice(0, 500).map((k) => cluster.sadd(k, 'a')));
  const tC = ms(t0);
  const extrapolated = (tC / 500) * NUM_SLOTS;
  console.log(`C. 500 individual awaited commands    : ${tC} ms  ` +
              `(=> ${extrapolated.toFixed(0)} ms extrapolated for 16384)`);

  console.log(`\nverdict: per-node pipelining is ${(extrapolated / tB).toFixed(0)}x faster ` +
              `than unpipelined commands.`);
  console.log(`         cluster.pipeline() ${tA ? `worked (${tA} ms)` : 'is unusable for fanout'}; ` +
              `the library groups by node itself.`);

  await cluster.quit();
})().catch((e) => { console.error(e); process.exit(1); });
