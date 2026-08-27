'use strict';
/**
 * Write-amplification benchmark: per-event fanout vs coalesced delta ticks.
 *
 *   npm run bench
 *
 * The question is whether a high-churn presence set is viable under full
 * 16384-slot fanout. It is, provided you never fan out per event.
 */

const Redis = require('ioredis');
const { IntraSlot, NUM_SLOTS } = require('../src/index');

const ms = (t0) => Number(process.hrtime.bigint() - t0) / 1e6;

(async () => {
  const cluster = new Redis.Cluster([{ host: '127.0.0.1', port: 7001 }]);
  await new Promise((r) => cluster.once('ready', r));

  const fan = new IntraSlot(cluster, { name: 'bench' });
  const nodes = cluster.nodes('master').length;
  console.log(`cluster: ${nodes} masters, ${NUM_SLOTS} replicas per set\n`);

  // --- initial full publish ------------------------------------------------
  const seed = Array.from({ length: 200 }, (_, i) => `user${i}`);
  let t0 = process.hrtime.bigint();
  await fan.publish(seed);
  const seedMs = ms(t0);
  console.log(`initial publish (200 members -> ${NUM_SLOTS} replicas): ` +
              `${seedMs.toFixed(0)} ms  (${(NUM_SLOTS / seedMs * 1000).toFixed(0)} keys/s)`);

  // --- coalesced delta ticks ----------------------------------------------
  console.log('\ncoalesced delta ticks:');
  const lat = [];
  let pool = [...seed];
  for (let tick = 0; tick < 5; tick++) {
    const added = Array.from({ length: 6 }, (_, i) => `new${tick}_${i}`);
    const removed = pool.slice(0, 4);
    pool = pool.slice(4).concat(added);

    t0 = process.hrtime.bigint();
    await fan.applyDelta({ add: added, remove: removed });
    const dt = ms(t0);
    lat.push(dt);
    console.log(`  tick ${tick}: +${added.length}/-${removed.length} over ${NUM_SLOTS} ` +
                `replicas -> ${dt.toFixed(0)} ms ` +
                `(${(NUM_SLOTS * 2 / dt * 1000).toFixed(0)} cmds/s)`);
  }
  const avg = lat.reduce((a, b) => a + b) / lat.length;
  const hz = 1000 / avg;
  console.log(`\n  mean tick: ${avg.toFixed(0)} ms => sustainable ~${hz.toFixed(1)} Hz`);

  // --- convergence check ---------------------------------------------------
  const report = await fan.verify({ sample: 256 });
  console.log(`  verify: ${report.consistent ? 'CONSISTENT' : 'DIVERGED'} ` +
              `(cardinalities ${JSON.stringify(report.cardinalities)})`);

  // --- the comparison that matters -----------------------------------------
  const coalesced = Math.round((NUM_SLOTS * 2) / avg * 1000);
  console.log('\n' + '='.repeat(74));
  console.log('cost for a presence set churning at R events/sec:');
  console.log(`  per-event : R x ${NUM_SLOTS} cmd/s      -- scales with churn`);
  console.log(`  coalesced : ${coalesced.toLocaleString()} cmd/s  -- INDEPENDENT of churn`);
  for (const r of [10, 50, 200, 1000]) {
    console.log(`    R=${String(r).padEnd(5)} -> per-event ${(r * NUM_SLOTS).toLocaleString().padStart(12)} cmd/s ` +
                `| coalesced ${coalesced.toLocaleString().padStart(9)} cmd/s`);
  }
  console.log('='.repeat(74));

  await fan.clear();
  await cluster.quit();
})().catch((e) => { console.error(e); process.exit(1); });
