'use strict';
/**
 * Live-cluster tests. Requires a 3-node cluster on 127.0.0.1:7001-7003:
 *
 *   npm run test:cluster
 *
 * These exist because the central claim -- that a fanout key is legal in the
 * same command as the sharded key it was derived from -- can only be settled by
 * the server.
 */

const test = require('node:test');
const assert = require('node:assert');
const Redis = require('ioredis');

const { IntraSlot, CrossSlotError, slotFor } = require('../src/index');

const NODES = [{ host: '127.0.0.1', port: 7001 }];
const USER = '1234567890';
const SUBS = `subs:{${USER}}`;

/** @type {import('ioredis').Cluster} */
let cluster;

test.before(async () => {
  cluster = new Redis.Cluster(NODES);
  await new Promise((resolve, reject) => {
    cluster.once('ready', resolve);
    cluster.once('error', reject);
  });
});

test.after(async () => {
  if (cluster) await cluster.quit();
});

async function seedSubs() {
  await cluster.del(SUBS);
  await cluster.sadd(SUBS, 'alice', 'bob', 'carol', 'dave', 'erin');
}

test('sinter returns the active subset of a sharded set, server-side', async () => {
  const fan = new IntraSlot(cluster, { name: 'set1' });
  await seedSubs();
  await fan.publish(['bob', 'erin', 'frank', 'grace']);

  const result = await fan.sinter(SUBS);
  assert.deepStrictEqual(result.sort(), ['bob', 'erin']);
});

test('the fanout key is co-slotted, unlike a naive global key', async () => {
  const fan = new IntraSlot(cluster, { name: 'set2' });
  await seedSubs();
  await fan.publish(['bob']);

  const subsSlot = slotFor(SUBS);
  assert.strictEqual(slotFor(fan.keyFor(SUBS)), subsSlot, 'fanout key must share the slot');

  // A naive un-tagged key is rejected by the server even when it happens to
  // land on the same node -- Redis checks the slot, not the node.
  await cluster.del('naive_global');
  await cluster.sadd('naive_global', 'bob');
  await assert.rejects(
    () => cluster.sinter(SUBS, 'naive_global'),
    /CROSSSLOT/,
    'naive global key should be rejected'
  );
});

test('sinter works inside a Lua script', async () => {
  const fan = new IntraSlot(cluster, { name: 'set3' });
  await seedSubs();
  await fan.publish(['bob', 'erin']);

  const script = "return redis.call('SINTER', KEYS[1], KEYS[2])";
  const out = await cluster.eval(script, 2, SUBS, fan.keyFor(SUBS));
  assert.deepStrictEqual(out.sort(), ['bob', 'erin']);
});

test('count, without and intersectInto all accept the fanout key', async () => {
  const fan = new IntraSlot(cluster, { name: 'set4' });
  await seedSubs();
  await fan.publish(['bob', 'erin', 'zoe']);

  assert.strictEqual(await fan.count(SUBS), 2);
  assert.deepStrictEqual((await fan.without(SUBS)).sort(), ['alice', 'carol', 'dave']);
  assert.strictEqual(await fan.intersectInto(`out:{${USER}}`, SUBS), 2);
});

test('assertSameSlot throws a descriptive CrossSlotError', async () => {
  const fan = new IntraSlot(cluster, { name: 'set5' });
  assert.throws(
    () => fan.assertSameSlot([SUBS, 'subs:{other-id}']),
    (err) => {
      assert.ok(err instanceof CrossSlotError);
      assert.match(err.message, /same \*node\* is not sufficient/);
      return true;
    }
  );
});

test('publish replaces atomically per replica and reaches every slot', async () => {
  const fan = new IntraSlot(cluster, { name: 'set6' });
  const r1 = await fan.publish(['a', 'b', 'c']);
  assert.strictEqual(r1.failed, 0);
  assert.strictEqual(r1.replicas, 16384);

  const report = await fan.verify({ sample: 512 });
  assert.ok(report.consistent, `replicas diverged: ${JSON.stringify(report.cardinalities)}`);
  assert.strictEqual(report.majority, 3);

  // Replacing shrinks every replica; no staging keys should survive.
  await fan.publish(['x']);
  const after = await fan.verify({ sample: 512 });
  assert.strictEqual(after.majority, 1);
  const staging = await cluster.exists(`intraslot:staging:{${require('../src/table').TAGS[0]}}`);
  assert.strictEqual(staging, 0, 'staging keys must not be left behind');
});

test('add and remove fan out to every replica', async () => {
  const fan = new IntraSlot(cluster, { name: 'set7' });
  await fan.publish(['a']);
  await fan.add(['b', 'c']);
  await fan.remove(['a']);

  const report = await fan.verify({ sample: 256 });
  assert.ok(report.consistent);
  assert.strictEqual(report.majority, 2);

  // Readable from a key on any node.
  for (const u of ['1', '99999', 'zzz', 'user-x']) {
    const members = await fan.smembers(`subs:{${u}}`);
    assert.deepStrictEqual(members.sort(), ['b', 'c']);
  }
});

test('applyDelta orders removals before additions', async () => {
  const fan = new IntraSlot(cluster, { name: 'set8' });
  await fan.publish(['a', 'b']);
  // 'b' both leaves and rejoins in the same delta: it should remain present.
  await fan.applyDelta({ remove: ['a', 'b'], add: ['b', 'c'] });

  const members = await fan.smembers(SUBS);
  assert.deepStrictEqual(members.sort(), ['b', 'c']);
});

async function netInputBytes() {
  const infos = await Promise.all(cluster.nodes('master').map((n) => n.info('stats')));
  return infos.reduce((sum, s) => sum + Number(/total_net_input_bytes:(\d+)/.exec(s)[1]), 0);
}

test('a small delta sends each replica key about once, not a command per replica', async () => {
  const fan = new IntraSlot(cluster, { name: 'setE' });
  await fan.publish(['a']);

  const before = await netInputBytes();
  await fan.applyDelta({ add: ['b'], remove: ['a'] });
  const sent = (await netInputBytes()) - before;

  assert.deepStrictEqual(await fan.smembers(SUBS), ['b']);
  // A pipelined SREM + SADD per replica costs ~80 bytes each.
  assert.ok(sent < 16384 * 30, `sent ${sent} bytes for a +1/-1 delta`);
});

test('a replica the delta cannot write fails alone, the rest still converge', async () => {
  const fan = new IntraSlot(cluster, { name: 'setF' });
  await fan.publish(['a']);
  const victimSlot = 4242;
  await cluster.set(fan.keys[victimSlot], 'not a set');

  const result = await fan.applyDelta({ add: ['b'] });
  assert.deepStrictEqual(result.failedSlots, [victimSlot]);

  await cluster.del(fan.keys[victimSlot]);
  const report = await fan.verify({ sample: 0 });
  assert.deepStrictEqual(report.drifted, [victimSlot]);
  assert.strictEqual(report.majority, 2);
});

test('concurrent writes apply in the order they were issued', async () => {
  const fan = new IntraSlot(cluster, { name: 'setH' });
  await fan.publish(['keep', 'x']);
  // A wrong-typed replica sends its batch down the slower fallback path.
  const victimSlot = 4242;
  await cluster.set(fan.keys[victimSlot], 'not a set');

  await Promise.all([fan.add(['y']), fan.remove(['x', 'y'])]);

  await cluster.del(fan.keys[victimSlot]);
  const report = await fan.verify({ sample: 0 });
  assert.deepStrictEqual(report.drifted, [victimSlot]);
  assert.strictEqual(report.majority, 1);
});

test('a batch that errors out counts all its slots as failed', async () => {
  const fan = new IntraSlot(cluster, { name: 'setI' });
  await fan.publish(['a']);
  const node = cluster.nodes('master')[0];
  const addr = `${node.options.host}:${node.options.port}`;
  const owned = cluster.slots.filter((owners) => owners[0] === addr).length;

  const lost = async () => {
    throw new Error('Connection is closed.');
  };
  const pipeline = node.pipeline;
  node.eval = lost;
  node.pipeline = (...args) => Object.assign(pipeline.apply(node, args), { exec: lost });
  let result;
  try {
    result = await fan.applyDelta({ add: ['b'] });
  } finally {
    delete node.eval;
    delete node.pipeline;
  }
  assert.strictEqual(result.failed, owned);
});

async function rejectedEvals(c) {
  const infos = await Promise.all(c.nodes('master').map((n) => n.info('commandstats')));
  return infos.reduce((sum, s) => sum + Number((/cmdstat_eval:.*rejected_calls=(\d+)/.exec(s) || [0, 0])[1]), 0);
}

test('a server that refuses EVAL is not asked again', async () => {
  const masters = cluster.nodes('master');
  const acl = (...args) => Promise.all(masters.map((n) => n.call('ACL', ...args)));
  await acl('SETUSER', 'noeval', 'on', 'nopass', '~*', '&*', '+@all', '-eval');
  const limited = new Redis.Cluster(NODES, { redisOptions: { username: 'noeval' } });
  try {
    await new Promise((resolve, reject) => {
      limited.once('ready', resolve);
      limited.once('error', reject);
    });
    const fan = new IntraSlot(limited, { name: 'setJ' });
    await fan.publish(['a']);
    await fan.add(['b']);
    const before = await rejectedEvals(limited);
    assert.ok(before > 0, 'the first write should have tried EVAL');

    await fan.add(['c']);
    assert.strictEqual(await rejectedEvals(limited), before);
    assert.deepStrictEqual((await fan.smembers(SUBS)).sort(), ['a', 'b', 'c']);
  } finally {
    limited.disconnect();
    await acl('DELUSER', 'noeval');
  }
});

/** Migrate every key in `slot` from master `src` to `dst`; resolves to a function that hands over the slot. */
async function startSlotMove(slot, src, dst) {
  const masters = cluster.nodes('master');
  const ids = await Promise.all(masters.map((n) => n.cluster('MYID')));
  await masters[dst].cluster('SETSLOT', slot, 'IMPORTING', ids[src]);
  await masters[src].cluster('SETSLOT', slot, 'MIGRATING', ids[dst]);
  const keys = await masters[src].cluster('GETKEYSINSLOT', slot, 1000);
  if (keys.length) {
    await masters[src].migrate('127.0.0.1', masters[dst].options.port, '', 0, 5000, 'KEYS', ...keys);
  }
  return () => Promise.all(masters.map((n) => n.cluster('SETSLOT', slot, 'NODE', ids[dst])));
}

test('writes reach a replica whose slot is migrating, then moved', async () => {
  const fan = new IntraSlot(cluster, { name: 'setG' });
  await fan.publish(['a']);
  const slot = 100;
  const key = fan.keys[slot];
  const masters = cluster.nodes('master');
  const src = masters.findIndex((n) => cluster.slots[slot][0] === `${n.options.host}:${n.options.port}`);
  const dst = (src + 1) % masters.length;

  let finish;
  try {
    finish = await startSlotMove(slot, src, dst);
    const asked = await fan.applyDelta({ add: ['b'] });
    assert.deepStrictEqual(asked.failedSlots, [], 'ASK');
    assert.deepStrictEqual((await cluster.smembers(key)).sort(), ['a', 'b']);

    await finish();
    const moved = await fan.applyDelta({ add: ['c'] });
    assert.deepStrictEqual(moved.failedSlots, [], 'MOVED');
    assert.deepStrictEqual((await cluster.smembers(key)).sort(), ['a', 'b', 'c']);
  } finally {
    if (finish) {
      await finish();
      await (await startSlotMove(slot, dst, src))();
    } else {
      await Promise.all(masters.map((n) => n.cluster('SETSLOT', slot, 'STABLE')));
    }
    await new Promise((resolve) => cluster.refreshSlotsCache(resolve));
  }
});

test('reconcile waits for writes already in flight', async () => {
  const fan = new IntraSlot(cluster, { name: 'setL' });
  await fan.publish(['a']);
  // A wrong-typed replica sends its batch, neighbor included, down the slower
  // fallback path, which would land after an unqueued reconcile.
  const victimSlot = 4242;
  const neighbor = 4243;
  await cluster.set(fan.keys[victimSlot], 'not a set');

  const delta = fan.applyDelta({ add: ['b'] });
  await fan.reconcile(['a'], [neighbor]);
  await delta;
  await cluster.del(fan.keys[victimSlot]);
  assert.deepStrictEqual(await cluster.smembers(fan.keys[neighbor]), ['a']);
});

test('verify detects a drifted replica and reconcile repairs it', async () => {
  const fan = new IntraSlot(cluster, { name: 'set9' });
  await fan.publish(['a', 'b', 'c']);

  // Corrupt one replica behind the library's back, as a failed publish would.
  const victimSlot = 4242;
  await cluster.srem(fan.keys[victimSlot], 'a');

  const report = await fan.verify({ sample: 0 });
  assert.ok(!report.consistent);
  assert.deepStrictEqual(report.drifted, [victimSlot]);

  const { repaired } = await fan.reconcile(['a', 'b', 'c']);
  assert.deepStrictEqual(repaired, [victimSlot]);
  assert.ok((await fan.verify({ sample: 0 })).consistent);
});

test('coalescer batches churn into ticks and converges', async () => {
  const fan = new IntraSlot(cluster, { name: 'setA' });
  await fan.publish([]);

  const flushes = [];
  const c = fan.coalescer({ tickMs: 50 });
  c.on('flush', (s) => flushes.push(s));

  // 40 events that should collapse into far fewer fanouts.
  for (let i = 0; i < 20; i++) c.add(`u${i}`);
  for (let i = 0; i < 10; i++) c.remove(`u${i}`);
  c.add('final');
  await c.close();

  assert.ok(flushes.length >= 1, 'should have flushed at least once');
  assert.ok(flushes.length < 30, `expected coalescing, got ${flushes.length} flushes`);
  assert.strictEqual(c.pending, 0);

  const members = await fan.smembers(SUBS);
  const expected = ['final', ...Array.from({ length: 10 }, (_, i) => `u${i + 10}`)];
  assert.deepStrictEqual(members.sort(), expected.sort());
});

test('coalescer add/remove of the same member resolves to last write', async () => {
  const fan = new IntraSlot(cluster, { name: 'setB' });
  await fan.publish([]);
  const c = fan.coalescer({ tickMs: 10000 }); // never ticks on its own

  c.add('x');
  c.remove('x');
  c.add('y');
  c.remove('z');
  c.add('z');
  await c.flush();
  await c.close();

  const members = await fan.smembers(SUBS);
  assert.deepStrictEqual(members.sort(), ['y', 'z']);
});

test('clear removes every replica', async () => {
  const fan = new IntraSlot(cluster, { name: 'setC' });
  await fan.publish(['a']);
  await fan.clear();
  const report = await fan.verify({ sample: 256 });
  assert.strictEqual(report.majority, 0);
});

test('Redis-named aliases match their plain-English primaries', async () => {
  const fan = new IntraSlot(cluster, { name: 'setD' });
  await seedSubs();
  await fan.publish(['bob', 'erin', 'zoe']);

  assert.deepStrictEqual(
    (await fan.sinter(SUBS)).sort(),
    (await fan.intersect(SUBS)).sort()
  );
  assert.strictEqual(await fan.sintercard(SUBS), await fan.count(SUBS));
  assert.deepStrictEqual((await fan.sdiff(SUBS)).sort(), (await fan.without(SUBS)).sort());
  assert.strictEqual(
    await fan.sinterstore(`a:{${USER}}`, SUBS),
    await fan.intersectInto(`b:{${USER}}`, SUBS)
  );
});

for (const [label, opts] of [
  ['cluster keyPrefix', { keyPrefix: 'pfx:' }],
  ['node keyPrefix', { redisOptions: { keyPrefix: 'pfx:' } }],
]) {
  test(`writes honor ${label}`, async () => {
    const prefixed = new Redis.Cluster(NODES, opts);
    try {
      await new Promise((resolve, reject) => {
        prefixed.once('ready', resolve);
        prefixed.once('error', reject);
      });
      // chunkSize 64 sends deltas of more than 32 members down the pipeline.
      const fan = new IntraSlot(prefixed, { name: 'setK', chunkSize: 64 });
      await fan.publish(['a']);
      await fan.applyDelta({ add: ['b'] });
      const many = Array.from({ length: 40 }, (_, i) => `m${i}`);
      await fan.add(many);
      await fan.remove(many);
      assert.deepStrictEqual((await fan.smembers(SUBS)).sort(), ['a', 'b']);
      assert.ok((await fan.verify({ sample: 0 })).consistent);
      assert.strictEqual(await cluster.exists(fan.keyFor(SUBS)), 0, 'no unprefixed replica');
    } finally {
      await prefixed.quit();
    }
  });
}

test('constructor requires a name', () => {
  assert.throws(() => new IntraSlot(cluster, {}), /options\.name is required/);
  assert.throws(() => new IntraSlot(null, { name: 'x' }), /cluster instance is required/);
});
