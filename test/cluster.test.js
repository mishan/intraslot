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

test('constructor requires a name', () => {
  assert.throws(() => new IntraSlot(cluster, {}), /options\.name is required/);
  assert.throws(() => new IntraSlot(null, { name: 'x' }), /cluster instance is required/);
});
