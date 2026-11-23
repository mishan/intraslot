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

const { IntraSlot, CrossSlotError, PartialPublishError, slotFor } = require('../src/index');

const NODES = [{ host: '127.0.0.1', port: 7001 }];
const USER = '1234567890';
const SUBS = `subs:{${USER}}`;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

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

test('applyDelta can target chosen slots', async () => {
  const fan = new IntraSlot(cluster, { name: 'setV' });
  await fan.publish(['a']);
  const result = await fan.applyDelta({ add: ['b'] }, { slots: [7, 12000] });
  assert.strictEqual(result.replicas, 2);
  const report = await fan.verify({ sample: 0 });
  assert.deepStrictEqual(report.drifted.sort((x, y) => x - y), [7, 12000]);
  assert.deepStrictEqual((await cluster.smembers(fan.keys[7])).sort(), ['a', 'b']);
  for (const bad of [99999, -1, '4', 1.5]) {
    await assert.rejects(() => fan.applyDelta({ add: ['z'] }, { slots: [3, bad] }), RangeError);
  }
});

test('slots that miss writes catch up as if they had seen every write in order', async () => {
  const fan = new IntraSlot(cluster, { name: 'setBA' });
  await fan.publish(['v']);
  const slot = 4242;
  await cluster.set(fan.keys[slot], 'not a set'); // fails every write until deleted
  try {
    assert.deepStrictEqual((await fan.add(['w', 'x'])).failedSlots, [slot]);
    assert.strictEqual((await fan.applyDelta({ remove: ['v', 'x'], add: ['y'] })).behind, 1);
  } finally {
    await cluster.del(fan.keys[slot]);
  }
  await cluster.sadd(fan.keys[slot], 'v'); // as if the replica came back as it was

  assert.strictEqual((await fan.catchUp()).behind, 0);
  assert.deepStrictEqual((await cluster.smembers(fan.keys[slot])).sort(), ['w', 'y']);
  assert.ok((await fan.verify({ sample: 0 })).consistent);
});

test('a targeted write to some behind slots leaves the others as they were', async () => {
  const fan = new IntraSlot(cluster, { name: 'setBD' });
  await fan.publish([]);
  const [one, other] = [4242, 4243];
  await Promise.all([one, other].map((slot) => cluster.set(fan.keys[slot], 'not a set')));
  try {
    await fan.add(['x']); // both fall behind together
    await fan.applyDelta({ add: ['y'] }, { slots: [one] });
  } finally {
    await Promise.all([one, other].map((slot) => cluster.del(fan.keys[slot])));
  }
  await fan.catchUp();
  assert.deepStrictEqual((await cluster.smembers(fan.keys[one])).sort(), ['x', 'y']);
  assert.deepStrictEqual(await cluster.smembers(fan.keys[other]), ['x']);
});

test('catchUp(slots) retries just those slots', async () => {
  const fan = new IntraSlot(cluster, { name: 'setCE' });
  await fan.publish([]);
  const [fixed, broken] = [4242, 8000];
  await Promise.all([fixed, broken].map((slot) => cluster.set(fan.keys[slot], 'not a set')));
  try {
    await fan.add(['x']);
    assert.strictEqual(fan.behind, 2);
    await cluster.del(fan.keys[fixed]);
    const before = await scriptCalls();
    assert.strictEqual((await fan.catchUp([fixed])).behind, 1);
    assert.strictEqual((await scriptCalls()) - before, 1, 'retried more than the slot asked for');
    assert.deepStrictEqual(await cluster.smembers(fan.keys[fixed]), ['x']);
  } finally {
    await cluster.del(fan.keys[broken]);
  }
});

test('a publish supersedes what a behind slot missed', async () => {
  const fan = new IntraSlot(cluster, { name: 'setBB' });
  await fan.publish([]);
  const slot = 4242;
  await cluster.set(fan.keys[slot], 'not a set');
  const c = fan.coalescer({ tickMs: 20 });
  try {
    c.add('x');
    await c.flush();
    assert.strictEqual(fan.behind, 1);
    await fan.publish(['p']); // replaces the wrong-typed key too
    assert.strictEqual(fan.behind, 0);
    await sleep(100);
  } finally {
    await c.close();
  }
  const report = await fan.verify({ sample: 0 });
  assert.ok(report.consistent);
  assert.strictEqual(report.majority, 1);
});

test('a slot that keeps failing stays behind once, costing one retry per write', async () => {
  const fan = new IntraSlot(cluster, { name: 'setBC' });
  await fan.publish(['a']);
  const slot = 4242;
  await cluster.set(fan.keys[slot], 'not a set');
  try {
    for (let i = 0; i < 20; i++) await fan.add([`m${i}`]);
    const before = await scriptCalls();
    const result = await fan.add(['final']);
    assert.strictEqual(result.behind, 1);
    // ~16 script calls for the delta plus one retry, not one per earlier write.
    assert.ok((await scriptCalls()) - before < 25, 'retries piled up');
  } finally {
    await cluster.del(fan.keys[slot]);
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

test('a small delta sends neither a command nor a key name per replica', async () => {
  const fan = new IntraSlot(cluster, { name: 'setE' });
  await fan.publish(['a']);

  const before = await netInputBytes();
  await fan.applyDelta({ add: ['b'], remove: ['a'] });
  const sent = (await netInputBytes()) - before;

  assert.deepStrictEqual(await fan.smembers(SUBS), ['b']);
  // Even one byte per replica would be 16 KB.
  assert.ok(sent < 16384, `sent ${sent} bytes for a +1/-1 delta`);
});

test('publish and clear send the members once per call, not once per replica', async () => {
  const fan = new IntraSlot(cluster, { name: 'setN' });
  const members = Array.from({ length: 200 }, (_, i) => `user${i}`);
  await fan.publish(['old']);

  let before = await netInputBytes();
  await fan.publish(members);
  const published = (await netInputBytes()) - before;
  assert.strictEqual((await fan.verify({ sample: 0 })).majority, 200);
  before = await netInputBytes();
  await fan.clear();
  const cleared = (await netInputBytes()) - before;

  // Sending 200 members to each of 16384 replicas would be over 25 MB.
  assert.ok(published < 2_000_000, `publish sent ${published} bytes`);
  assert.ok(cleared < 16384, `clear sent ${cleared} bytes`);
  assert.strictEqual((await fan.verify({ sample: 0 })).majority, 0);
});

async function scriptCalls() {
  const infos = await Promise.all(cluster.nodes('master').map((n) => n.info('commandstats')));
  return infos.reduce((sum, s) => sum + Number((/cmdstat_evalsha:calls=(\d+)/.exec(s) || [0, 0])[1]), 0);
}

test('a script call writes at most scriptBudget members', async () => {
  const fan = new IntraSlot(cluster, { name: 'setO', scriptBudget: 1000 });
  const before = await scriptCalls();
  await fan.publish(Array.from({ length: 100 }, (_, i) => `user${i}`));
  // 100 members within a budget of 1000 means 10 replicas per call.
  assert.ok((await scriptCalls()) - before >= 16384 / 10);
  assert.strictEqual((await fan.verify({ sample: 0 })).majority, 100);
});

test('undefined options fall back to the defaults', async () => {
  const fan = new IntraSlot(cluster, { name: 'setS', chunkSize: undefined, scriptBudget: undefined });
  const before = await scriptCalls();
  await fan.publish(['a']);
  assert.ok((await scriptCalls()) > before, 'publish should use the script');
  assert.strictEqual((await fan.verify({ sample: 0 })).majority, 1);
});

async function lazyfreed() {
  const infos = await Promise.all(cluster.nodes('master').map((n) => n.info('memory')));
  return infos.reduce((sum, s) => sum + Number(/lazyfreed_objects:(\d+)/.exec(s)[1]), 0);
}

test('replacing or clearing large replicas frees them in the background', async () => {
  const fan = new IntraSlot(cluster, { name: 'setR' });
  // UNLINK frees a set in the background only if it is a hashtable of more
  // than 64 elements; under 128 members it would be a single listpack.
  const large = Array.from({ length: 200 }, (_, i) => `user${i}`);
  await fan.publish(large);
  let before = await lazyfreed();
  await fan.publish(['x']);
  await fan.publish(large);
  await fan.clear();
  // Background frees are counted once done, so allow them a moment.
  for (let i = 0; i < 50 && (await lazyfreed()) - before < 2 * 16384; i++) await sleep(20);
  assert.strictEqual((await lazyfreed()) - before, 2 * 16384);
});

test('instances share one script load per node', async () => {
  const fans = ['setT1', 'setT2'].map((name) => new IntraSlot(cluster, { name }));
  await Promise.all(fans.map((fan) => fan.publish(['a'])));
  await Promise.all(cluster.nodes('master').map((n) => n.script('FLUSH')));

  const before = await netInputBytes();
  await Promise.all(fans.map((fan) => fan.add(['b'])));
  const sent = (await netInputBytes()) - before;

  // One ~115 KB load per node is ~350 KB; one per instance would be ~700 KB.
  assert.ok(sent < 500_000, `sent ${sent} bytes`);
});

test('a server without the script cached is sent it once, not per call', async () => {
  const fan = new IntraSlot(cluster, { name: 'setM' });
  await fan.publish(['a']);
  await Promise.all(cluster.nodes('master').map((n) => n.script('FLUSH')));

  const before = await netInputBytes();
  await fan.applyDelta({ add: ['b'], remove: ['a'] });
  const sent = (await netInputBytes()) - before;

  assert.deepStrictEqual(await fan.smembers(SUBS), ['b']);
  // The script is ~115 KB, so three nodes need ~350 KB; resending it with every
  // call would be about 1 MB.
  assert.ok(sent < 500_000, `sent ${sent} bytes`);
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
  node.evalsha = lost;
  node.pipeline = (...args) => Object.assign(pipeline.apply(node, args), { exec: lost });
  let result;
  try {
    result = await fan.applyDelta({ add: ['b'] });
  } finally {
    delete node.evalsha;
    delete node.pipeline;
  }
  assert.strictEqual(result.failed, owned);
});

async function rejectedScripts(c) {
  const infos = await Promise.all(c.nodes('master').map((n) => n.info('commandstats')));
  const rejected = (s) => [...s.matchAll(/cmdstat_eval(?:sha)?:.*rejected_calls=(\d+)/g)];
  return infos.reduce((sum, s) => rejected(s).reduce((t, m) => t + Number(m[1]), sum), 0);
}

test('a server that refuses scripts is not asked again', async () => {
  const masters = cluster.nodes('master');
  const acl = (...args) => Promise.all(masters.map((n) => n.call('ACL', ...args)));
  await acl('SETUSER', 'noeval', 'on', 'nopass', '~*', '&*', '+@all', '-eval', '-evalsha');
  const limited = new Redis.Cluster(NODES, { redisOptions: { username: 'noeval' } });
  try {
    await new Promise((resolve, reject) => {
      limited.once('ready', resolve);
      limited.once('error', reject);
    });
    const fan = new IntraSlot(limited, { name: 'setJ' });
    await fan.publish(['a']);
    await fan.add(['b']);
    const before = await rejectedScripts(limited);
    assert.ok(before > 0, 'the first write should have tried a script');

    await fan.add(['c']);
    assert.strictEqual(await rejectedScripts(limited), before);
    assert.deepStrictEqual((await fan.smembers(SUBS)).sort(), ['a', 'b', 'c']);

    // Without scripts, verify can still compare sizes, and finds a short replica.
    await cluster.srem(fan.keys[1024], 'a');
    const report = await fan.verify({ sample: 16 });
    assert.strictEqual(report.contents, false);
    assert.deepStrictEqual(report.drifted, [1024]);

    // An instance that has not yet met the refusal learns it inside verify.
    const fresh = await new IntraSlot(limited, { name: 'setJ' }).verify({ sample: 16 });
    assert.strictEqual(fresh.contents, false);
    assert.deepStrictEqual(fresh.drifted, [1024]);
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

test('verify catches a replica with the right size but the wrong members', async () => {
  const fan = new IntraSlot(cluster, { name: 'setU' });
  await fan.publish(['a', 'b']);
  const victimSlot = 4242;
  await cluster.srem(fan.keys[victimSlot], 'a');
  await cluster.sadd(fan.keys[victimSlot], 'c');

  const report = await fan.verify({ sample: 0 });
  assert.strictEqual(report.contents, true);
  assert.deepStrictEqual(report.drifted, [victimSlot]);

  assert.deepStrictEqual((await fan.reconcile(['a', 'b'])).repaired, [victimSlot]);
  assert.ok((await fan.verify({ sample: 0 })).consistent);
});

test('verify flags an unreadable replica, even at slot 0, and reconcile repairs it', async () => {
  const fan = new IntraSlot(cluster, { name: 'setW' });
  await fan.publish(['a', 'b']);
  await cluster.set(fan.keys[0], 'not a set');

  const report = await fan.verify({ sample: 0 });
  assert.deepStrictEqual(report.drifted, [0]);
  assert.strictEqual(report.majority, 2);

  assert.deepStrictEqual((await fan.reconcile(['a', 'b'])).repaired, [0]);
  assert.ok((await fan.verify({ sample: 0 })).consistent);
});

test('verify refuses to report a sample it could not read at all', async () => {
  const fan = new IntraSlot(cluster, { name: 'setAA' });
  // verify({ sample: 16 }) reads every 1024th slot.
  const sampled = Array.from({ length: 16 }, (_, i) => i * 1024);
  await Promise.all(sampled.map((slot) => cluster.set(fan.keys[slot], 'not a set')));
  try {
    await assert.rejects(() => fan.verify({ sample: 16 }), /could not read any sampled replica/);
  } finally {
    await Promise.all(sampled.map((slot) => cluster.del(fan.keys[slot])));
  }
});

test('verify with an unreadable replica stays cheap on the wire', async () => {
  const fan = new IntraSlot(cluster, { name: 'setX' });
  await fan.publish(['a']);
  await cluster.set(fan.keys[100], 'not a set');
  let report;
  let sent;
  try {
    const before = await netInputBytes();
    report = await fan.verify({ sample: 0 });
    sent = (await netInputBytes()) - before;
  } finally {
    await cluster.del(fan.keys[100]);
  }
  assert.deepStrictEqual(report.drifted, [100]);
  assert.ok(sent < 1_000_000, `sent ${sent} bytes`);
});

test('verify still reads replicas while masters refuse writes', async () => {
  const fan = new IntraSlot(cluster, { name: 'setY' });
  await fan.publish(['a', 'b']);
  await cluster.srem(fan.keys[42], 'a');
  await cluster.sadd(fan.keys[42], 'c');
  const masters = cluster.nodes('master');
  const previous = await Promise.all(masters.map((n) => n.config('GET', 'maxmemory')));
  await Promise.all(masters.map((n) => n.config('SET', 'maxmemory', '1')));
  let report;
  try {
    report = await fan.verify({ sample: 0 });
  } finally {
    await Promise.all(masters.map((n, i) => n.config('SET', 'maxmemory', previous[i][1])));
  }
  assert.strictEqual(report.contents, true);
  assert.deepStrictEqual(report.drifted, [42]);
});

test('verify sizes digest calls by members read, not by one replica', async () => {
  // A budget of 1600 writes is 100 members' digests per call.
  const fan = new IntraSlot(cluster, { name: 'setZ', scriptBudget: 1600 });
  await fan.publish(Array.from({ length: 50 }, (_, i) => `user${i}`));
  await cluster.del(fan.keys[0]); // an empty slot 0 must not inflate every call
  const before = await scriptCalls();
  const report = await fan.verify({ sample: 0 });
  assert.deepStrictEqual(report.drifted, [0]);
  // 16384 replicas of 50 members at 100 members per call, give or take.
  const calls = (await scriptCalls()) - before;
  assert.ok(calls >= 16384 / 2 - 100 && calls <= 16384 / 2 + 100, `${calls} calls`);
});

test('verify sends each call only the slots it can get through', async () => {
  // 100 members' digests per call over 50-member replicas: 2 replicas a call.
  const fan = new IntraSlot(cluster, { name: 'setBF', scriptBudget: 1600 });
  await fan.publish(Array.from({ length: 50 }, (_, i) => `user${i}`));
  const before = await netInputBytes();
  const report = await fan.verify({ sample: 8192 }); // every other slot
  const sent = (await netInputBytes()) - before;
  assert.ok(report.consistent);
  // Resending all of a 2048-slot batch's ranges each call would be ~80 MB.
  assert.ok(sent < 2_000_000, `sent ${sent} bytes`);
});

test('verify reads a slot that moved since the client last looked', async () => {
  const fan = new IntraSlot(cluster, { name: 'setBE' });
  await fan.publish(['a', 'b']);
  const slot = 100;
  const masters = cluster.nodes('master');
  const src = masters.findIndex((n) => cluster.slots[slot][0] === `${n.options.host}:${n.options.port}`);
  const dst = (src + 1) % masters.length;
  await (await startSlotMove(slot, src, dst))(); // the client's slot map is now stale
  try {
    const report = await fan.verify({ sample: 0 });
    assert.strictEqual(report.contents, true);
    assert.deepStrictEqual(report.drifted, []);
  } finally {
    await (await startSlotMove(slot, dst, src))();
    await new Promise((resolve) => cluster.refreshSlotsCache(resolve));
  }
});

for (const [label, retries] of [['retries a slot caught mid-migration', 3], ['reports it at once with retries: 0', 0]]) {
  test(`publish ${label}`, async () => {
    const fan = new IntraSlot(cluster, { name: `setCA${retries}` });
    await fan.publish(['a']);
    const slot = 100;
    const masters = cluster.nodes('master');
    const ids = await Promise.all(masters.map((n) => n.cluster('MYID')));
    const src = masters.findIndex((n) => cluster.slots[slot][0] === `${n.options.host}:${n.options.port}`);
    const dst = (src + 1) % masters.length;
    // Mid-migration with the replica still on the source, a replace fails
    // until its keys move. Move them, and hand the slot over, at the first
    // retry, so the first attempt is sure to have failed.
    await masters[dst].cluster('SETSLOT', slot, 'IMPORTING', ids[src]);
    await masters[src].cluster('SETSLOT', slot, 'MIGRATING', ids[dst]);
    let handedOver = false;
    const catchUp = fan.catchUp.bind(fan);
    fan.catchUp = async (...args) => {
      if (!handedOver) {
        const keys = await masters[src].cluster('GETKEYSINSLOT', slot, 1000);
        await masters[src].migrate('127.0.0.1', masters[dst].options.port, '', 0, 5000, 'KEYS', ...keys);
        await Promise.all(masters.map((n) => n.cluster('SETSLOT', slot, 'NODE', ids[dst])));
        handedOver = true;
      }
      return catchUp(...args);
    };
    try {
      if (retries) {
        assert.strictEqual((await fan.publish(['b'], { retries })).failed, 0);
        assert.ok(handedOver, 'publish never retried');
        assert.deepStrictEqual(await cluster.smembers(fan.keys[slot]), ['b']);
      } else {
        await assert.rejects(
          () => fan.publish(['b'], { retries }),
          (err) => err instanceof PartialPublishError && /slot 100: (ASK|TRYAGAIN)/.test(err.message)
        );
        assert.ok(!handedOver, 'publish retried');
      }
    } finally {
      if (handedOver) {
        await (await startSlotMove(slot, dst, src))();
      } else {
        await Promise.all([src, dst].map((i) => masters[i].cluster('SETSLOT', slot, 'STABLE')));
      }
      await new Promise((resolve) => cluster.refreshSlotsCache(resolve));
    }
  });
}

test('a replace that a migration blocks leaves the old replica whole', async () => {
  const fan = new IntraSlot(cluster, { name: 'setCC' });
  await fan.publish(['a', 'x']);
  const slot = 100;
  const masters = cluster.nodes('master');
  const ids = await Promise.all(masters.map((n) => n.cluster('MYID')));
  const src = masters.findIndex((n) => cluster.slots[slot][0] === `${n.options.host}:${n.options.port}`);
  const dst = (src + 1) % masters.length;
  await masters[dst].cluster('SETSLOT', slot, 'IMPORTING', ids[src]);
  await masters[src].cluster('SETSLOT', slot, 'MIGRATING', ids[dst]);
  try {
    // The retry sends slot 100 alone, so a script call starts with it.
    const result = await fan.publish(['b'], { retries: 1, throwOnPartial: false });
    assert.deepStrictEqual(result.failedSlots, [slot]);
    assert.deepStrictEqual((await masters[src].smembers(fan.keys[slot])).sort(), ['a', 'x']);
    const staging = `intraslot:staging:{${require('../src/table').TAGS[slot]}}`;
    assert.strictEqual(await masters[dst].call('ASKING').then(() => masters[dst].exists(staging)), 0);
  } finally {
    await Promise.all([src, dst].map((i) => masters[i].cluster('SETSLOT', slot, 'STABLE')));
  }
});

test('a user who may not RENAME still publishes, replacing in place', async () => {
  const fan = new IntraSlot(cluster, { name: 'setCD' });
  await fan.publish(['a']);
  const masters = cluster.nodes('master');
  const acl = (...args) => Promise.all(masters.map((n) => n.call('ACL', ...args)));
  await acl('SETUSER', 'norename', 'on', 'nopass', '~*', '&*', '+@all', '-rename');
  const limited = new Redis.Cluster(NODES, { redisOptions: { username: 'norename' } });
  try {
    await new Promise((resolve, reject) => {
      limited.once('ready', resolve);
      limited.once('error', reject);
    });
    const own = new IntraSlot(limited, { name: 'setCD' });
    assert.strictEqual((await own.publish(['b', 'c'])).failed, 0);
    const report = await own.verify({ sample: 0 });
    assert.strictEqual(report.contents, true, 'the script was switched off');
    assert.ok(report.consistent);
    assert.strictEqual(report.majority, 2);
  } finally {
    limited.disconnect();
    await acl('DELUSER', 'norename');
  }
});

test('a user who may not touch the staging keys can still publish and clear', async () => {
  const masters = cluster.nodes('master');
  const acl = (...args) => Promise.all(masters.map((n) => n.call('ACL', ...args)));
  // Both with scripts and without, where every write takes the plain commands.
  await acl('SETUSER', 'nostaging', 'on', 'nopass', '~setCG*', '&*', '+@all');
  await acl('SETUSER', 'nostagingnoscript', 'on', 'nopass', '~setCG*', '&*', '+@all', '-@scripting');
  const clients = ['nostaging', 'nostagingnoscript'].map(
    (username) => new Redis.Cluster(NODES, { redisOptions: { username } })
  );
  try {
    await Promise.all(clients.map((c) => new Promise((resolve, reject) => {
      c.once('ready', resolve);
      c.once('error', reject);
    })));
    for (const [i, client] of clients.entries()) {
      const fan = new IntraSlot(client, { name: `setCG${i}` });
      if (i === 0) assert.strictEqual((await fan.publish(['a', 'b'])).failed, 0, 'publish');
      else await fan.add(['a', 'b']); // the plain path cannot replace without staging access
      assert.strictEqual((await fan.clear()).failed, 0, `clear as user ${i}`);
      assert.strictEqual(fan.behind, 0);
    }
  } finally {
    clients.forEach((c) => c.disconnect());
    await acl('DELUSER', 'nostaging', 'nostagingnoscript');
  }
});

for (const [label, moveKeys] of [['still on the source', false], ['moved', true]]) {
  test(`clear() empties a slot mid-migration with its keys ${label}`, async () => {
    const fan = new IntraSlot(cluster, { name: `setCF${Number(moveKeys)}` });
    await fan.publish(['a']);
    const slot = 100;
    const masters = cluster.nodes('master');
    const ids = await Promise.all(masters.map((n) => n.cluster('MYID')));
    const src = masters.findIndex((n) => cluster.slots[slot][0] === `${n.options.host}:${n.options.port}`);
    const dst = (src + 1) % masters.length;
    await masters[dst].cluster('SETSLOT', slot, 'IMPORTING', ids[src]);
    await masters[src].cluster('SETSLOT', slot, 'MIGRATING', ids[dst]);
    try {
      if (moveKeys) {
        const keys = await masters[src].cluster('GETKEYSINSLOT', slot, 1000);
        await masters[src].migrate('127.0.0.1', masters[dst].options.port, '', 0, 5000, 'KEYS', ...keys);
      }
      assert.deepStrictEqual((await fan.clear({ retries: 0 })).failedSlots, []);
      // Mid-migration the source answers ASK for a key it no longer has.
      const onSource = await masters[src].exists(fan.keys[slot]).catch((err) => (/^ASK /.test(err.message) ? 0 : 1));
      assert.strictEqual(onSource, 0);
      assert.strictEqual(await masters[dst].call('ASKING').then(() => masters[dst].exists(fan.keys[slot])), 0);
    } finally {
      if (moveKeys) {
        await Promise.all(masters.map((n) => n.cluster('SETSLOT', slot, 'NODE', ids[dst])));
        await (await startSlotMove(slot, dst, src))();
      } else {
        await Promise.all([src, dst].map((i) => masters[i].cluster('SETSLOT', slot, 'STABLE')));
      }
      await new Promise((resolve) => cluster.refreshSlotsCache(resolve));
    }
  });
}

test('publish does not wait out a stalled node more than once', async () => {
  const stalled = cluster.nodes('master')[0];
  const timing = new Redis.Cluster(NODES, { redisOptions: { commandTimeout: 300 } });
  try {
    await new Promise((resolve, reject) => {
      timing.once('ready', resolve);
      timing.once('error', reject);
    });
    const fan = new IntraSlot(timing, { name: 'setCB' });
    await stalled.call('CLIENT', 'PAUSE', '5000', 'WRITE');
    const t0 = Date.now();
    await assert.rejects(
      () => fan.publish(['a']),
      (err) => err instanceof PartialPublishError && /timed out/i.test(err.message)
    );
    // Three retries would add a second of waiting plus three more timeouts.
    assert.ok(Date.now() - t0 < 1500, `publish took ${Date.now() - t0} ms`);
  } finally {
    await stalled.call('CLIENT', 'UNPAUSE');
    timing.disconnect();
  }
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
  const c = fan.coalescer({ tickMs: 10000 });

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

/** Resolves with the time of `c`'s next flush, or rejects after `ms`. */
function nextFlush(c, ms) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`no flush within ${ms} ms`)), ms);
    c.once('flush', () => {
      clearTimeout(timer);
      resolve(Date.now());
    });
  });
}

test('coalescer applies a change queued while a flush is running', async () => {
  const fan = new IntraSlot(cluster, { name: 'setQ' });
  await fan.publish([]);
  const c = fan.coalescer({ tickMs: 1 }); // the tick ends before the flush does
  try {
    c.add('a');
    const first = c.flush();
    c.add('b');
    await first;
    await nextFlush(c, 2000);
    assert.deepStrictEqual((await fan.smembers(SUBS)).sort(), ['a', 'b']);
  } finally {
    await c.close();
  }
});

test('coalescer flushes a change after idle at once, then holds the next for a tick', async () => {
  const fan = new IntraSlot(cluster, { name: 'setP' });
  await fan.publish([]);
  const tickMs = 2000;
  const c = fan.coalescer({ tickMs });
  try {
    const start = Date.now();
    c.add('first');
    await nextFlush(c, tickMs / 2);
    assert.deepStrictEqual(await fan.smembers(SUBS), ['first']);

    c.add('second');
    const second = await nextFlush(c, tickMs * 2);
    assert.ok(second - start >= tickMs, `second flush after ${second - start} ms`);
    assert.deepStrictEqual((await fan.smembers(SUBS)).sort(), ['first', 'second']);
  } finally {
    await c.close();
  }
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
      // scriptBudget 64 sends writes of more than 32 members down the pipeline.
      const fan = new IntraSlot(prefixed, { name: 'setK', scriptBudget: 64 });
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
