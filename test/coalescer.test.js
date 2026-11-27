'use strict';
/**
 * Coalescer timing, against a fake fanout. No Redis required.
 */

const test = require('node:test');
const assert = require('node:assert');
const { performance } = require('node:perf_hooks');

const { Coalescer } = require('../src/coalescer');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
// The fake stamps a start a few microseconds after the coalescer does.
const SLACK = 0.1;

/** A fanout that records when each flush starts and ends. */
function fakeFanout({ ms = 5, failures = 0 } = {}) {
  return {
    starts: [],
    ends: [],
    async applyDelta() {
      this.starts.push(performance.now());
      await sleep(ms);
      this.ends.push(performance.now());
      if (failures-- > 0) throw new Error('boom');
      return { ms, failed: 0 };
    },
  };
}

test('a backward wall-clock step does not delay pending changes', async () => {
  const fanout = fakeFanout();
  const c = new Coalescer(fanout, { tickMs: 100 });
  const realNow = Date.now;
  try {
    c.add('a');
    await sleep(20);
    Date.now = () => realNow() - 60 * 60 * 1000;
    c.add('b');
    await sleep(250);
    assert.strictEqual(fanout.starts.length, 2);
    assert.strictEqual(c.pending, 0);
  } finally {
    Date.now = realNow;
    await c.close();
  }
});

for (const [label, opts, flushEarly] of [
  ['a manual flush', {}, (c) => (c.add('b'), c.flush())],
  ['a maxBuffer flush', { maxBuffer: 2 }, (c) => c.add('b', 'c')],
]) {
  test(`${label} restarts the tick`, async () => {
    const tickMs = 200;
    const fanout = fakeFanout();
    const c = new Coalescer(fanout, { tickMs, ...opts });
    try {
      c.add('a');
      await sleep(20);
      c.add('x'); // arms the timer for a tick after the first flush
      await sleep(80);
      await flushEarly(c);
      c.add('d');
      await sleep(tickMs + 100);
      assert.strictEqual(fanout.starts.length, 3);
      const gap = fanout.starts[2] - fanout.starts[1];
      assert.ok(gap >= tickMs - SLACK, `flushes started ${gap.toFixed(0)} ms apart`);
    } finally {
      await c.close();
    }
  });
}

test('a failed flush is retried a tick after it fails', async () => {
  const tickMs = 100;
  const fanout = fakeFanout({ ms: 150, failures: 1 });
  const c = new Coalescer(fanout, { tickMs });
  c.on('error', () => {});
  try {
    c.add('a');
    await sleep(150 + tickMs + 100);
    assert.strictEqual(fanout.starts.length, 2);
    const wait = fanout.starts[1] - fanout.ends[0];
    assert.ok(wait >= tickMs - SLACK, `retried ${wait.toFixed(0)} ms after failing`);
  } finally {
    await c.close();
  }
});

test('ticks keep coming while slots are behind, with no new change', async () => {
  const fanout = {
    behind: 0,
    catchUps: 0,
    async applyDelta() {
      this.behind = 3;
      return { ms: 1, failed: 3, failedSlots: [1, 2, 3], behind: 3 };
    },
    async catchUp() {
      this.catchUps++;
      this.behind = 0;
      return { behind: 0 };
    },
  };
  const c = new Coalescer(fanout, { tickMs: 20 });
  const failed = [];
  c.on('flush', (stats) => failed.push(stats.failed));
  try {
    c.add('x');
    await sleep(150);
    assert.strictEqual(fanout.catchUps, 1);
    assert.deepStrictEqual(failed, [3, 0]);
  } finally {
    await c.close();
  }
});

test('changes after close() are refused, not silently held', async () => {
  const c = new Coalescer(fakeFanout(), { tickMs: 20 });
  await c.close();
  assert.throws(() => c.add('x'), /closed/);
  assert.throws(() => c.remove('x'), /closed/);
});

test('every close() call waits for the final flush', async () => {
  const fanout = fakeFanout({ ms: 50 });
  const c = new Coalescer(fanout, { tickMs: 1000 });
  c.add('x');
  const first = c.close();
  await c.close();
  assert.strictEqual(fanout.ends.length, 1, 'second close() returned before the flush finished');
  await first;
});

test('a flush listener that throws does not get the delta sent again', async () => {
  const fanout = fakeFanout();
  const c = new Coalescer(fanout, { tickMs: 20 });
  const errors = [];
  c.on('error', (err) => errors.push(err));
  c.once('flush', () => {
    throw new Error('listener bug');
  });
  try {
    c.add('x');
    await sleep(150);
    assert.strictEqual(fanout.starts.length, 1);
    assert.strictEqual(c.pending, 0);
    assert.match(errors[0].message, /listener bug/);
  } finally {
    await c.close();
  }
});

test('close() tries again after its final flush failed', async () => {
  const fanout = fakeFanout({ failures: 1 });
  const c = new Coalescer(fanout, { tickMs: 1000 });
  c.add('x');
  await assert.rejects(() => c.close(), /boom/);
  assert.strictEqual(c.pending, 1);
  await c.close();
  assert.strictEqual(c.pending, 0);
  assert.strictEqual(fanout.starts.length, 2);
});

test('close() waits for a flush a flush listener starts', async () => {
  const fanout = fakeFanout({ ms: 20 });
  const c = new Coalescer(fanout, { tickMs: 1000 });
  c.add('a');
  const first = c.flush();
  c.add('b');
  c.once('flush', () => c.flush());
  await c.close();
  await first;
  assert.strictEqual(fanout.ends.length, 2, 'close() resolved before the second flush landed');
  assert.strictEqual(c.pending, 0);
});

test('flush() during another flush still sends its own changes', async () => {
  const fanout = fakeFanout({ ms: 20 });
  const c = new Coalescer(fanout, { tickMs: 1000 });
  try {
    c.add('a');
    const first = c.flush();
    c.add('b');
    await c.flush();
    await first;
    assert.strictEqual(c.pending, 0);
    assert.strictEqual(fanout.starts.length, 2);
  } finally {
    await c.close();
  }
});

test('close() returns even if a flush listener keeps flushing', async () => {
  const fanout = {
    behind: 1,
    calls: 0,
    async applyDelta() {
      this.calls++;
      return { ms: 1, failed: 1, failedSlots: [5], behind: 1 };
    },
    async catchUp() {
      this.calls++;
      await sleep(1);
      if (this.calls >= 50) this.behind = 0; // so the listener's loop ends
      return { behind: this.behind };
    },
  };
  const c = new Coalescer(fanout, { tickMs: 1000 });
  c.on('flush', (stats) => stats.failed && c.flush());
  c.add('x');
  await c.close();
  assert.ok(fanout.calls < 20, `close() waited through ${fanout.calls} fanouts`);
  while (fanout.behind) await sleep(5);
});
