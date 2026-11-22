'use strict';
/**
 * Coalescer timing, against a fake fanout. No Redis required.
 */

const test = require('node:test');
const assert = require('node:assert');
const { performance } = require('node:perf_hooks');

const { Coalescer } = require('../src/coalescer');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
// Timers can fire up to a millisecond early relative to performance.now().
const SLACK = 2;

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
