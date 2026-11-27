'use strict';
/** @module intraslot/coalescer */

const { EventEmitter } = require('events');
const { performance } = require('perf_hooks');

/**
 * @typedef {object} CoalescerOptions
 * @property {number} [tickMs]   minimum time between the starts of two flushes (default 250)
 * @property {number} [maxBuffer] flush early once this many pending changes accumulate (default Infinity)
 * @property {(err: Error) => void} [onError] called on flush failure; otherwise emitted as 'error'
 */

/**
 * Buffers membership changes and applies them at most once per tick.
 *
 * Fanning out per event costs `events/sec x 16384` replica writes and is
 * hopeless past a trickle. A tick costs `2 x 16384` replica writes no matter how
 * many events it absorbed, so the write cost becomes a function of tick rate
 * rather than churn rate. The price is staleness bounded by one tick -- fine for
 * presence data, wrong wherever a stale read is incorrect rather than merely
 * dated.
 *
 * A change that arrives when no flush has started for a tick is applied at once,
 * on the next turn of the event loop so that a synchronous burst still lands in
 * one flush. Changes arriving sooner wait until a tick after the last flush
 * started, so rare changes land in milliseconds and heavy churn still costs one
 * fanout per tick.
 *
 * Flushes never overlap: changes queued while one runs wait for it to finish,
 * since two overlapping deltas can land out of order.
 *
 * Slots a flush fails on stay behind in the IntraSlot, which brings them up to
 * date in order with every later write. While any are behind, ticks keep
 * coming, so they catch up without waiting for another change.
 *
 * @example
 * const c = fan.coalescer({ tickMs: 250 });
 * c.on('flush', (s) => metrics.timing('fanout.tick', s.ms));
 * onActivate((u) => c.add(u));
 * onDeactivate((u) => c.remove(u));
 * // later
 * await c.close();
 *
 * @fires Coalescer#flush
 * @fires Coalescer#error
 */
class Coalescer extends EventEmitter {
  /**
   * @param {import('./intraslot').IntraSlot} fanout
   * @param {CoalescerOptions} [opts]
   */
  constructor(fanout, opts = {}) {
    super();
    this.fanout = fanout;
    this.tickMs = opts.tickMs ?? 250;
    this.maxBuffer = opts.maxBuffer ?? Infinity;
    if (opts.onError) this.on('error', opts.onError);

    /** @type {Set<string>} */
    this._add = new Set();
    /** @type {Set<string>} */
    this._remove = new Set();
    this._flushing = false;
    this._closed = false;
    /** The final flush close() started, shared by every call. @type {Promise<void>|null} */
    this._closing = null;
    /**
     * When the last flush started, or failed, on the monotonic clock: a
     * wall-clock step must not stretch or skip a tick.
     */
    this._lastStart = -Infinity;
    /** @type {NodeJS.Timeout|null} */
    this._timer = null;
  }

  /** Pending change count. @returns {number} */
  get pending() {
    return this._add.size + this._remove.size;
  }

  /**
   * Queue an addition. Cancels a pending removal of the same member, so the
   * last write within a tick wins.
   * @param {...string} members
   */
  add(...members) {
    this._assertOpen();
    for (const m of members.flat()) {
      this._remove.delete(m);
      this._add.add(m);
    }
    if (this.pending >= this.maxBuffer) this._maybeFlush();
    else this._schedule();
  }

  /**
   * Queue a removal. Cancels a pending addition of the same member.
   * @param {...string} members
   */
  remove(...members) {
    this._assertOpen();
    for (const m of members.flat()) {
      this._add.delete(m);
      this._remove.add(m);
    }
    if (this.pending >= this.maxBuffer) this._maybeFlush();
    else this._schedule();
  }

  /** @private */
  _assertOpen() {
    if (this._closed) throw new Error('intraslot: this coalescer is closed; changes would never be sent');
  }

  /** @private @returns {boolean} whether a flush has anything to send */
  _hasWork() {
    return this.pending > 0 || this.fanout.behind > 0;
  }

  /**
   * Arm the timer for the next flush, a tick after the last one started.
   * @private
   */
  _schedule() {
    if (this._timer || this._closed || !this._hasWork()) return;
    const wait = Math.max(0, this._lastStart + this.tickMs - performance.now());
    this._timer = setTimeout(() => {
      this._timer = null;
      // Timers run off the event loop's cached millisecond clock, so one can
      // fire a little before performance.now() says the tick is over.
      if (performance.now() < this._lastStart + this.tickMs) return this._schedule();
      this._maybeFlush();
    }, wait);
    if (this._timer.unref) this._timer.unref();
  }

  /** @private */
  _maybeFlush() {
    if (this._flushing || !this._hasWork()) return;
    this.flush().catch((err) => this.emit('error', err));
  }

  /**
   * Apply the buffered delta, or catch up slots that are behind, immediately.
   * Safe to call directly; overlapping calls are serialized. Rejects if the
   * write fails, or if a 'flush' listener throws, though the write landed.
   * @returns {Promise<import('./intraslot').PublishResult|null>} null if nothing was pending
   */
  async flush() {
    if (this._flushing) {
      // Wait for the in-flight flush rather than racing it, then send what it
      // did not take. A 'flush' listener can keep starting flushes, so wait a
      // bounded number; if one is still running, it will take these changes.
      await this._settle();
      return this._flushing ? this._inflight : this.flush();
    }
    if (!this._hasWork()) return null;

    const add = this._add;
    const remove = this._remove;
    this._add = new Set();
    this._remove = new Set();
    this._flushing = true;
    this._lastStart = performance.now();
    // A timer armed for the previous tick would fire too soon after this flush.
    if (this._timer) clearTimeout(this._timer);
    this._timer = null;

    this._inflight = (async () => {
      let result;
      try {
        result = add.size || remove.size
          ? await this.fanout.applyDelta({ add, remove })
          : { ...(await this.fanout.catchUp()), ms: 0, failed: 0 };
      } catch (err) {
        // Wait a full tick before retrying, however long this attempt took.
        this._lastStart = performance.now();
        // Put the delta back so the next tick retries it, without clobbering
        // changes queued while this flush was in flight.
        for (const m of add) if (!this._remove.has(m)) this._add.add(m);
        for (const m of remove) if (!this._add.has(m)) this._remove.add(m);
        throw err;
      } finally {
        this._flushing = false;
        // Changes queued meanwhile, a failed delta put back, or slots still
        // behind go next tick.
        this._schedule();
      }
      // Only once the delta is sent, so a listener that throws cannot get it
      // put back and sent again every tick.
      /**
       * @event Coalescer#flush
       * @type {{ added: number, removed: number, ms: number, failed: number }}
       * `failed` counts slots still behind, which later ticks retry.
       */
      this.emit('flush', {
        added: add.size,
        removed: remove.size,
        ms: result.ms,
        failed: result.behind ?? result.failed,
      });
      return result;
    })();

    return this._inflight;
  }

  /**
   * Wait for the flush in flight and for up to two more that 'flush' listeners
   * start after it; a listener that flushes on every flush would never stop.
   * @private
   */
  async _settle() {
    for (let i = 0; i < 3 && this._flushing; i++) await this._inflight.catch(() => {});
  }

  /**
   * Stop ticking and flush whatever remains. Every call waits for that flush,
   * and rejects if it fails, or if a 'flush' listener throws. Changes that
   * failed to send stay pending, and calling close() again tries once more.
   * @returns {Promise<void>}
   */
  close() {
    if (!this._closing) {
      this._closed = true;
      if (this._timer) clearTimeout(this._timer);
      this._timer = null;
      this._closing = (async () => {
        await this._settle();
        if (this._hasWork()) await this.flush();
        // A flush a 'flush' listener starts can follow that one.
        await this._settle();
        // Changes a failed listener-started flush put back.
        if (this.pending) await this.flush();
      })().catch((err) => {
        this._closing = null;
        throw err;
      });
    }
    return this._closing;
  }
}

// Assigned rather than module.exports = {...}: see the note in intraslot.js.
exports.Coalescer = Coalescer;
