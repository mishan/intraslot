'use strict';
/** @module intraslot/coalescer */

const { EventEmitter } = require('events');

/**
 * @typedef {object} CoalescerOptions
 * @property {number} [tickMs]   how long to buffer before flushing (default 250)
 * @property {number} [maxBuffer] flush early once this many pending changes accumulate (default Infinity)
 * @property {(err: Error) => void} [onError] called on flush failure; otherwise emitted as 'error'
 */

/**
 * Buffers membership changes and applies them once per tick.
 *
 * Fanning out per event costs `events/sec x 16384` commands and is hopeless past
 * a trickle. A tick costs `2 x 16384` commands no matter how many events it
 * absorbed, so the write cost becomes a function of tick rate rather than churn
 * rate. The price is staleness bounded by one tick -- fine for presence data,
 * wrong wherever a stale read is incorrect rather than merely dated.
 *
 * Ticks never overlap: if a flush is still running when the timer fires, the
 * next flush is deferred rather than issued concurrently, since two overlapping
 * deltas can land out of order.
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
    /** @type {NodeJS.Timeout|null} */
    this._timer = setInterval(() => this._maybeFlush(), this.tickMs);
    if (this._timer.unref) this._timer.unref();
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
    for (const m of members.flat()) {
      this._remove.delete(m);
      this._add.add(m);
    }
    if (this.pending >= this.maxBuffer) this._maybeFlush();
  }

  /**
   * Queue a removal. Cancels a pending addition of the same member.
   * @param {...string} members
   */
  remove(...members) {
    for (const m of members.flat()) {
      this._add.delete(m);
      this._remove.add(m);
    }
    if (this.pending >= this.maxBuffer) this._maybeFlush();
  }

  /** @private */
  _maybeFlush() {
    if (this._flushing || this.pending === 0) return;
    this.flush().catch((err) => this.emit('error', err));
  }

  /**
   * Apply the buffered delta immediately. Safe to call directly; overlapping
   * calls are serialized.
   * @returns {Promise<import('./intraslot').PublishResult|null>} null if nothing was pending
   */
  async flush() {
    if (this._flushing) {
      // Wait for the in-flight flush rather than racing it.
      await this._inflight;
      return null;
    }
    if (this.pending === 0) return null;

    const add = this._add;
    const remove = this._remove;
    this._add = new Set();
    this._remove = new Set();
    this._flushing = true;

    this._inflight = (async () => {
      try {
        const result = await this.fanout.applyDelta({ add, remove });
        /**
         * @event Coalescer#flush
         * @type {{ added: number, removed: number, ms: number, failed: number }}
         */
        this.emit('flush', {
          added: add.size,
          removed: remove.size,
          ms: result.ms,
          failed: result.failed,
        });
        return result;
      } catch (err) {
        // Put the delta back so the next tick retries it, without clobbering
        // changes queued while this flush was in flight.
        for (const m of add) if (!this._remove.has(m)) this._add.add(m);
        for (const m of remove) if (!this._add.has(m)) this._remove.add(m);
        throw err;
      } finally {
        this._flushing = false;
      }
    })();

    return this._inflight;
  }

  /**
   * Stop ticking and flush whatever remains.
   * @returns {Promise<void>}
   */
  async close() {
    if (this._closed) return;
    this._closed = true;
    if (this._timer) clearInterval(this._timer);
    this._timer = null;
    if (this._flushing) await this._inflight.catch(() => {});
    if (this.pending) await this.flush();
  }
}

module.exports = { Coalescer };
