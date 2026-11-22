'use strict';
/** @module intraslot */

const crypto = require('crypto');
const { NUM_SLOTS, slotFor } = require('./slot');
const { TAGS, verifyTable } = require('./table');
const { CrossSlotError, PartialPublishError } = require('./errors');

/**
 * @typedef {import('ioredis').Cluster} Cluster
 * @typedef {import('ioredis').Redis} Redis
 */

/**
 * @typedef {object} IntraSlotOptions
 * @property {string} name         name of the replicated set, e.g. "active".
 *   Replicas are stored as `<name>:{tag}`, one per slot, but the name is the
 *   set's identity -- it is the second operand of every operation below.
 * @property {string} [stagingName] name for staging keys used by atomic publish
 * @property {boolean} [verify]    verify the shipped slot table on construction (default true)
 * @property {number} [chunkSize]  max keys per pipeline or script call (default 2048)
 * @property {number} [scriptBudget] max member writes per script call, which
 *   bounds how long one call blocks a node (default 32768, a few ms)
 */

/**
 * @typedef {object} PublishResult
 * @property {number} replicas  replicas written
 * @property {number} failed    replicas that errored
 * @property {number} ms        wall time
 * @property {number[]} failedSlots
 */

const DEFAULTS = {
  stagingName: 'intraslot:staging',
  verify: true,
  chunkSize: 2048,
  scriptBudget: 32768,
};

// Errors meaning this server will never run WRITE_SCRIPT for us.
const SCRIPT_REFUSED =
  /^(NOPERM|ERR unknown command|ERR Error compiling script|ERR Unexpected flag|ERR ACL failure in script)/;

// The slot table, packed so the script can name any replica itself: tags are at
// most TAG_WIDTH digits, space-padded, and TAG_LENGTHS holds each one's length.
const TAG_WIDTH = 6;
const PACKED_TAGS = TAGS.map((t) => t.padEnd(TAG_WIDTH)).join('');
const TAG_LENGTHS = TAGS.map((t) => t.length).join('');

// Writes the replicas in some slot ranges on one node, building their names
// from the embedded table, so neither key names nor a command per replica cross
// the wire. Each replica is replaced (DEL, then SADD) or changed by a delta
// (SREM, then SADD); a script runs atomically, so readers never see a replica
// half-replaced. No keys are declared because allow-cross-slot-keys still
// rejects declared KEYS from different slots, and the same rule keeps COPY from
// cloning one replica into another.
// unpack() is chunked to stay under Lua's C stack limit (~8000 values).
// ARGV: key prefix + name, replace (1/0), nranges, nremove, nadd,
//       (first slot, last slot) x nranges, removals..., additions...
const WRITE_SCRIPT = `#!lua flags=allow-cross-slot-keys
local TAGS, LENGTHS = "${PACKED_TAGS}", "${TAG_LENGTHS}"
local head, replace = ARGV[1] .. ':{', ARGV[2] == '1'
local ng, nr, na = tonumber(ARGV[3]), tonumber(ARGV[4]), tonumber(ARGV[5])
local base = 6 + 2 * ng
local function apply(cmd, key, from, n)
  for i = from, from + n - 1, 4096 do
    redis.call(cmd, key, unpack(ARGV, i, math.min(i + 4095, from + n - 1)))
  end
end
for g = 0, ng - 1 do
  for slot = tonumber(ARGV[6 + 2 * g]), tonumber(ARGV[7 + 2 * g]) do
    local at = slot * ${TAG_WIDTH} + 1
    local len = string.byte(LENGTHS, slot + 1) - 48
    local key = head .. string.sub(TAGS, at, at + len - 1) .. '}'
    if replace then redis.call('DEL', key) end
    apply('SREM', key, base, nr)
    apply('SADD', key, base + nr, na)
  end
end
return 1`;
const WRITE_SHA = crypto.createHash('sha1').update(WRITE_SCRIPT).digest('hex');

/**
 * Collapse ascending slot numbers into [first, last] runs.
 * @param {number[]} slots
 * @returns {number[]} first0, last0, first1, last1, ...
 */
function slotRanges(slots) {
  const out = [];
  for (const slot of slots) {
    if (out.length && out[out.length - 1] === slot - 1) out[out.length - 1] = slot;
    else out.push(slot, slot);
  }
  return out;
}

/**
 * A set replicated to all 16384 slots, so it can be intersected server-side
 * against any sharded key without a CROSSSLOT error.
 *
 * **The instance is one of the two operands.** Every read below takes a single
 * sharded key and pairs it with this set's replica for that key's slot, so
 * `active.intersect(k)` issues `SINTER k active:{<tag for k's slot>}`.
 *
 * @example
 * const active = new IntraSlot(cluster, { name: 'active' });
 * await active.publish(['alice', 'bob']);
 *
 * const activeSubs = await active.intersect('subs:{1234567890}');
 * // -> SINTER subs:{1234567890} active:{1405}
 * //    both keys are in slot 1405, so the server accepts it
 */
class IntraSlot {
  /**
   * @param {Cluster} cluster an ioredis Cluster instance
   * @param {IntraSlotOptions} options
   */
  constructor(cluster, options) {
    if (!cluster) throw new TypeError('intraslot: a cluster instance is required');
    if (!options || !options.name) {
      throw new TypeError('intraslot: options.name is required');
    }
    /** @type {Cluster} */
    this.cluster = cluster;
    /** Name of this replicated set; also the key prefix. @type {string} */
    this.name = options.name;
    this.opts = { ...DEFAULTS, ...options };
    if (this.opts.verify) verifyTable();

    /** All 16384 replica keys, precomputed once. @type {string[]} */
    this.keys = TAGS.map((t) => `${this.name}:{${t}}`);

    /** The prefix ioredis adds to keys sent through the cluster client. @private */
    this._keyPrefix = cluster.options?.keyPrefix || '';
    /** Tail of the write queue. @private */
    this._lastWrite = Promise.resolve();
    /** Cleared once a server shows it cannot run WRITE_SCRIPT. @private */
    this._useScript = true;
    /** In-flight SCRIPT LOADs, by node. @private @type {Map<Redis, Promise<string>>} */
    this._loading = new Map();
  }

  // -- addressing ----------------------------------------------------------

  /**
   * @param {string} key
   * @returns {number} the slot `key` hashes to
   */
  slotOf(key) {
    return slotFor(key);
  }

  /**
   * The replica key co-slotted with `key`. This is the whole trick: the result
   * is guaranteed to be in the same slot -- and therefore on the same node,
   * under every topology -- as the key you already hold.
   *
   * @param {string} key a key you are already working with, e.g. "subs:{123}"
   * @returns {string} e.g. "active:{1405}"
   */
  keyFor(key) {
    return this.keys[slotFor(key)];
  }

  /**
   * Throw a descriptive error if the given keys are not all in one slot.
   * @param {string[]} keys
   * @returns {number} the shared slot
   */
  assertSameSlot(keys) {
    const withSlots = keys.map((key) => ({ key, slot: slotFor(key) }));
    const slot = withSlots[0].slot;
    if (withSlots.some((k) => k.slot !== slot)) throw new CrossSlotError(withSlots);
    return slot;
  }

  // -- reads (always node-local, no topology knowledge needed) -------------

  /**
   * Members of `key` that are also in this set.
   *
   * The second operand is this instance -- you pass one key, and its co-slotted
   * replica is supplied for you:
   *
   *     active.intersect('subs:{1234567890}')
   *       -> SINTER subs:{1234567890} active:{1405}
   *
   * Only the result crosses the wire, so intersecting a 5,000-member sharded
   * set costs you the matches, not the 5,000.
   *
   * @param {string} key a sharded key you already hold, e.g. "subs:{1234567890}"
   * @param {...string} extra further keys, which must share `key`'s slot
   * @returns {Promise<string[]>}
   */
  async intersect(key, ...extra) {
    const keys = [key, ...extra, this.keyFor(key)];
    this.assertSameSlot(keys);
    return this.cluster.sinter(...keys);
  }

  /**
   * How many members of `key` are also in this set, without transferring them.
   *
   *     active.count('subs:{1234567890}')
   *       -> SINTERCARD 2 subs:{1234567890} active:{1405}
   *
   * @param {string} key
   * @param {{ limit?: number }} [opts] stop counting at `limit`
   * @returns {Promise<number>}
   */
  async count(key, opts = {}) {
    const keys = [key, this.keyFor(key)];
    this.assertSameSlot(keys);
    const args = [String(keys.length), ...keys];
    if (opts.limit != null) args.push('LIMIT', String(opts.limit));
    return this.cluster.call('SINTERCARD', ...args);
  }

  /**
   * Members of `key` that are NOT in this set.
   *
   *     active.without('subs:{1234567890}')
   *       -> SDIFF subs:{1234567890} active:{1405}
   *
   * @param {string} key
   * @returns {Promise<string[]>}
   */
  async without(key) {
    const keys = [key, this.keyFor(key)];
    this.assertSameSlot(keys);
    return this.cluster.sdiff(...keys);
  }

  /**
   * Store the intersection into `dest`, which must share `key`'s slot.
   *
   *     active.intersectInto('out:{1234567890}', 'subs:{1234567890}')
   *       -> SINTERSTORE out:{1234567890} subs:{1234567890} active:{1405}
   *
   * @param {string} dest
   * @param {string} key
   * @returns {Promise<number>} cardinality of the stored result
   */
  async intersectInto(dest, key) {
    const keys = [dest, key, this.keyFor(key)];
    this.assertSameSlot(keys);
    return this.cluster.sinterstore(dest, key, this.keyFor(key));
  }

  // Redis-named aliases, for anyone reaching for the command they already know.
  // Same behaviour; the receiver still supplies the second key.

  /** Alias of {@link IntraSlot#intersect}. @returns {Promise<string[]>} */
  async sinter(key, ...extra) {
    return this.intersect(key, ...extra);
  }

  /** Alias of {@link IntraSlot#count}. @returns {Promise<number>} */
  async sintercard(key, opts = {}) {
    return this.count(key, opts);
  }

  /** Alias of {@link IntraSlot#without}. @returns {Promise<string[]>} */
  async sdiff(key) {
    return this.without(key);
  }

  /** Alias of {@link IntraSlot#intersectInto}. @returns {Promise<number>} */
  async sinterstore(dest, key) {
    return this.intersectInto(dest, key);
  }

  /**
   * Read the replicated set from the node local to `key`.
   * @param {string} key any key whose node you want to read from
   * @returns {Promise<string[]>}
   */
  async smembers(key) {
    return this.cluster.smembers(this.keyFor(key));
  }

  /**
   * @param {string} key
   * @param {string} member
   * @returns {Promise<boolean>}
   */
  async sismember(key, member) {
    return (await this.cluster.sismember(this.keyFor(key), member)) === 1;
  }

  /**
   * @param {string} key
   * @returns {Promise<number>} cardinality of the local replica
   */
  async scard(key) {
    return this.cluster.scard(this.keyFor(key));
  }

  // -- topology (write path only) -----------------------------------------

  /**
   * Group all 16384 replica keys by the node that currently owns them.
   *
   * A stale grouping is harmless: a write that gets MOVED or ASK is replayed
   * through the cluster client. (A publish to a slot mid-migration can still
   * fail, on TRYAGAIN, and is reported in `failedSlots`.) This exists purely so
   * writes can be pipelined.
   *
   * @private
   * @returns {Map<Redis, number[]>} node -> slot numbers
   */
  _groupSlotsByNode() {
    const masters = this.cluster.nodes('master');
    /** @type {Map<string, Redis>} */
    const byAddr = new Map();
    for (const n of masters) byAddr.set(`${n.options.host}:${n.options.port}`, n);

    /** @type {Map<Redis, number[]>} */
    const groups = new Map();
    const slotMap = this.cluster.slots;
    for (let slot = 0; slot < NUM_SLOTS; slot++) {
      const owner = slotMap && slotMap[slot] && slotMap[slot][0];
      const node = owner && byAddr.get(owner);
      // Unknown owner (mid-failover, say): fall through to the cluster client,
      // which will route the command itself.
      const target = node || null;
      if (!groups.has(target)) groups.set(target, []);
      groups.get(target).push(slot);
    }
    return groups;
  }

  /**
   * Run `build(pipeline, slot, keyPrefix)` across every replica, batched per
   * node. With `write`, each node applies it in script calls covering many
   * replicas each, and `build` is only the fallback.
   * @private
   * @param {(p: any, slot: number, keyPrefix: string) => void} build
   * @param {{ replace?: boolean, remove: string[], add: string[] }} [write]
   * @returns {Promise<PublishResult>}
   */
  _fanout(build, write) {
    return this._serialize(() => this._fanoutNow(build, write));
  }

  /**
   * Run `fn` once every write queued before it has settled. Writes run one at a
   * time: a batch's fallback is sent only after its script fails, so it could
   * otherwise land after, and undo, a later write.
   * @private
   * @template T
   * @param {() => Promise<T>} fn
   * @returns {Promise<T>}
   */
  _serialize(fn) {
    const run = this._lastWrite.then(fn);
    this._lastWrite = run.catch(() => {});
    return run;
  }

  /** @private */
  async _fanoutNow(build, write) {
    const t0 = process.hrtime.bigint();
    const groups = this._groupSlotsByNode();
    const { chunkSize, scriptBudget } = this.opts;

    /** @type {Promise<{ slots: number[], results?: any[], error?: Error }>[]} */
    const jobs = [];
    for (const [node, slots] of groups) {
      const target = node || this.cluster;
      // ioredis applies a cluster-level keyPrefix only to commands sent through
      // the cluster client, so a node's own pipeline needs it added by hand.
      const pre = node && !node.options.keyPrefix ? this._keyPrefix : '';
      const viaPipeline = async (batch) => {
        const p = target.pipeline();
        for (const slot of batch) build(p, slot, pre);
        const results = await p.exec();
        if (!node || !results) return { slots: batch, results };
        // A node's own pipeline does not follow redirects, so replay each
        // redirected slot through the cluster client, which does.
        const perSlot = results.length / batch.length;
        await Promise.all(
          batch.map(async (slot, i) => {
            const own = results.slice(i * perSlot, (i + 1) * perSlot);
            if (!own.some(([err]) => err && /^(MOVED|ASK) /.test(err.message))) return;
            const retry = this.cluster.pipeline();
            build(retry, slot, '');
            results.splice(i * perSlot, perSlot, ...(await retry.exec()));
          })
        );
        return { slots: batch, results };
      };
      // A script call blocks its node, so bound each call to scriptBudget member
      // writes. A call covering a single key saves nothing over the pipeline,
      // so very large writes skip the script.
      const membersPerKey = write ? Math.max(1, write.remove.length + write.add.length) : 0;
      const keysPerCall = write ? Math.min(chunkSize, Math.floor(scriptBudget / membersPerKey)) : 0;
      const useScript = Boolean(node && this._useScript && keysPerCall > 1);
      const size = useScript ? keysPerCall : chunkSize;
      for (let i = 0; i < slots.length; i += size) {
        const batch = slots.slice(i, i + size);
        const run = useScript ? this._viaScript(node, batch, write, viaPipeline) : viaPipeline(batch);
        // A batch that throws may have written some slots, but counting them all
        // as failed only costs a needless reconcile.
        jobs.push(run.catch((error) => ({ slots: batch, error })));
      }
    }

    const settled = await Promise.all(jobs);
    /** @type {number[]} */
    const failedSlots = [];
    /** @type {Error[]} */
    const causes = [];

    for (const { slots, results, error } of settled) {
      if (error) {
        failedSlots.push(...slots);
        causes.push(error);
        continue;
      }
      if (!results) continue;
      // Each slot contributed a fixed number of commands; find which failed.
      const perSlot = results.length / slots.length;
      results.forEach(([err], idx) => {
        if (err) {
          const slot = slots[Math.floor(idx / perSlot)];
          if (!failedSlots.includes(slot)) failedSlots.push(slot);
          causes.push(err);
        }
      });
    }

    const ms = Number(process.hrtime.bigint() - t0) / 1e6;
    return {
      replicas: NUM_SLOTS - failedSlots.length,
      failed: failedSlots.length,
      failedSlots,
      ms,
    };
  }

  /**
   * Apply `write` to `batch` in one script call, replaying it through
   * `viaPipeline` on any error.
   * @private
   */
  async _viaScript(node, batch, write, viaPipeline) {
    const ranges = slotRanges(batch);
    // ioredis prefixes declared KEYS only, and the script builds its own.
    const args = [
      this._keyPrefix + this.name, write.replace ? 1 : 0, ranges.length / 2,
      write.remove.length, write.add.length, ...ranges, ...write.remove, ...write.add,
    ];
    try {
      await node.evalsha(WRITE_SHA, 0, ...args).catch(async (err) => {
        if (!/^NOSCRIPT/.test(err.message)) throw err;
        await this._loadScript(node);
        return node.evalsha(WRITE_SHA, 0, ...args);
      });
      return { slots: batch };
    } catch (err) {
      if (SCRIPT_REFUSED.test(err.message)) this._useScript = false;
      // A script aborts on its first error (a migrating slot, a wrong-typed
      // replica), so replay the batch per slot: the delta is idempotent, and
      // errors get pinned to their own slots.
      return viaPipeline(batch);
    }
  }

  /**
   * SCRIPT LOAD the delta script on `node`, sharing one load among every call
   * that found it missing, rather than resending the ~115 KB body with each.
   * @private
   */
  _loadScript(node) {
    let load = this._loading.get(node);
    if (!load) {
      load = node.script('LOAD', WRITE_SCRIPT).finally(() => this._loading.delete(node));
      this._loading.set(node, load);
    }
    return load;
  }

  // -- writes --------------------------------------------------------------

  /**
   * Replace the replicated set everywhere.
   *
   * Each replica is swapped atomically, inside a script call or, on the
   * per-replica fallback, by staging the members under a co-slotted key and
   * RENAMEing it over the live one. Readers see either the old set or the new
   * one, never a half-built one. The fanout as a whole is still
   * eventually consistent -- there is no cross-slot transaction, and there
   * cannot be -- so different nodes may briefly disagree.
   *
   * @param {Iterable<string>} members
   * @param {{ throwOnPartial?: boolean }} [opts]
   * @returns {Promise<PublishResult>}
   */
  async publish(members, opts = {}) {
    const list = [...members];
    const result = await this._fanout((p, slot, pre) => {
      const key = pre + this.keys[slot];
      if (list.length === 0) {
        p.del(key);
        return;
      }
      const staging = `${pre}${this.opts.stagingName}:{${TAGS[slot]}}`;
      p.del(staging);
      p.sadd(staging, ...list);
      p.rename(staging, key);
    }, { replace: true, remove: [], add: list });
    if (result.failed && opts.throwOnPartial !== false) {
      throw new PartialPublishError(result.failed, NUM_SLOTS, [
        new Error(`slots ${result.failedSlots.slice(0, 5).join(', ')} ...`),
      ]);
    }
    return result;
  }

  /**
   * Add members to every replica.
   * @param {Iterable<string>} members
   * @returns {Promise<PublishResult>}
   */
  async add(members) {
    return this.applyDelta({ add: members });
  }

  /**
   * Remove members from every replica.
   * @param {Iterable<string>} members
   * @returns {Promise<PublishResult>}
   */
  async remove(members) {
    return this.applyDelta({ remove: members });
  }

  /**
   * Apply a removal and an addition in one pass. Removals are issued first, so
   * a member that left and rejoined within the same delta ends up present.
   *
   * @param {{ add?: Iterable<string>, remove?: Iterable<string> }} delta
   * @returns {Promise<PublishResult>}
   */
  async applyDelta(delta) {
    const toAdd = [...(delta.add || [])];
    const toRemove = [...(delta.remove || [])].filter((m) => !toAdd.includes(m));
    if (!toAdd.length && !toRemove.length) {
      return { replicas: NUM_SLOTS, failed: 0, failedSlots: [], ms: 0 };
    }
    return this._fanout((p, slot, pre) => {
      const key = pre + this.keys[slot];
      if (toRemove.length) p.srem(key, ...toRemove);
      if (toAdd.length) p.sadd(key, ...toAdd);
    }, { remove: toRemove, add: toAdd });
  }

  /** Delete every replica. @returns {Promise<PublishResult>} */
  async clear() {
    return this._fanout((p, slot, pre) => p.del(pre + this.keys[slot]), {
      replace: true,
      remove: [],
      add: [],
    });
  }

  // -- operations ----------------------------------------------------------

  /**
   * Sample replicas and report divergence. A partial publish, a node restored
   * from an old snapshot, or a crashed publisher all show up as cardinality
   * drift.
   *
   * @param {{ sample?: number }} [opts] slots to sample (default 256; 0 = all)
   * @returns {Promise<{ sampled: number, cardinalities: Record<number, number>,
   *   majority: number, drifted: number[], consistent: boolean }>}
   */
  async verify(opts = {}) {
    const n = opts.sample === 0 ? NUM_SLOTS : opts.sample || 256;
    const step = Math.max(1, Math.floor(NUM_SLOTS / n));
    /** @type {number[]} */
    const slots = [];
    for (let s = 0; s < NUM_SLOTS; s += step) slots.push(s);

    const counts = await Promise.all(slots.map((s) => this.cluster.scard(this.keys[s])));

    /** @type {Record<number, number>} */
    const histogram = {};
    for (const c of counts) histogram[c] = (histogram[c] || 0) + 1;
    const majority = Number(
      Object.entries(histogram).sort((a, b) => b[1] - a[1])[0][0]
    );
    const drifted = slots.filter((_, i) => counts[i] !== majority);

    return {
      sampled: slots.length,
      cardinalities: histogram,
      majority,
      drifted,
      consistent: drifted.length === 0,
    };
  }

  /**
   * Repair drifted replicas by republishing to them only.
   * @param {Iterable<string>} members the authoritative set
   * @param {number[]} [slots] slots to repair; defaults to those verify() flags
   * @returns {Promise<{ repaired: number[] }>}
   */
  reconcile(members, slots) {
    return this._serialize(() => this._reconcileNow(members, slots));
  }

  /** @private */
  async _reconcileNow(members, slots) {
    let targets = slots;
    if (!targets) {
      const report = await this.verify({ sample: 0 });
      targets = report.drifted;
    }
    if (!targets.length) return { repaired: [] };

    const list = [...members];
    await Promise.all(
      targets.map(async (slot) => {
        const key = this.keys[slot];
        const staging = `${this.opts.stagingName}:{${TAGS[slot]}}`;
        if (!list.length) return this.cluster.del(key);
        const p = this.cluster.pipeline();
        p.del(staging);
        p.sadd(staging, ...list);
        p.rename(staging, key);
        return p.exec();
      })
    );
    return { repaired: targets };
  }

  /**
   * A tick-based publisher for high-churn sets. See {@link Coalescer}.
   * @param {import('./coalescer').CoalescerOptions} [opts]
   * @returns {import('./coalescer').Coalescer}
   */
  coalescer(opts) {
    const { Coalescer } = require('./coalescer');
    return new Coalescer(this, opts);
  }
}

module.exports = { IntraSlot, slotRanges };
