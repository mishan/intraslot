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
 * @property {string} [stagingName] name for the staging keys a replacement is
 *   built in; swapping one in needs RENAME and access to `<keyPrefix><stagingName>:*`
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
 * @property {number} [behind] slots still waiting to catch up after this write,
 *   from it or an earlier one
 * @property {Record<number, string>} [errors] the first error for each slot that
 *   failed in this call, including slots behind from earlier writes that it
 *   retried, which are not in `failedSlots`
 */

const DEFAULTS = {
  stagingName: 'intraslot:staging',
  verify: true,
  chunkSize: 2048,
  scriptBudget: 32768,
};

// Errors meaning this server will never run WRITE_SCRIPT for us.
const SCRIPT_REFUSED =
  /^(NOPERM|ERR unknown command|ERR Error compiling script|ERR Unexpected flag|ERR ACL failure in script|ERR The user executing the script can't run this command)/;

// The slot table, packed so the script can name any replica itself: tags are at
// most TAG_WIDTH digits, space-padded, and TAG_LENGTHS holds each one's length.
const TAG_WIDTH = 6;
const PACKED_TAGS = TAGS.map((t) => t.padEnd(TAG_WIDTH)).join('');
const TAG_LENGTHS = TAGS.map((t) => t.length).join('');

// Works on the replicas in some slot ranges on one node, building their names
// from the embedded table, so neither key names nor a command per replica cross
// the wire. Each replica is replaced, changed by a delta (SREM, then SADD), or
// read back as a digest of its members. A replacement is built in a co-slotted
// staging key and swapped in (UNLINK, RENAME): scripts do not roll back, and in
// a slot mid-migration the first write to a key that is not there fails, so
// writing the staging key first leaves the old replica whole if the call fails.
// A script runs atomically, so readers never see a replica half-replaced.
// UNLINK rather than DEL so a large old set is freed in the background instead
// of inside the call, which scriptBudget does not count. No keys are declared because
// allow-cross-slot-keys still rejects declared KEYS from different slots, and
// the same rule keeps COPY from cloning one replica into another.
// unpack() is chunked to stay under Lua's C stack limit (~8000 values).
// A digest is "<cardinality>:<XOR of the first 64 bits of each member's SHA-1>",
// so it ignores member order and does not depend on the server's collation
// locale, as sorting the members would. A replica that cannot be read digests
// as "?". A digest call stops once it has read nremove members (at least one
// replica), so the caller learns how far it got from the length of the reply.
// ARGV: key prefix + name, op (1 replace, 0 delta, d digest), nranges, nremove,
//       nadd, key prefix + staging name, (first slot, last slot) x nranges,
//       removals..., additions...
const SCRIPT_BODY = `
local TAGS, LENGTHS = "${PACKED_TAGS}", "${TAG_LENGTHS}"
local head, op = ARGV[1] .. ':{', ARGV[2]
local ng, nr, na = tonumber(ARGV[3]), tonumber(ARGV[4]), tonumber(ARGV[5])
local stage = ARGV[6] .. ':{'
local base = 7 + 2 * ng
local function apply(cmd, key, from, n)
  for i = from, from + n - 1, 4096 do
    redis.call(cmd, key, unpack(ARGV, i, math.min(i + 4095, from + n - 1)))
  end
end
local function digest(key)
  local members = redis.pcall('SMEMBERS', key)
  if members.err then return '?', 1 end
  local h = {0, 0}
  for _, m in ipairs(members) do
    local sha = redis.sha1hex(m)
    for w = 1, 2 do h[w] = bit.bxor(h[w], tonumber(string.sub(sha, 8 * w - 7, 8 * w), 16)) end
  end
  for w = 1, 2 do h[w] = bit.tohex(h[w]) end
  return #members .. ':' .. table.concat(h), #members
end
local digests, read = {}, 0
for g = 0, ng - 1 do
  for slot = tonumber(ARGV[7 + 2 * g]), tonumber(ARGV[8 + 2 * g]) do
    local at = slot * ${TAG_WIDTH} + 1
    local len = string.byte(LENGTHS, slot + 1) - 48
    local tag = string.sub(TAGS, at, at + len - 1) .. '}'
    local key = head .. tag
    if op == 'd' then
      local d, n = digest(key)
      digests[#digests + 1] = d
      read = read + math.max(1, n)
      if read >= nr then return digests end
    elseif op == '1' and na > 0 then
      local staging = stage .. tag
      -- The old replica is unlinked before the RENAME, so the swap is only
      -- safe if the RENAME is allowed. Without it, or without access to the
      -- staging key, replace in place, which a failure mid-migration can
      -- leave empty.
      -- pcall: acl_check_cmd raises, rather than answering, for a RENAME that
      -- rename-command has renamed or disabled.
      local checked, allowed = pcall(redis.acl_check_cmd, 'RENAME', staging, key)
      if checked and allowed then
        redis.call('UNLINK', staging)
        apply('SADD', staging, base + nr, na)
        redis.call('UNLINK', key)
        redis.call('RENAME', staging, key)
      else
        redis.call('UNLINK', key)
        apply('SADD', key, base + nr, na)
      end
    elseif op == '1' then
      -- One key per command: a multi-key command with a key missing is refused
      -- mid-migration, and the staging key is usually missing. Skip the
      -- staging key for a user who may not touch it.
      redis.call('UNLINK', key)
      local checked, allowed = pcall(redis.acl_check_cmd, 'UNLINK', stage .. tag)
      if checked and allowed then redis.call('UNLINK', stage .. tag) end
    else
      apply('SREM', key, base, nr)
      apply('SADD', key, base + nr, na)
    end
  end
end
return op == 'd' and digests or 1`;
const WRITE_SCRIPT = `#!lua flags=allow-cross-slot-keys${SCRIPT_BODY}`;
const WRITE_SHA = crypto.createHash('sha1').update(WRITE_SCRIPT).digest('hex');
// Digests only, flagged no-writes so it still runs on a master refusing writes
// (OOM, MISCONF, too few replicas).
const READ_SCRIPT = `#!lua flags=no-writes,allow-cross-slot-keys${SCRIPT_BODY}`;
const READ_SHA = crypto.createHash('sha1').update(READ_SCRIPT).digest('hex');

/** In-flight SCRIPT LOADs, by server address and script. @type {Map<string, Promise<string>>} */
const loading = new Map();

/**
 * SCRIPT LOAD `script` on `node`, sharing one load among every call that found
 * it missing, from any instance, rather than resending the ~115 KB body with
 * each.
 * @param {Redis} node
 * @param {string} script
 * @returns {Promise<string>}
 */
function loadScript(node, script) {
  // Per user too: one whose ACL refuses SCRIPT LOAD must not fail another's.
  const user = node.options.username || '';
  const id = `${node.options.host}:${node.options.port} ${user} ${script === READ_SCRIPT ? 'read' : 'write'}`;
  let load = loading.get(id);
  if (!load) {
    load = node.script('LOAD', script).finally(() => loading.delete(id));
    loading.set(id, load);
  }
  return load;
}

/**
 * EVALSHA `script` on `node`, or through the cluster client when `routeKey` is
 * given, loading it first where it is missing.
 * @param {Redis | Cluster} target
 * @param {string} script
 * @param {(string|number)[]} args
 * @param {string} [routeKey] a key declared only so the cluster client routes the call
 * @returns {Promise<any>}
 */
function evalScript(target, script, args, routeKey) {
  const sha = script === READ_SCRIPT ? READ_SHA : WRITE_SHA;
  const keys = routeKey ? [1, routeKey] : [0];
  return target.evalsha(sha, ...keys, ...args).catch(async (err) => {
    if (!/^NOSCRIPT/.test(err.message)) throw err;
    // Through the cluster client the node is not known, so send the body once.
    if (routeKey) return target.eval(script, ...keys, ...args);
    await loadScript(target, script);
    return target.evalsha(sha, ...keys, ...args);
  });
}

/**
 * A copy of a write's pending changes, safe to fold into.
 * @param {{ replace: boolean, add: Set<string>, remove: Set<string> }} write
 */
function copyPending(write) {
  return { replace: write.replace, add: new Set(write.add), remove: new Set(write.remove) };
}

/**
 * Fold a later delta into what a slot is waiting for, last write winning per
 * member. Into a pending replacement it edits the members to write.
 * @param {{ replace: boolean, add: Set<string>, remove: Set<string> }} pending
 * @param {{ add: Set<string>, remove: Set<string> }} delta
 */
function foldInto(pending, delta) {
  for (const m of delta.remove) {
    pending.add.delete(m);
    if (!pending.replace) pending.remove.add(m);
  }
  for (const m of delta.add) {
    pending.remove.delete(m);
    pending.add.add(m);
  }
}

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
    // An option passed as undefined, say from an unset config value, means the default.
    const given = Object.entries(options).filter(([, v]) => v !== undefined);
    this.opts = { ...DEFAULTS, ...Object.fromEntries(given) };
    if (this.opts.verify) verifyTable();

    /** All 16384 replica keys, precomputed once. @type {string[]} */
    this.keys = TAGS.map((t) => `${this.name}:{${t}}`);

    /** The prefix ioredis adds to keys sent through the cluster client. @type {string} */
    this._keyPrefix = cluster.options?.keyPrefix || '';
    /** Tail of the write queue. @type {Promise<unknown>} */
    this._lastWrite = Promise.resolve();
    /** Cleared once a server shows it cannot run WRITE_SCRIPT. @type {boolean} */
    this._useScript = true;
    /**
     * Groups of slots that missed writes, each with what it still needs.
     * @type {{ slots: number[], replace: boolean, add: Set<string>, remove: Set<string> }[]}
     */
    this._behind = [];
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
   * through the cluster client. (A replace in a slot mid-migration can still
   * fail, on ASK or TRYAGAIN, until its keys move; publish, clear and reconcile
   * retry it for a while.) This exists purely so writes can be pipelined.
   *
   * @private
   * @param {Set<number>} [only] group just these slots
   * @returns {Map<Redis, number[]>} node -> slot numbers
   */
  _groupSlotsByNode(only) {
    const masters = this.cluster.nodes('master');
    /** @type {Map<string, Redis>} */
    const byAddr = new Map();
    for (const n of masters) byAddr.set(`${n.options.host}:${n.options.port}`, n);

    /** @type {Map<Redis, number[]>} */
    const groups = new Map();
    const slotMap = this.cluster.slots;
    for (let slot = 0; slot < NUM_SLOTS; slot++) {
      if (only && !only.has(slot)) continue;
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

  /**
   * Run `build(pipeline, slot, keyPrefix)` across every replica, batched per
   * node. With `write`, each node applies it in script calls covering many
   * replicas each, and `build` is only the fallback. Callers serialize.
   * @private
   * @param {(p: any, slot: number, keyPrefix: string) => void} build
   * @param {{ replace?: boolean, remove: string[], add: string[] }} [write]
   * @param {Set<number>} [only] write just these slots
   * @returns {Promise<PublishResult>}
   */
  async _fanoutNow(build, write, only) {
    const t0 = process.hrtime.bigint();
    const groups = this._groupSlotsByNode(only);
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
    /** @type {Record<number, string>} */
    const errors = {};

    for (const { slots, results, error } of settled) {
      if (error) {
        failedSlots.push(...slots);
        for (const slot of slots) errors[slot] = error.message;
        continue;
      }
      if (!results) continue;
      // Each slot contributed a fixed number of commands; find which failed.
      const perSlot = results.length / slots.length;
      results.forEach(([err], idx) => {
        if (err) {
          const slot = slots[Math.floor(idx / perSlot)];
          if (!(slot in errors)) {
            failedSlots.push(slot);
            errors[slot] = err.message;
          }
        }
      });
    }

    const ms = Number(process.hrtime.bigint() - t0) / 1e6;
    return {
      replicas: (only ? only.size : NUM_SLOTS) - failedSlots.length,
      failed: failedSlots.length,
      failedSlots,
      errors,
      ms,
    };
  }

  /**
   * Apply `write` to `batch` in one script call, replaying it through
   * `viaPipeline` on any error.
   * @private
   */
  async _viaScript(node, batch, write, viaPipeline) {
    try {
      await this._script(node, batch, write.replace ? '1' : '0', write.remove, write.add);
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
   * Run WRITE_SCRIPT with `op` on `slots`, all owned by `node`.
   * @private
   */
  _script(node, slots, op, remove = [], add = []) {
    const args = this._argv(op, slots, remove.length, add.length, [...remove, ...add]);
    return evalScript(node, WRITE_SCRIPT, args);
  }

  /**
   * The script's ARGV for `slots`.
   * @private
   * @returns {(string|number)[]}
   */
  _argv(op, slots, nremove, nadd, values = []) {
    const ranges = slotRanges(slots);
    // ioredis prefixes declared KEYS only, and the script builds its own.
    return [
      this._keyPrefix + this.name, op, ranges.length / 2, nremove, nadd,
      this._keyPrefix + this.opts.stagingName, ...ranges, ...values,
    ];
  }

  // -- writes --------------------------------------------------------------

  /** Slots that missed a write and are waiting to catch up. @returns {number} */
  get behind() {
    return this._behind.reduce((n, group) => n + group.slots.length, 0);
  }

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
   * When every slot it misses failed because a slot migration is under way,
   * they are retried up to `retries` times over about a second before it
   * reports them; any other failure among them means they are reported at
   * once. Either way they stay behind, so later writes keep retrying them.
   *
   * @param {Iterable<string>} members
   * @param {{ throwOnPartial?: boolean, retries?: number }} [opts] retries
   *   defaults to 3; 0 reports migrating slots at once
   * @returns {Promise<PublishResult>}
   */
  async publish(members, opts = {}) {
    const t0 = process.hrtime.bigint();
    const write = { replace: true, add: new Set(members), remove: new Set() };
    const result = await this._retryMigrations(await this._write(write), opts.retries ?? 3, NUM_SLOTS);
    result.ms = Number(process.hrtime.bigint() - t0) / 1e6;
    if (result.failed && opts.throwOnPartial !== false) {
      const causes = result.failedSlots.slice(0, 5).map((slot) => new Error(`slot ${slot}: ${result.errors[slot]}`));
      throw new PartialPublishError(result.failed, NUM_SLOTS, causes);
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
   * @param {{ slots?: Iterable<number> }} [opts] apply to just these slots
   * @returns {Promise<PublishResult>}
   */
  async applyDelta(delta, opts = {}) {
    const add = new Set(delta.add || []);
    const remove = new Set([...(delta.remove || [])].filter((m) => !add.has(m)));
    const only = opts.slots ? new Set(opts.slots) : undefined;
    for (const slot of only || []) {
      if (!Number.isInteger(slot) || slot < 0 || slot >= NUM_SLOTS) {
        throw new RangeError(`intraslot: ${JSON.stringify(slot)} is not a slot number`);
      }
    }
    if (!add.size && !remove.size) {
      return { replicas: only ? only.size : NUM_SLOTS, failed: 0, failedSlots: [], ms: 0, behind: this.behind };
    }
    return this._write({ replace: false, add, remove }, only);
  }

  /**
   * Delete every replica, retrying slots caught mid-migration as publish does.
   * @param {{ retries?: number }} [opts]
   * @returns {Promise<PublishResult>}
   */
  async clear(opts = {}) {
    const t0 = process.hrtime.bigint();
    const write = { replace: true, add: new Set(), remove: new Set() };
    const result = await this._retryMigrations(await this._write(write), opts.retries ?? 3, NUM_SLOTS);
    result.ms = Number(process.hrtime.bigint() - t0) / 1e6;
    return result;
  }

  /**
   * Retry the slots that missed earlier writes, without writing anything new.
   * Every write retries them too; this is for when there is nothing to write.
   * @param {Iterable<number>} [slots] retry just these of them
   * @returns {Promise<{ behind: number, errors: Record<number, string> }>} slots
   *   still behind afterwards, and why the retried ones failed
   */
  catchUp(slots) {
    const only = slots ? new Set(slots) : undefined;
    return this._serialize(async () => {
      const { behind, errors } = await this._writeNow(null, only);
      return { behind, errors };
    });
  }

  /**
   * Retry, through catchUp(), the slots a replacement missed while a slot
   * migration made them fail (TRYAGAIN, ASK or MOVED once replayed), `retries`
   * times, waiting 100, 300, then 600 ms before each (about a second for the
   * default 3). Any other failure, such as a timeout, is returned at once:
   * waiting it out again would only delay the caller.
   * @private
   * @param {PublishResult & { errors: Record<number, string> }} result
   * @param {number} retries
   * @param {number} total slots the write targeted
   */
  async _retryMigrations(result, retries, total) {
    let { failedSlots, errors } = result;
    const migrating = () => failedSlots.every((slot) => /^(TRYAGAIN|ASK|MOVED) /.test(errors[slot] || ''));
    for (let attempt = 0; attempt < retries && failedSlots.length && migrating(); attempt++) {
      await new Promise((resolve) => setTimeout(resolve, [100, 300, 600][attempt] ?? 600));
      ({ errors } = await this.catchUp(failedSlots));
      const behind = new Set(this._behind.flatMap((group) => group.slots));
      failedSlots = failedSlots.filter((slot) => behind.has(slot));
    }
    const kept = Object.fromEntries(failedSlots.map((slot) => [slot, errors[slot] || result.errors[slot]]));
    return {
      ...result,
      failedSlots,
      failed: failedSlots.length,
      replicas: total - failedSlots.length,
      behind: this.behind,
      errors: kept,
    };
  }

  /** @private */
  _write(write, targets) {
    return this._serialize(() => this._writeNow(write, targets));
  }

  /**
   * Apply `write` (a replacement or a delta) to `targets`, every slot when
   * omitted, then retry the slots that missed earlier writes.
   *
   * A slot that misses a write is kept behind with what it missed. A later
   * delta is folded into that rather than sent to it, so when the slot catches
   * up it ends as if it had seen every write in order; a later replacement
   * supersedes it. This holds for every write through this instance, which is
   * why it lives here rather than in the coalescer.
   *
   * @private
   * @param {{ replace: boolean, add: Set<string>, remove: Set<string> } | null} write
   * @param {Set<number>} [targets]
   * @returns {Promise<PublishResult & { behind: number, errors: Record<number, string> }>}
   *   the result for `write`, with `errors` also covering the retried slots
   */
  async _writeNow(write, targets) {
    const covers = (slot) => !targets || targets.has(slot);
    /** @type {{ slots: number[], replace: boolean, add: Set<string>, remove: Set<string> }[]} */
    const retry = [];
    /** @type {{ slots: number[], replace: boolean, add: Set<string>, remove: Set<string> }[]} */
    const next = [];
    /** @type {Set<number>} */
    const folded = new Set();
    for (const group of this._behind) {
      if (!write) {
        // Catching up: retry the targeted slots and keep the rest as they are.
        const mine = group.slots.filter(covers);
        const others = targets ? group.slots.filter((slot) => !covers(slot)) : [];
        if (mine.length) retry.push({ ...group, slots: mine });
        if (others.length) next.push({ ...group, slots: others });
        continue;
      }
      const covered = write ? group.slots.filter(covers) : [];
      const rest = covered.length ? group.slots.filter((slot) => !covers(slot)) : group.slots;
      if (rest.length) retry.push({ ...group, slots: rest });
      if (!covered.length || write.replace) continue;
      // Copy before folding when the group was split, so the halves stay apart.
      const own = rest.length ? copyPending(group) : group;
      foldInto(own, write);
      retry.push({ ...own, slots: covered });
      for (const slot of covered) folded.add(slot);
    }

    let result = null;
    if (write) {
      let only = targets;
      if (folded.size) {
        only = new Set();
        for (let slot = 0; slot < NUM_SLOTS; slot++) if (covers(slot) && !folded.has(slot)) only.add(slot);
      }
      result = await this._apply(write, only);
      if (result.failedSlots.length) next.push({ ...copyPending(write), slots: result.failedSlots });
    }
    /** @type {Record<number, string>} */
    const errors = { ...(result && result.errors) };
    for (const group of retry) {
      const retried = await this._apply(group, new Set(group.slots));
      if (retried.failedSlots.length) next.push({ ...group, slots: retried.failedSlots });
      Object.assign(errors, retried.errors);
    }
    this._behind = next;
    return { ...result, behind: this.behind, errors };
  }

  /**
   * Write `write` to `only`, every slot when omitted.
   * @private
   */
  _apply(write, only) {
    const add = [...write.add];
    if (write.replace) {
      return this._fanoutNow((p, slot, pre) => {
        const key = pre + this.keys[slot];
        const staging = `${pre}${this.opts.stagingName}:{${TAGS[slot]}}`;
        if (!add.length) {
          // Not the staging key too: a user may lack access to it.
          p.del(key);
          return;
        }
        p.del(staging);
        p.sadd(staging, ...add);
        p.rename(staging, key);
        // Removes the copy when the RENAME failed; after one that worked, a no-op.
        p.del(staging);
      }, { replace: true, remove: [], add }, only);
    }
    const remove = [...write.remove];
    return this._fanoutNow((p, slot, pre) => {
      const key = pre + this.keys[slot];
      if (remove.length) p.srem(key, ...remove);
      if (add.length) p.sadd(key, ...add);
    }, { remove, add }, only);
  }

  // -- operations ----------------------------------------------------------

  /**
   * Sample replicas and report divergence. A partial publish, a node restored
   * from an old snapshot, or a crashed publisher all show up as drift.
   *
   * Replicas are compared by a digest of their members, so one holding the
   * right number of the wrong members is caught too. On a server that refuses
   * scripts only cardinalities are compared, and `contents` is false.
   *
   * @param {{ sample?: number }} [opts] slots to sample (default 256; 0 = all)
   * @returns {Promise<{ sampled: number, cardinalities: Record<number, number>,
   *   majority: number, drifted: number[], consistent: boolean, contents: boolean }>}
   */
  async verify(opts = {}) {
    const n = opts.sample === 0 ? NUM_SLOTS : opts.sample || 256;
    const step = Math.max(1, Math.floor(NUM_SLOTS / n));
    /** @type {number[]} */
    const slots = [];
    for (let s = 0; s < NUM_SLOTS; s += step) slots.push(s);

    const contents = this._useScript;
    const states = contents
      ? await this._digests(slots)
      : await Promise.all(slots.map((s) => this.cluster.scard(this.keys[s]).then(String, () => '?')));
    // A server that refused the script partway leaves no digests to compare.
    if (contents && !this._useScript) return this.verify(opts);

    /** @type {Record<number, number>} */
    const histogram = {};
    /** @type {Map<string, number>} */
    const tally = new Map();
    for (const state of states) {
      if (state === '?') continue;
      const card = parseInt(state, 10);
      histogram[card] = (histogram[card] || 0) + 1;
      tally.set(state, (tally.get(state) || 0) + 1);
    }
    // Rather than report all-unreadable as consistent, or flag every replica.
    if (!tally.size) throw new Error('intraslot: verify could not read any sampled replica');
    const common = [...tally].sort((a, b) => b[1] - a[1])[0][0];
    // An unreadable replica is never in the majority, so it counts as drifted.
    const drifted = slots.filter((_, i) => states[i] !== common);

    return {
      sampled: slots.length,
      cardinalities: histogram,
      majority: parseInt(common, 10),
      drifted,
      consistent: drifted.length === 0,
      contents,
    };
  }

  /**
   * Each slot's digest, in order, or "?" for a slot whose replica could not be
   * read.
   * @private
   * @param {number[]} slots
   * @returns {Promise<string[]>}
   */
  async _digests(slots) {
    // Digesting a member costs up to about 16 writes (measured: a SHA-1 and some
    // Lua per member), so a call stops after scriptBudget / 16 members.
    const budget = Math.max(1, Math.floor(this.opts.scriptBudget / 16));
    /** @type {Map<number, string>} */
    const bySlot = new Map();
    /** @type {number[]} */
    const retry = [];
    // One call in flight per node. Calls this small would otherwise queue in the
    // node's input buffer and run back to back, starving its cluster bus for
    // long enough that the cluster marks it failed.
    const jobs = [...this._groupSlotsByNode(new Set(slots))].map(async ([node, owned]) => {
      // Unknown owner (mid-failover, say): let the cluster client route each.
      if (!node) return void retry.push(...owned);
      // Send about as many slots as the last call got through, not chunkSize
      // every time when replicas are large.
      let size = this.opts.chunkSize;
      for (let i = 0; i < owned.length; ) {
        const batch = owned.slice(i, i + size);
        let out;
        try {
          out = await evalScript(node, READ_SCRIPT, this._argv('d', batch, budget, 0));
        } catch (err) {
          // The node itself failed (down, busy, refusing scripts): its slots are
          // unreadable now, and retrying each through the cluster would only
          // wait out the client's retries slot by slot.
          if (SCRIPT_REFUSED.test(err.message)) this._useScript = false;
          return;
        }
        out.forEach((state, j) => (state === '?' ? retry.push(batch[j]) : bySlot.set(batch[j], state)));
        i += out.length;
        size = Math.min(this.opts.chunkSize, 2 * out.length);
      }
    });
    await Promise.all(jobs);
    // Then a few at a time through the cluster client, which follows
    // redirects, for replicas the script could not read where it ran.
    for (let i = 0; i < retry.length && this._useScript; i += 8) {
      await Promise.all(retry.slice(i, i + 8).map(async (slot) => {
        const args = this._argv('d', [slot], budget, 0);
        const out = await evalScript(this.cluster, READ_SCRIPT, args, this.keys[slot]).catch((err) => {
          if (SCRIPT_REFUSED.test(err.message)) this._useScript = false;
          return ['?'];
        });
        bySlot.set(slot, out[0]);
      }));
    }
    return slots.map((slot) => bySlot.get(slot) || '?');
  }

  /**
   * Repair drifted replicas by republishing to them only.
   * Slots caught mid-migration are retried as publish does.
   * @param {Iterable<string>} members the authoritative set
   * @param {number[]} [slots] slots to repair; defaults to those verify() flags
   * @param {{ retries?: number }} [opts]
   * @returns {Promise<{ repaired: number[] }>}
   */
  async reconcile(members, slots, opts = {}) {
    const { targets, result } = await this._serialize(() => this._reconcileNow(members, slots));
    if (!targets.length) return { repaired: [] };
    const { failedSlots } = await this._retryMigrations(result, opts.retries ?? 3, targets.length);
    const failed = new Set(failedSlots);
    return { repaired: targets.filter((slot) => !failed.has(slot)) };
  }

  /** @private */
  async _reconcileNow(members, slots) {
    let targets = slots;
    if (!targets) {
      const report = await this.verify({ sample: 0 });
      targets = report.drifted;
    }
    if (!targets.length) return { targets, result: null };

    const write = { replace: true, add: new Set(members), remove: new Set() };
    return { targets, result: await this._writeNow(write, new Set(targets)) };
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

// Assigned one by one: tsc cannot emit declarations for a module.exports object
// holding a class with private members.
exports.IntraSlot = IntraSlot;
exports.slotRanges = slotRanges;
