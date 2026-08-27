'use strict';
/** @module intraslot/errors */

/**
 * Thrown when keys handed to a multi-key operation are not in the same slot.
 *
 * Deliberately verbose: this is the exact confusion the library exists to
 * resolve, and Redis's own `CROSSSLOT` message does not explain that same-node
 * is insufficient.
 */
class CrossSlotError extends Error {
  /**
   * @param {Array<{ key: string, slot: number }>} keys
   */
  constructor(keys) {
    const detail = keys.map((k) => `  slot ${String(k.slot).padStart(5)}  ${k.key}`).join('\n');
    super(
      `intraslot: keys do not share a slot, so Redis Cluster will reject this ` +
        `command with CROSSSLOT.\n${detail}\n` +
        `Note that being on the same *node* is not sufficient -- Redis checks the ` +
        `slot number. Use intraslot.keyFor(yourKey) to build a co-slotted key.`
    );
    this.name = 'CrossSlotError';
    this.keys = keys;
  }
}

/** Thrown when a fanout write did not reach every replica. */
class PartialPublishError extends Error {
  /**
   * @param {number} failed
   * @param {number} total
   * @param {Error[]} causes
   */
  constructor(failed, total, causes) {
    super(
      `intraslot: publish reached ${total - failed}/${total} replicas; ` +
        `${failed} failed. Retry or run reconcile(). First cause: ` +
        `${causes[0] && causes[0].message}`
    );
    this.name = 'PartialPublishError';
    this.failed = failed;
    this.total = total;
    this.causes = causes;
  }
}

module.exports = { CrossSlotError, PartialPublishError };
