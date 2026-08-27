'use strict';
/**
 * The canonical slot table.
 *
 * For each of the 16384 slots, the lowest positive integer whose CRC16 lands in
 * that slot. This is a pure consequence of the cluster spec -- CRC16-XMODEM and
 * the fixed slot count -- so it is a permanent constant, generated once by
 * scripts/gen-slot-table.js and committed.
 *
 * A changed table means a corrupted artifact, never an update, so the digest is
 * verified on load.
 *
 * @module intraslot/table
 */

const crypto = require('crypto');
const { NUM_SLOTS, slotFor } = require('./slot');
const raw = require('../data/slot-table.json');

/** @type {string[]} tag for each slot, indexed by slot number */
const TAGS = raw.tags;

/**
 * @returns {string} sha256 over the newline-joined tags
 */
function digest() {
  return crypto.createHash('sha256').update(TAGS.join('\n')).digest('hex');
}

let verified = false;

/**
 * Verify the shipped table is intact. Cheap enough to run at construction, and
 * memoized so repeated instances do not re-pay for it.
 *
 * @param {{ deep?: boolean }} [opts] deep also re-hashes every tag (~16k CRC16s)
 * @throws {Error} if the table is the wrong shape, size, or digest
 */
function verifyTable(opts = {}) {
  if (verified && !opts.deep) return;

  if (!Array.isArray(TAGS) || TAGS.length !== NUM_SLOTS) {
    throw new Error(
      `intraslot: slot table has ${TAGS && TAGS.length} entries, expected ${NUM_SLOTS}`
    );
  }
  if (raw.num_slots !== NUM_SLOTS || raw.algorithm !== 'crc16-xmodem') {
    throw new Error(
      `intraslot: slot table declares ${raw.algorithm}/${raw.num_slots}, ` +
        `expected crc16-xmodem/${NUM_SLOTS}`
    );
  }
  const got = digest();
  if (got !== raw.sha256) {
    throw new Error(
      `intraslot: slot table digest mismatch.\n` +
        `  expected ${raw.sha256}\n  got      ${got}\n` +
        `The table is a constant; a mismatch means the file is corrupt.`
    );
  }
  if (opts.deep) {
    for (let s = 0; s < NUM_SLOTS; s++) {
      if (slotFor(TAGS[s]) !== s) {
        throw new Error(`intraslot: tag ${TAGS[s]} does not map to slot ${s}`);
      }
    }
  }
  verified = true;
}

/**
 * The canonical tag for a slot.
 * @param {number} slot
 * @returns {string}
 */
function tagForSlot(slot) {
  if (!Number.isInteger(slot) || slot < 0 || slot >= NUM_SLOTS) {
    throw new RangeError(`intraslot: slot ${slot} out of range [0, ${NUM_SLOTS})`);
  }
  return TAGS[slot];
}

module.exports = { TAGS, tagForSlot, verifyTable, digest, meta: raw };
