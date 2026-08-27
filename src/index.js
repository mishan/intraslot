'use strict';
/**
 * intraslot -- server-side set intersection against a replicated set on Redis Cluster.
 *
 * @module intraslot
 */

const { IntraSlot } = require('./intraslot');
const { Coalescer } = require('./coalescer');
const { CrossSlotError, PartialPublishError } = require('./errors');
const { NUM_SLOTS, crc16, hashTag, slotFor } = require('./slot');
const { TAGS, tagForSlot, verifyTable, digest } = require('./table');

module.exports = {
  IntraSlot,
  Coalescer,
  CrossSlotError,
  PartialPublishError,
  // slot math, exported because it is useful on its own
  NUM_SLOTS,
  crc16,
  hashTag,
  slotFor,
  // the canonical table
  TAGS,
  tagForSlot,
  verifyTable,
  digest,
};
