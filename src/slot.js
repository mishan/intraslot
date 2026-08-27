'use strict';
/**
 * Redis Cluster key -> slot math.
 *
 * Implemented here rather than pulled from a dependency so the package has no
 * runtime deps beyond the ioredis peer. Correctness is not taken on faith:
 * test/slot.test.js checks this against `cluster-key-slot` (the implementation
 * ioredis itself uses) over the full table plus hash-tag edge cases.
 *
 * @module intraslot/slot
 */

const NUM_SLOTS = 16384;

// CRC16-XMODEM (CCITT): poly 0x1021, init 0x0000, no reflection, no xorout.
const CRC16_TABLE = (() => {
  const t = new Uint16Array(256);
  for (let i = 0; i < 256; i++) {
    let crc = i << 8;
    for (let bit = 0; bit < 8; bit++) {
      crc = crc & 0x8000 ? ((crc << 1) ^ 0x1021) & 0xffff : (crc << 1) & 0xffff;
    }
    t[i] = crc;
  }
  return t;
})();

/**
 * CRC16-XMODEM over a string, hashed as bytes.
 * @param {string} str
 * @returns {number} 16-bit checksum
 */
function crc16(str) {
  const buf = Buffer.from(str, 'utf8');
  let crc = 0;
  for (let i = 0; i < buf.length; i++) {
    crc = ((crc << 8) ^ CRC16_TABLE[((crc >> 8) ^ buf[i]) & 0xff]) & 0xffff;
  }
  return crc;
}

/**
 * Extract the hash tag from a key, following Redis's exact rule: the substring
 * between the first `{` and the first `}` that follows it. If there is no `}`
 * after the `{`, or the braces are empty, the whole key is hashed.
 *
 * @param {string} key
 * @returns {string} the substring Redis will actually hash
 */
function hashTag(key) {
  const open = key.indexOf('{');
  if (open === -1) return key;
  const close = key.indexOf('}', open + 1);
  if (close === -1 || close === open + 1) return key;
  return key.slice(open + 1, close);
}

/**
 * The slot a key belongs to.
 * @param {string} key
 * @returns {number} slot in [0, 16384)
 */
function slotFor(key) {
  return crc16(hashTag(key)) % NUM_SLOTS;
}

module.exports = { NUM_SLOTS, crc16, hashTag, slotFor };
