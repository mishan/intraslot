# cluster-key-slot: empty hash tag produces the wrong slot

Repro, patch and validation for a bug in
[`cluster-key-slot`](https://github.com/invertase/cluster-key-slot) (v1.1.2, and
unchanged on `master` as of this writing). Filed here because `intraslot` had to
work around it; intended to be sent upstream.

## Summary

Redis treats an *empty* hash tag as no tag at all. From `keyHashSlot()` in
`src/cluster.c`:

```c
for (s = 0; s < keylen; s++)
    if (key[s] == '{') break;
if (s == keylen) return crc16(key,keylen) & 0x3FFF;      /* no '{' */

for (e = s+1; e < keylen; e++)
    if (key[e] == '}') break;
if (e == keylen || e == s+1) return crc16(key,keylen) & 0x3FFF;  /* no '}', or "{}" */

return crc16(key+s+1,e-s-1) & 0x3FFF;
```

The `e == s+1` case — `{}` — hashes the **whole key** and stops looking.

`cluster-key-slot`'s single-pass scanner handles that case by *not returning*,
but it also does not clear `start`. So it keeps scanning in tag mode and latches
onto the next `}` it finds, hashing whatever sits between:

```js
} else if (char !== 0x7D) {
  resultHash = lookup[...] ^ ...;
} else if (i - 1 !== start) {
  return resultHash & 0x3FFF;
}
// empty tag falls through here with `start` still set -> keeps scanning
```

## Repro

```js
const calculateSlot = require('cluster-key-slot');
calculateSlot('a{}b}c');   // 3300  -- hashes "b"
// redis-cli CLUSTER KEYSLOT "a{}b}c"
// (integer) 2041           -- hashes the whole key
```

More cases, all verified against a live `CLUSTER KEYSLOT` (Redis 8.0.6):

| key | Redis | cluster-key-slot |
|---|---|---|
| `a{}b}c` | 2041 | 3300 |
| `{}a}b` | 5168 | 15495 |
| `x{}}y` | 2083 | 0 |
| `{}{a}` | 13650 | 10276 |
| `:{}1:-0c9--0}_9c8-{b` | 6535 | 2335 |
| `тест{}ы}z` | 3109 | 1781 |

Over a corpus of 8016 brace-heavy keys checked against the server,
`cluster-key-slot@1.1.2` disagreed **460 times**.

## Why it matters downstream

ioredis uses `generateMulti()` to decide whether a multi-key command is legal.
The bug breaks that judgement in *both* directions:

**False accept** — ioredis builds a command the server then rejects:

```
redis    x{}}y -> 2083,  z{}}w -> 11630   (different slots)
upstream generateMulti(['x{}}y','z{}}w']) = 0   (not -1, so allowed through)
=> server replies CROSSSLOT
```

**False reject** — ioredis refuses a command the server would have accepted:

```
redis    a{}b}c -> 2041,  42703 -> 2041   (same slot)
upstream generateMulti(['a{}b}c','42703']) = -1  (wrongly refused)
```

Single-key commands still work, since a mis-routed command is recovered via
`MOVED`, but they pay an extra round trip.

Keys of this shape are not exotic in practice: `user:{}:1` is what you get when
an empty variable is interpolated into a key template.

## Patch

Latch the whole-key fallback when the empty-tag case is seen. One boolean, no
extra pass, no change to the hot path for keys without braces.

```diff
 var generate = module.exports = function generate(str) {
   var char;
   var i = 0;
   var start = -1;
+  var done = false;
   var result = 0;
   var resultHash = 0;
   var utf8 = typeof str === 'string' ? toUTF8Array(str) : str;
   var len = utf8.length;
 
   while (i < len) {
     char = utf8[i++];
-    if (start === -1) {
-      if (char === 0x7B) {
+    if (done || start === -1) {
+      if (!done && char === 0x7B) {
         start = i;
       }
     } else if (char !== 0x7D) {
       resultHash = lookup[(char ^ (resultHash >> 8)) & 0xFF] ^ (resultHash << 8);
     } else if (i - 1 !== start) {
       return resultHash & 0x3FFF;
+    } else {
+      done = true;
     }
 
     result = lookup[(char ^ (result >> 8)) & 0xFF] ^ (result << 8);
   }
 
   return result & 0x3FFF;
 };
```

## Validation

`../../scripts/cks-fix-check.js` runs upstream, the patch, and this package's
implementation against a live server:

```
corpus: 8016 keys, checked against live CLUSTER KEYSLOT

  cluster-key-slot@1.1.2 wrong : 460
  patched generate()     wrong : 0
  intraslot src/slot.js  wrong : 0
```

Run it with:

```sh
bash scripts/with-cluster.sh node scripts/cks-fix-check.js
```

## Suggested upstream test cases

```js
// Empty hash tag means "no tag": hash the whole key and stop searching.
assert.equal(calculateSlot('a{}b}c'), 2041);
assert.equal(calculateSlot('{}a}b'), 5168);
assert.equal(calculateSlot('x{}}y'), 2083);
assert.equal(calculateSlot('{}{a}'), 13650);
assert.equal(calculateSlot('user:{}:1'), 8272);   // already correct; guards the fix
assert.equal(calculateSlot('a{b}c'), 3300);       // normal tag, unchanged
```
