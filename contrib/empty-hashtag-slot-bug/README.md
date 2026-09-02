# Wrong cluster slot for keys containing an empty hash tag

**Affects:** `ioredis` (through 6.0.0) and `node-redis` / `@redis/client`
(through 6.2.1), via their shared `cluster-key-slot` dependency.

**Status:** fixed in both clients, on `main` ahead of a release — ioredis
[#2172](https://github.com/redis/ioredis/pull/2172) (merged 2026-09-01) and
node-redis [#3431](https://github.com/redis/node-redis/pull/3431) (merged
2026-08-31). Each vendors the slot function with the patch below and drops the
`cluster-key-slot` dependency, since it could not be fixed upstream — see [Why
this had to land in the clients](#why-this-had-to-land-in-the-clients). Releases
up to the versions above still carry the bug.

Written up here because [intraslot](https://github.com/mishan/intraslot) had to
implement slot math itself to work around it. Everything below was measured
against a live Redis 8.0.6 cluster, using `CLUSTER KEYSLOT` as ground truth.

---

## Summary

Redis treats an *empty* hash tag as no tag at all: given `{}` it hashes the whole
key and stops searching. `cluster-key-slot` does not stop — it keeps scanning and
latches onto the next `}` it finds, hashing whatever sits between.

```js
const calculateSlot = require('cluster-key-slot');
calculateSlot('a{}b}c');   // 3300  -- hashes "b"

// redis-cli CLUSTER KEYSLOT "a{}b}c"
// (integer) 2041            -- hashes the whole key
```

Over a corpus of 8016 brace-heavy keys checked against a live server,
`cluster-key-slot` disagreed with Redis **460 times**.

Keys of this shape are not exotic. `user:{}:1` is what you get when an empty
variable is interpolated into a key template — a bug in the caller, but one that
currently produces silent misrouting rather than anything diagnosable.

## The rule being violated

From `keyHashSlot()` in Redis `src/cluster.c`:

```c
for (s = 0; s < keylen; s++)
    if (key[s] == '{') break;
if (s == keylen) return crc16(key,keylen) & 0x3FFF;      /* no '{' */

for (e = s+1; e < keylen; e++)
    if (key[e] == '}') break;
if (e == keylen || e == s+1) return crc16(key,keylen) & 0x3FFF;  /* no '}', or "{}" */

return crc16(key+s+1,e-s-1) & 0x3FFF;
```

`e == s+1` is the `{}` case: hash the **whole key**, and stop looking.

`cluster-key-slot`'s single-pass scanner handles that case by *not returning* —
but it also never clears `start`, so it stays in tag-scanning mode:

```js
} else if (char !== 0x7D) {
  resultHash = lookup[...] ^ ...;
} else if (i - 1 !== start) {
  return resultHash & 0x3FFF;
}
// empty tag falls through here with `start` still set -> keeps scanning,
// and the NEXT '}' returns whatever accumulated after the "{}"
```

## Measured divergence

All verified against a live server with `CLUSTER KEYSLOT` (Redis 8.0.6):

| key | Redis | cluster-key-slot |
|---|---|---|
| `a{}b}c` | 2041 | 3300 |
| `{}a}b` | 5168 | 15495 |
| `x{}}y` | 2083 | 0 |
| `{}{a}` | 13650 | 10276 |
| `:{}1:-0c9--0}_9c8-{b` | 6535 | 2335 |
| `тест{}ы}z` | 3109 | 1781 |

On versions: measurements were taken against `cluster-key-slot@1.1.2`, but
`lib/index.js` is **byte-identical** in `1.1.1` and `1.1.2` (1.1.2 was a license
metadata fix). These results apply unchanged to the `1.1.1` that ioredis pins.

## Where it bites

### ioredis (depends on `cluster-key-slot` `1.1.1`, exact pin)

| call site | effect of a wrong slot |
|---|---|
| `Command.js:101` — `calculateSlot(key)` | command routed to the wrong node; recovered via `MOVED`, at the cost of a round trip |
| `Pipeline.js:273` — `calculateSlot.generateMulti(keys)` | gates each command in a pipeline; misjudges in **both** directions (below) |
| `Pipeline.js:17` — `generateMultiWithNodes()` | maps each key's slot through `_groupsBySlot`, so a wrong slot means a wrong group |
| `autoPipelining.js:133` — `client.slots[calculateSlot(...)]` | auto-pipeline batched against the wrong node |
| `cluster/ClusterSubscriberGroup.js:91,112` | sharded pub/sub channels wrongly judged to share (or not share) a slot |

The `Pipeline.js:273` gate fails both ways:

**False reject** — ioredis refuses a command the server would have accepted:

```
redis    a{}b}c -> 2041,  42703 -> 2041           (same slot)
upstream generateMulti(['a{}b}c','42703']) = -1
  -> "All the keys in a pipeline command should belong to the same slot"
```

**False accept** — ioredis builds a command the server then rejects:

```
redis    x{}}y -> 2083,  z{}}w -> 11630           (different slots)
upstream generateMulti(['x{}}y','z{}}w']) = 0
  -> passes the gate, server replies CROSSSLOT
```

### node-redis / `@redis/client` (depends on `cluster-key-slot` `1.1.2`)

`@redis/client` does **not** use `generateMulti`, so it has no equivalent
client-side gate and none of the false accept/reject behaviour above. Its
exposure is routing and sharded pub/sub only:

| call site | effect |
|---|---|
| `cluster/index.js:390` | slot derived from `parser.firstKey`; a wrong slot routes to the wrong node |
| `cluster/cluster-slots.js:762,776` | same, for the slot→node lookup |
| `cluster/cluster-slots.js:960,992` | `SSUBSCRIBE` / sharded pub/sub resolves the channel to the wrong master |

Single-key misrouting is recoverable through `MOVED`, so the practical impact for
node-redis is latency rather than wrong results — except for sharded pub/sub,
where subscribing on the wrong master means messages are simply never received.

## Why this had to land in the clients

The upstream repo,
[invertase/cluster-key-slot](https://github.com/invertase/cluster-key-slot), was
**archived by its owner on Mar 17, 2026 and is now read-only**, with 3 open
issues and 1 open PR left unresolved. Its last npm publish was November 2022, so
it had been unmaintained in practice for over three years before the archive made
that official.

Even if a fixed version were published, ioredis pins the **exact** version
`"cluster-key-slot": "1.1.1"` — not a range — so no upstream release would reach
users without a change in ioredis regardless.

The function is roughly 40 lines plus a 256-entry lookup table, with no
dependencies, which puts it well inside vendoring range for either client.
That is what both did: ioredis now carries it as `lib/utils/calculateSlot.ts`
and `@redis/client` as `lib/utils/calculate-slot.ts`, each with the patch
applied, unit tests for the empty-tag cases, and parity tests against
upstream's own suite.

## The patch

Latch the whole-key fallback once the empty-tag case is seen. One boolean, no
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

`../../scripts/cks-fix-check.js` runs upstream, the patch, and intraslot's own
implementation against a live server:

```
corpus: 8016 keys, checked against live CLUSTER KEYSLOT

  cluster-key-slot@1.1.2 wrong : 460
  patched generate()     wrong : 0
  intraslot src/slot.js  wrong : 0
```

Reproduce with:

```sh
bash scripts/with-cluster.sh node scripts/cks-fix-check.js
```

## Suggested test cases

```js
// Empty hash tag means "no tag": hash the whole key and stop searching.
assert.equal(calculateSlot('a{}b}c'), 2041);
assert.equal(calculateSlot('{}a}b'), 5168);
assert.equal(calculateSlot('x{}}y'), 2083);
assert.equal(calculateSlot('{}{a}'), 13650);

// Regression guards -- these are already correct today.
assert.equal(calculateSlot('user:{}:1'), 8272);   // empty tag, no later '}'
assert.equal(calculateSlot('a{b}c'), 3300);       // ordinary tag
assert.equal(calculateSlot('a{b{c}d'), 15725);    // first '}' after first '{'
assert.equal(calculateSlot('foo'), 12182);        // no braces
```
