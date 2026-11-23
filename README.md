# intraslot

Server-side set intersection against a replicated set on Redis Cluster.

```js
const active = new IntraSlot(cluster, { name: 'active' });

// subs:{user} is sharded. Which of those subscriptions are currently active?
const activeSubs = await active.intersect('subs:{1234567890}');
// -> SINTER subs:{1234567890} active:{1405}   both keys in slot 1405
// -> ['bob', 'erin']   one round trip, node-local, no CROSSSLOT
```

`active` is one of the two operands: you pass the sharded key you already hold,
and the set supplies its own replica for that key's slot.

Redis Cluster will not let you do that by default. `intraslot` makes it legal.

## The problem

You are holding a sharded key and need to consult a small global set before you
act on it — a blocklist, a set of canceled accounts, which entities are currently
available. On a single Redis that is a free `SINTER`. On Redis Cluster the two
keys land in different slots, so you get:

```
(error) CROSSSLOT Keys in request don't hash to the same slot
```

Note **slot**, not node. Two keys on the same physical node in different slots
are still rejected — the slot is the unit of migration, so the server checks the
slot number and nothing else.

That distinction is the name. `CROSSSLOT` is the failure; *intra-slot* — both
keys inside one slot — is the property this library guarantees.

That detail matters if you are coming from twemproxy, where a shard *was* a Redis
instance and same-shard was enough to intersect. Measured on a live 3-node
cluster, intersecting against a "one canonical key per node" replica:

| strategy | node | same node? | same slot? | `SINTER` |
|---|---|---|---|---|
| naive single global key | 7002 | **yes** | no | **CROSSSLOT** |
| one canonical key per **node** | 7002 | **yes** | no | **CROSSSLOT** |
| one canonical key per **slot** | 7002 | yes | **yes** | `['bob','erin']` |

The workaround people reach for — cache the set in the application — works until
the set changes and you need bounded staleness across a fleet, at which point you
have rebuilt cache invalidation. And you still pay to ship the sharded set over
the wire to intersect it locally: 5,000 subscription IDs transferred to find the 20
that matter.

## How it works

Redis maps keys to slots with `CRC16(hashtag) mod 16384`. Both the hash function
and the slot count are frozen by the cluster spec, so the map from tag string to
slot is a permanent constant. Invert it once: for each of the 16384 slots, find
the lowest integer whose CRC16 lands there. That table ships with this package.

Given it, `keyFor(k)` returns a key guaranteed to be in `k`'s own slot — and
therefore on `k`'s own node, under every topology, forever. Write your set under
all 16384 of those keys and every read is node-local and every intersection is
legal.

Two properties fall out:

**The read path needs no topology knowledge.** Hash the key you already have,
index a static array, issue a normal command. No `CLUSTER SLOTS`, no cached node
map, no background refresh, nothing to go stale. If your client can route `k`, it
can route `keyFor(k)`.

**Resharding cannot break coverage.** Every node owns at least one slot, every
slot has a canonical key, so every node has one. Redis migrates the replica along
with its slot, so a reshard moves it for you:

```
BEFORE reshard: slot 8283 on 127.0.0.1:7002   SINTER -> ['bob','erin']  OK
AFTER  reshard: slot 8283 on 127.0.0.1:7003   SINTER -> ['bob','erin']  OK
  no republish, no client change, no table change
```

Pinning replicas to *nodes* instead of slots fails both ways: it cannot
intersect at all, and it loses coverage on 96.7% of simulated reshards.

## Install

```sh
npm install intraslot ioredis
```

`ioredis` is a peer dependency. Node 18+, and Redis 7+ or Valkey 8+: CI runs the
live-cluster tests against Redis 7.0 and 8.10 and Valkey 8.1 and 9.1.

## Usage

```js
const Redis = require('ioredis');
const { IntraSlot } = require('intraslot');

const cluster = new Redis.Cluster([{ host: '127.0.0.1', port: 7000 }]);
const active = new IntraSlot(cluster, { name: 'active' });

// Publish the set to all 16384 replicas.
await active.publish(['alice', 'bob', 'erin']);

// Each of these takes ONE key. `active` is the other operand, and the comment
// shows the command that actually reaches the server.
await active.intersect('subs:{1234567890}');
// -> SINTER subs:{1234567890} active:{1405}          ['bob', 'erin']

await active.count('subs:{1234567890}');
// -> SINTERCARD 2 subs:{1234567890} active:{1405}    2

await active.without('subs:{1234567890}');
// -> SDIFF subs:{1234567890} active:{1405}           ['alice', 'carol', 'dave']

await active.intersectInto('out:{1234567890}', 'subs:{1234567890}');
// -> SINTERSTORE out:{1234567890} subs:{1234567890} active:{1405}

// Or read the replicated set itself, from whichever node a key lives on.
await active.smembers('subs:{1234567890}');
await active.sismember('subs:{1234567890}', 'bob');
```

If you think in Redis commands, `sinter`, `sintercard`, `sdiff` and
`sinterstore` are aliases of the four above and behave identically — the
receiver still supplies the second key.

### High-churn sets

Presence data changes constantly, and fanning out per event costs
`events/sec × 16384` replica writes. Don't. Buffer into ticks — a tick costs
`2 × 16384` replica writes regardless of how many events it absorbed, so write cost
becomes a function of tick rate rather than churn rate:

```js
const c = active.coalescer({ tickMs: 250 });
c.on('flush', (s) => metrics.timing('fanout.tick', s.ms));

onActivate((user) => c.add(user));
onDeactivate((user) => c.remove(user));

await c.close();   // stops ticking, flushes what's buffered
```

| churn | per-event fanout | coalesced @ 250 ms |
|---|---|---|
| 50 events/s | 819,200 writes/s | 131,072 writes/s |
| 1000 events/s | 16,384,000 writes/s | 131,072 writes/s |

Measured by `npm run bench` on a 3-node cluster sharing one sandbox CPU — a
pessimistic floor:

```
initial publish (200 members -> 16384 replicas):  138 ms (~119,000 keys/s)
coalesced delta tick (+6/-4)                   :   23 ms (~1,450,000 replica writes/s)
sustainable tick rate                          : ~44 Hz
verify across 256 sampled replicas             : CONSISTENT
```

So a 250 ms tick has roughly 10x headroom. Writes go through a Lua script
(`allow-cross-slot-keys`, Redis 7+) that embeds the slot table, so each call
names a range of slots and the script builds the replica keys itself. The
members cross the wire once per call, and neither key names nor a command per
replica do: the +6/-4 tick above sends about 2 KB, where per-replica commands
send about 300 KB, and a 200-member publish sends about 0.3 MB instead of 42 MB.
That is most of why publish is about 7x faster than per-replica commands. The
saving is between the client and the masters only: Redis replicates the
commands a script runs, not the call, so replicas and the AOF still receive one
write per replica.

Each call writes at most `scriptBudget` members (default 32768, a few ms of
blocking per call), across at most `chunkSize` replicas. The ~115 KB script is
loaded once per node and called by its SHA after that. A call that fails is
replayed as plain per-replica commands. Writes of more than `scriptBudget / 2`
members, and servers or ACL users that refuse the script, use the plain
commands throughout.

A replacement (`publish`, `reconcile`) is built in a co-slotted staging key and
swapped in, so a slot caught mid-migration keeps its old set rather than
losing it. That needs `RENAME` and read and write access to the staging keys
(`<keyPrefix><stagingName>:*`, by default `intraslot:staging:*`), in the same
ACL selector as the replica keys. Without them, or where `rename-command` has
renamed `RENAME`, the script replaces in place instead, as before this check
existed, and a replacement caught mid-migration can leave that slot's replica
empty until a later write reaches it. The plain per-replica commands always
stage and `RENAME`, so for such a user they fail: replacements of more than
`scriptBudget / 2` members, and a script batch that falls back because one of
its slots is mid-migration, are reported failed (NOPERM) for the whole batch.

Ticks never overlap, and a member added and removed within one tick resolves to
the last write. The cost is staleness bounded by one tick — fine for presence,
wrong wherever a stale read is *incorrect* rather than merely dated. Only busy
sets pay it: a change that arrives when no flush has started for a tick is
applied at once, so a rarely changing set sees its writes land in about one
fanout's time (~20 ms above) rather than up to `tickMs`.

Slots a write fails on, such as those on a node that timed out, are kept
behind by the `IntraSlot` and retried with every later write through it, and by
the coalescer's next tick even when nothing new arrives (or call `catchUp()`).
Until they catch up, later deltas are folded into what they missed instead of
sent to them, so they end up as if they had seen every write in order; a
`publish`, `clear` or `reconcile` that covers them supersedes it. `behind`, and
the `flush` event's `failed`, count slots still behind. What they missed grows
with every member changed meanwhile, and the whole of it is resent each retry,
so a long outage under heavy churn costs memory and bandwidth until the node
returns. A process that exits while slots are behind loses track of them;
`verify()` and `reconcile()` are the backstop.

### One writer per set

An `IntraSlot` instance applies its writes one at a time, in the order they were
called. Two instances writing the same set get no such ordering and can undo
each other's changes, so give each set a single writer: one instance in one
process, ideally behind a coalescer. Different sets are independent and can be
spread across processes.

Because writes queue, one stuck on an unresponsive node holds up the ones behind
it. Set ioredis's `commandTimeout` so it fails instead, and prefer the coalescer
to per-event `add()` calls so the queue cannot grow without bound.

### Operations

```js
const report = await active.verify({ sample: 512 });
// { sampled, cardinalities, majority, drifted: [slot...], consistent, contents }

if (!report.consistent) {
  await active.reconcile(authoritativeMembers);  // repairs only drifted slots
}
```

A partial publish, a crashed publisher, or a replica restored from an old
snapshot all show up as drift. Replicas are compared by a digest of their
members computed on the server, so a replica with the right number of the wrong
members is caught too (`contents: true`). The digest script is read-only, so
it still runs on a master refusing writes. A replica that cannot be read, such
as a key of the wrong type, counts as drifted, and `verify` throws if it could
read none of its sample. Against a server or ACL user that refuses scripts,
only cardinalities are compared and `contents` is false. `reconcile()` republishes
to the affected slots only.

Digesting costs a SHA-1 per member, so checking every replica is not cheap:
`verify({ sample: 0 })`, which `reconcile()` runs when not given slots, took
about 1 s for a 200-member set and 12 s for a 2000-member set on the 3-node
cluster above. Each call stops once it has read `scriptBudget / 16` members,
finishing the replica it is on, so a few ms of blocking for replicas of
ordinary size, and calls go one at a time per node. A node that fails the call
outright has its sampled slots reported as drifted rather than retried.

## API

`new IntraSlot(cluster, { name })` — `name` identifies the replicated set and is
the second operand of every read below. Replicas live at `<name>:{tag}`, one per
slot.

| method | issues | notes |
|---|---|---|
| `intersect(key, ...extra)` | `SINTER key <replica>` | members of `key` also in this set |
| `count(key, {limit})` | `SINTERCARD` | size of the intersection, members stay server-side |
| `without(key)` | `SDIFF key <replica>` | members of `key` *not* in this set |
| `intersectInto(dest, key)` | `SINTERSTORE` | `dest` must share `key`'s slot |
| `sinter` / `sintercard` / `sdiff` / `sinterstore` | | aliases of the four above |
| `keyFor(key)` | — | the replica key co-slotted with `key`. Pure function, no I/O |
| `slotOf(key)` | — | slot number, per Redis's own rule |
| `assertSameSlot(keys)` | — | throws `CrossSlotError` with an explanation |
| `smembers` / `sismember` / `scard` | | read the replica local to a key |
| `publish(members, {retries})` | | replace everywhere; atomic per replica; when only a slot migration blocked it, retries for about a second |
| `add` / `remove` / `applyDelta(delta, {slots})` | | incremental fanout, optionally to chosen slots |
| `clear({retries})` | | delete every replica, retrying as `publish` does |
| `catchUp([slots])` / `behind` | | retry / count slots that missed writes |
| `verify({sample})` | | drift report |
| `reconcile(members, [slots], {retries})` | | repair drifted replicas, retrying as `publish` does |
| `coalescer(opts)` | | tick-based publisher for high churn |

`publish()` replaces each replica inside a script call, or on the per-replica
fallback stages the members under a co-slotted key and `RENAME`s it over the live
one, so a reader sees either the old set or the new one — never a half-built
one. The fanout as a whole is still eventually consistent; there is no
cross-slot transaction spanning 16384 slots, and there cannot be.

## Cost

16384 replicas is the price of the guarantee. Measured: a 200-member set of short
string IDs costs **~141 MB cluster-wide**, ~8.6 KB per replica. Budget roughly
`16384 × (payload + 50–100 B key overhead)`, so a **1–2 MB floor** before payload.

Comfortable for flags, cohorts, blocklists and presence sets. Wrong tool for
anything large — at 256 KB per copy you are looking at 4 GB.

## Correctness

The slot table is a constant, verified on load against an embedded sha256; a
mismatch means a corrupt artifact, never an update. `npm test` checks:

- CRC16 against the published XMODEM check value, and slots against published
  Redis vectors
- our `slotFor` against a direct transcription of Redis's `keyHashSlot()` from
  `cluster.c`, over 20k random keys
- every one of the 16384 tags round-trips to its own slot
- the table is *minimal* — no smaller tag exists for any slot
- `keyFor(k)` is co-slotted with `k` across hash-tag edge cases

`npm run test:cluster` brings up a real 3-node cluster and checks the claims that
only a server can settle: `SINTER` succeeds on the fanout key and is rejected on
a naive one, the same holds inside Lua, publish reaches all 16384 replicas,
`verify`/`reconcile` detect and repair drift, and the coalescer converges. It
needs `redis-server` and `redis-cli` on `PATH`. Valkey's official images provide
both names; with Valkey installed elsewhere, put links by those names first on
`PATH`.

### A note on `cluster-key-slot`

This package computes slots itself rather than using `cluster-key-slot`, because
that package is wrong for keys containing an empty hash tag.

Redis treats `{}` as *no tag* and hashes the whole key, stopping the search.
`cluster-key-slot` does not stop — it keeps scanning and latches onto the next
`}`:

```
"a{}b}c"   Redis: 2041 (hashes the whole key)
           cluster-key-slot: 3300 (hashes "b")
```

Checked against a live server's `CLUSTER KEYSLOT` over 8016 keys:
`cluster-key-slot@1.1.2` disagreed with Redis 460 times; this package, 0.

Both major Node clients have depended on it — ioredis pins `1.1.1` exactly, node-redis's
`@redis/client` uses `1.1.2` — and the two files are byte-identical, so the bug
has been live in both. In ioredis it also reaches `generateMulti()`, which gates every
command in a pipeline and misjudges in both directions: allowing commands the
server then rejects with `CROSSSLOT`, and refusing ones it would have accepted.

It couldn't be fixed upstream — [invertase/cluster-key-slot](https://github.com/invertase/cluster-key-slot)
was archived read-only on Mar 17, 2026, and ioredis's exact pin means an upstream
release wouldn't reach anyone anyway — so it was fixed in the clients instead:
both now vendor the slot function with the patch applied
(ioredis [#2172](https://github.com/redis/ioredis/pull/2172), node-redis
[#3431](https://github.com/redis/node-redis/pull/3431)). The node-redis fix
shipped in `@redis/client` 6.3.0; the 5.x maintenance line still has the bug.
The ioredis fix is in 6.1.0-beta.1, and the latest stable release, 6.0.0, still
has the bug as of this writing. A full write-up, per-client call sites,
the patch and reproducible validation are in
[`contrib/empty-hashtag-slot-bug/`](contrib/empty-hashtag-slot-bug/).

None of this affects `intraslot`: the shipped tags are plain integers with no
braces, where every implementation agrees. It only matters if *your* keys contain
`{}`, which is worth avoiding regardless — it usually means an empty template
variable leaked into a key name.

## Provenance

The canonical-key technique here was first implemented in
[twemredis-py](https://github.com/mishan/twemredis-py) (2016, now archived),
which reproduced twemproxy's md5 key distribution inside the client so
applications could address sharded Redis instances directly rather than routing
requests through a twemproxy process. There, a shard was a whole Redis instance,
so one canonical key per shard was enough to make two keys co-resident and
intersectable.

Redis Cluster checks the slot rather than the node, so that mapping no longer
suffices — see [The problem](#the-problem). `intraslot` pins one canonical key
per *slot* instead, which also makes coverage survive resharding. No code is
shared between the two; they share a technique.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
