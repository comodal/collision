## Collision [![Build Status](https://travis-ci.org/comodal/collision.svg?branch=master)](https://travis-ci.org/comodal/collision)  [![Download](https://api.bintray.com/packages/comodal/libraries/collision/images/download.svg) ](https://bintray.com/comodal/libraries/collision/_latestVersion)  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
> Java 9 Fixed-Capacity Loading Cache

```java
CollisionCache<Key, Value> cache = CollisionCache
  .<Value>withCapacity(1_048_576)
  .<Key, byte[]>setLoader(
    guid -> loadFromDisk(guid),
    (guid, loaded) -> deserialize(loaded))
  .setIsValForKey((guid, val) -> guid.equals(val.getGUID()))
  .buildSparse();
```

### Design Features
* Optional key storage.  If equality can be tested between keys and values with a supplied predicate, e.g., `boolean isValForKey(K key, V val)`, then keys will not be stored.
  * For use cases with large keys relative to the size of values, using that space to store more values may dramatically improve performance.
* Two-phase loading to separate loading of raw data and deserialization/parsing of data.  Helps to prevent unnecessary processing.
* Uses CAS atomic operations as much as possible to optimize for concurrent access.
* Optional user supplied `int hashCode(K key)` function.
* Eviction is scoped to individual hash buckets using an LFU strategy.  With this limited scope, eviction is less intelligent but has very little overhead.
* Compact [concurrent 8-bit atomic logarithmic counters](src/systems.comodal.collision/java/systems/comodal/collision/cache/AtomicLogCounters.java#L52) inspired by Salvatore Sanfilippo's [blog post on adding LFU caching to Redis](http://antirez.com/news/109), see the section on _Implementing LFU in 24 bits of space_.
* Atomic or aggressive loading of missing values.

### Implementation Notes & Cache Types
* Collision caches are backed by a large two dimensional array of generic values or [KeyVal](src/systems.comodal.collision/java/systems/comodal/collision/cache/KeyVal.java) wrappers if storing keys.  Each hash bucket is fixed in length and should be kept small.
* Hash tables are sized as a power of two.  Hash codes for keys are masked against `hashTable.length - 1` for indexing.
* A single large byte array stores a counter for each possible entry.

#### Packed Caches
The number of elements is not explicitly tracked, instead it is limited organically by the number of slots available in the backing hash table.  This might be useful for rare use cases where you can probably fit everything into cache, but it could possibly overflow and need some convenient mechanism to swap out elements.

The number of slots in the hash table is the next power of two greater than `capacity - 1`, e.g., if capacity is 1024, then the number of slots is 1024 and if capacity is 800 then the number of slots is 1024.  The extra space over capacity is given because it is unlikely every slot will be populated.

#### Sparse Caches
The number of elements is explicitly tracked and can be strictly limited to `capacity` or allowed to temporarily go over `capacity` and organically decay back down as buckets with multiple entries are accessed.

The number of slots in the hash table is the next power of two greater than `(sparseFactor * capacity) - 1`.

The hash table, a two dimensional array, is completely initialized by default.  If using a large `sparseFactor` consider setting `lazyInitBuckets` to true to save space.

If not strictly limiting, capacity can be exceeded by the following number of entries:
```
(nextPow2(sparseFactor * capacity - 1) / bucketSize) - (capacity / bucketSize)
```
For this to happen, the same `capacity / bucketSize` buckets would have to fill up before any other buckets are accessed for writes.  Followed by all remaining empty buckets being accessed before full buckets are accessed, avoiding the eviction of zero count elements from crowded buckets.

### Contribute
Pull requests for benchmarks and tests are welcome. Feel free to open an issue for feature requests, ideas, or issues.
