##Collision [![Build Status](https://travis-ci.org/jamespedwards42/collision.svg?branch=master)](https://travis-ci.org/jamespedwards42/collision) [![bintray](https://img.shields.io/bintray/v/jamespedwards42/libs/collision.svg)](https://bintray.com/jamespedwards42/libs/collision/_latestVersion) [![license](https://img.shields.io/badge/license-Apache%202-blue.svg)](https://raw.githubusercontent.com/collision/jedipus/master/LICENSE)

> Java 9 Fixed Capacity Loading Cache

```java
CollisionCache<Key, Value> cache = CollisionCache
    .<Value>withCapacity(1_048_576)
    .<Key, byte[]>setLoader(
        key -> loadFromDisk(key), 
        (key, loaded) -> deserialize(loaded))
    .setIsValForKey((key, val) -> ((Value) val).getGUID().equals(key))
    .buildSparse();
```

####Design Features
* Optionally store keys.  If equality can be tested directly between keys and values via a supplied predicate, e.g., `isValForKey(K key, V val)`, then keys will not be stored.
  * For use cases with large keys relative to the size of values, using that space to store more values may dramatically improve performance.
* Two phase loading to separate loading of raw data and deserialization/parsing of data.  Helps to prevent unnecessary processing.
* Uses CAS atomic operations as much as possible to optimize for concurrent access.
* Optional user supplied `int hashCode(K key)` function.
* Eviction is scoped to individual hash buckets using an LFU strategy.  With this limited scope, eviction is less intelligent but comes with very little overhead.
* Compact [8-bit atomic logarithmic counters](src/main/java/com/fabahaba/collision/cache/LogCounterCache.java#L29) inspired by Salvatore Sanfilippo's [blog post on adding LFU caching to Redis](http://antirez.com/news/109), see the section on _Implementing LFU in 24 bits of space_.

####Implementation Notes & Cache Types
* Collision caches are backed by a large hash table of generic values or java.until.Map.Entry's if storing keys.  Each hash bucket is fixed in length and should be kept small.
* Backing hash tables are sized as a power of two.  Hash codes for keys are masked against `hashTable.length - 1` for indexing.
* A single large byte[] stores an 8-bit counter for each possible entry.

######Packed Caches
TODO
######Sparse Caches
TODO
