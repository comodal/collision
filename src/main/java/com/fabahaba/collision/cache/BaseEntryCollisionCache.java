package com.fabahaba.collision.cache;

import java.lang.reflect.Array;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToIntFunction;

abstract class BaseEntryCollisionCache<K, L, V> extends LogCounterCache
    implements LoadingCollisionCache<K, L, V> {

  final int maxCollisionsShift;
  final Map.Entry<K, V>[][] hashTable;
  final int mask;
  final ToIntFunction<K> hashCoder;
  private final Function<K, L> loader;
  private final BiFunction<K, L, V> mapper;

  BaseEntryCollisionCache(
      final int maxCollisionsShift,
      final byte[] counters,
      final int initCount,
      final int pow2LogFactor,
      final Map.Entry<K, V>[][] hashTable,
      final ToIntFunction<K> hashCoder,
      final Function<K, L> loader,
      final BiFunction<K, L, V> mapper) {
    super(counters, initCount, pow2LogFactor);
    this.maxCollisionsShift = maxCollisionsShift;
    this.hashTable = hashTable;
    this.mask = hashTable.length - 1;
    this.hashCoder = hashCoder;
    this.loader = loader;
    this.mapper = mapper;
  }

  /**
   * CAS initialize an array for holding values at a given hash location.
   *
   * @param hash The hash table index.
   * @return The hash bucket array, referred to as collisions.
   */
  @SuppressWarnings("unchecked")
  Map.Entry<K, V>[] getCreateCollisions(final int hash) {
    Map.Entry<K, V>[] collisions = hashTable[hash];
    if (collisions == null) {
      collisions = (Map.Entry<K, V>[]) Array.newInstance(Map.Entry.class, 1 << maxCollisionsShift);
      final Object witness = OA.compareAndExchangeRelease(hashTable, hash, null, collisions);
      return witness == null ? collisions : (Map.Entry<K, V>[]) witness;
    }
    return collisions;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V get(final K key) {
    return get(key, loader, mapper);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V get(final K key, final Function<K, L> loader) {
    return get(key, loader, mapper);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public V getIfPresent(final K key) {
    final int hash = hashCoder.applyAsInt(key) & mask;
    final Map.Entry<K, V>[] collisions = getCreateCollisions(hash);
    int index = 0;
    do {
      final Map.Entry<K, V> entry = collisions[index];
      if (entry == null) {
        return null;
      }
      if (key.equals(entry.getKey())) {
        atomicIncrement((hash << maxCollisionsShift) + index);
        return entry.getValue();
      }
    } while (++index < collisions.length);
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public V getIfPresentVolatile(final K key) {
    final int hash = hashCoder.applyAsInt(key) & mask;
    final Map.Entry<K, V>[] collisions = getCreateCollisions(hash);
    int index = 0;
    do {
      final Map.Entry<K, V> entry = (Map.Entry<K, V>) OA.getVolatile(collisions, index);
      if (entry == null) {
        return null;
      }
      if (key.equals(entry.getKey())) {
        atomicIncrement((hash << maxCollisionsShift) + index);
        return entry.getValue();
      }
    } while (++index < collisions.length);
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public V replace(final K key, final V val) {
    final int hash = hashCoder.applyAsInt(key) & mask;
    final Map.Entry<K, V>[] collisions = getCreateCollisions(hash);
    int index = 0;
    do {
      final Map.Entry<K, V> entry = (Map.Entry<K, V>) OA.getVolatile(collisions, index);
      if (entry == null) {
        return null;
      }
      if (entry.getValue() == val) {
        return val;
      }
      if (key.equals(entry.getKey())) {
        final Map.Entry<K, V> witness = (Map.Entry<K, V>) OA
            .compareAndExchangeRelease(collisions, index, entry, Map.entry(key, val));
        if (witness == entry) {
          return val;
        }
        // If another thread raced to PUT, let it win.
        if (key.equals(witness.getKey())) {
          return witness.getValue();
        }
      }
    } while (++index < collisions.length);
    return null;
  }

  @Override
  public String toString() {
    return "maxCollisions=" + (1 << maxCollisionsShift)
        + ", numCounters=" + counters.length
        + ", initCount=" + initCount
        + ", hashTableLength=" + hashTable.length;
  }
}
