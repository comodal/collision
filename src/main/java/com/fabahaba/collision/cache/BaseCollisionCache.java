package com.fabahaba.collision.cache;

import java.lang.reflect.Array;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.ToIntFunction;

/**
 * @param <K> the type of keys used to map to values
 * @param <L> the type of loaded values before being mapped to type V
 * @param <V> the type of mapped values
 * @author James P. Edwards
 */
abstract class BaseCollisionCache<K, L, V> extends LogCounterCache
    implements LoadingCollisionCache<K, L, V> {

  final Class<V> valueType;
  final int maxCollisionsShift;
  final V[][] hashTable;
  final int mask;
  final ToIntFunction<K> hashCoder;
  final BiPredicate<K, Object> isValForKey;
  private final Function<K, L> loader;
  private final BiFunction<K, L, V> mapper;

  BaseCollisionCache(final Class<V> valueType,
      final int maxCollisionsShift,
      final byte[] counters,
      final int initCount,
      final int pow2LogFactor,
      final V[][] hashTable,
      final ToIntFunction<K> hashCoder,
      final BiPredicate<K, Object> isValForKey,
      final Function<K, L> loader,
      final BiFunction<K, L, V> mapper) {
    super(counters, initCount, pow2LogFactor);
    this.valueType = valueType;
    this.maxCollisionsShift = maxCollisionsShift;
    this.hashTable = hashTable;
    this.mask = hashTable.length - 1;
    this.hashCoder = hashCoder;
    this.isValForKey = isValForKey;
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
  V[] getCreateCollisions(final int hash) {
    V[] collisions = hashTable[hash];
    if (collisions == null) {
      collisions = (V[]) Array.newInstance(valueType, 1 << maxCollisionsShift);
      final Object witness = OA.compareAndExchangeRelease(hashTable, hash, null, collisions);
      return witness == null ? collisions : (V[]) witness;
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
    final V[] collisions = getCreateCollisions(hash);
    int index = 0;
    do {
      final V val = collisions[index];
      if (val == null) {
        return null;
      }
      if (isValForKey.test(key, val)) {
        atomicIncrement((hash << maxCollisionsShift) + index);
        return val;
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
    final V[] collisions = getCreateCollisions(hash);
    int index = 0;
    do {
      final Object val = OA.getVolatile(collisions, index);
      if (val == null) {
        return null;
      }
      if (isValForKey.test(key, val)) {
        atomicIncrement((hash << maxCollisionsShift) + index);
        return (V) val;
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
    if (val == null) {
      throw new NullPointerException("Cannot cache a null value.");
    }
    final int hash = hashCoder.applyAsInt(key) & mask;
    final V[] collisions = getCreateCollisions(hash);
    int index = 0;
    do {
      final Object collision = OA.getVolatile(collisions, index);
      if (collision == null) {
        return null;
      }
      if (collision == val) {
        return val;
      }
      if (isValForKey.test(key, collision)) {
        final Object witness = OA.compareAndExchangeRelease(collisions, index, collision, val);
        if (witness == collision) {
          return val;
        }
        // If another thread raced to PUT, let it win.
        if (isValForKey.test(key, witness)) {
          return (V) witness;
        }
      }
    } while (++index < collisions.length);
    return null;
  }

  @Override
  public String toString() {
    return "valueType=" + valueType
        + ", maxCollisions=" + (1 << maxCollisionsShift)
        + ", numCounters=" + counters.length
        + ", initCount=" + initCount
        + ", hashTableLength=" + hashTable.length;
  }
}
