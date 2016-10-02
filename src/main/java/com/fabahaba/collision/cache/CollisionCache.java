package com.fabahaba.collision.cache;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @param <K> the type of keys used to map to values
 * @param <V> the type of mapped values
 * @author James P. Edwards
 */
public interface CollisionCache<K, V> {

  static <V> CollisionBuilder<V> withCapacity(final int capacity) {
    return new CollisionBuilder<>(capacity);
  }

  static <V> CollisionBuilder<V> withCapacity(final int capacity, final Class<V> valueType) {
    return new CollisionBuilder<V>(capacity).setValueType(valueType);
  }

  /**
   * If a value already exists for the key it is returned, otherwise it is loaded and filled into a
   * null space or swapped with the least frequently used for its hash bucket.
   *
   * @param key used for table hash and value equality.
   * @return a value for the corresponding key.
   */
  V get(final K key);

  /**
   * If a value already exists for the key it is returned, otherwise it is loaded and filled into a
   * null space or swapped with the least frequently used for its hash bucket.
   *
   * @param key    used for table hash and value equality.
   * @param loader creates values in the event of a cache miss.
   * @param mapper Maps loaded values to value types.
   * @return a value for the corresponding key.
   */
  <I> V get(final K key, final Function<K, I> loader, final BiFunction<K, I, V> mapper);

  /**
   * @param key used for table hash and value equality.
   * @param val The value to put.  In race conditions occurring after entry to this call, another
   *            value may win for this key.
   * @return the value in the cache after this call.
   */
  V putReplace(final K key, final V val);

  /**
   * @param key used for table hash and value equality.
   * @param val The value to put if an entry for the key exists. In race conditions occurring after
   *            entry to this call, another value may win for this key.
   * @return the value in the cache after this call.
   */
  V replace(final K key, final V val);

  /**
   * @param key used for table hash and value equality.
   * @param val The value to put if no current entry for the key.
   * @return the value in the cache after this call.
   */
  V putIfAbsent(final K key, final V val);

  /**
   * @param key used for table hash and value equality.
   * @param val The value to put if null space an entry does not currently exist this key.
   * @return the value in the cache after this call.
   */
  V putIfSpaceAbsent(final K key, final V val);

  /**
   * @param key used for table hash and value equality.
   * @param val The value to put if an entry exists or there is null space.  In race conditions
   *            occurring after entry to this call, another value may win.
   * @return the value in the cache after this call.
   */
  V putIfSpaceReplace(final K key, final V val);

  /**
   * @param key used for table hash and value equality.
   * @return the value in pre-existing in the cache for this key.
   */
  V getIfPresent(final K key);

  /**
   * Uses volatile memory access semantics to check existing entries.
   *
   * @param key used for table hash and value equality.
   * @return the value in pre-existing in the cache for this key.
   */
  V getIfPresentVolatile(final K key);

  void clear();

  void nullBuckets();
}
