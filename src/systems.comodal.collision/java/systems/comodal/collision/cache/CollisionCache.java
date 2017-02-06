package systems.comodal.collision.cache;

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
   * null space or swapped with the least frequently used within its hash bucket.  Calls to the
   * loader are synchronized behind the hash bucket for this key.  If no loader is registered with
   * this cache then null will be returned if the item does not exist in the cache.  If the loader
   * returns null, then null will be returned.
   *
   * @param key used for table hash and stored key/value equality.
   * @return a value for the corresponding key.
   */
  V get(final K key);

  /**
   * If a value already exists for the key it is returned, otherwise it is loaded and filled into a
   * null space or swapped with the least frequently used within its hash bucket.  If the loader
   * returns null, then null will be returned.  Calls to the loader are synchronized behind the hash
   * bucket for this key.
   *
   * @param key        used for table hash and stored key/value equality.
   * @param loadAndMap creates values in the event of a cache miss.
   * @return a value for the corresponding key.
   */
  V get(final K key, final Function<K, V> loadAndMap);

  /**
   * If a value already exists for the key it is returned, otherwise it is loaded and filled into a
   * null space or swapped with the least frequently used within its hash bucket.  Calls to the
   * loader are NOT synchronized.  If no loader is registered with this cache then null will be
   * returned if the item does not exist in the cache.  If the loader returns null, then null will
   * be returned.
   *
   * @param key used for table hash and stored key/value equality.
   * @return a value for the corresponding key.
   */
  V getAggressive(final K key);

  /**
   * If a value already exists for the key it is returned, otherwise it is loaded and filled into a
   * null space or swapped with the least frequently used within its hash bucket.  Calls to the
   * loader are NOT synchronized.  If the loader returns null, then null will
   * be returned.  The mapper must not return null; cache performance could severely degrade.
   *
   * @param key    used for table hash and stored key/value equality.
   * @param loader creates values in the event of a cache miss.
   * @param mapper maps loaded values to value types.
   * @return a value for the corresponding key.
   */
  <I> V getAggressive(final K key, final Function<K, I> loader, final BiFunction<K, I, V> mapper);

  /**
   * The given value will be placed into the cache unless strictly over capacity and there are no
   * items to swap with within its hash bucket.  In race conditions, occurring after entry to this
   * call, another value may win for this key and will be returned.
   *
   * @param key used for table hash and stored key/value equality.
   * @param val The value to put.
   * @return the value in the cache after this call.
   */
  V putReplace(final K key, final V val);

  /**
   * The given value will replace any existing value for this key.  In race conditions, occurring
   * after entry to this call, another value may win for this key and will be returned.
   *
   * @param key used for table hash and stored key/value equality.
   * @param val The value to put if an entry for the key exists.
   * @return the value in the cache after this call.
   */
  V replace(final K key, final V val);

  /**
   * The given value will be placed into the cache unless a value for this key already exists or the
   * cache is strictly over capacity and there are no items to swap with within its hash bucket.  In
   * race conditions, occurring after entry to this call, another value may win for this key and
   * will be returned.
   *
   * @param key used for table hash and stored key/value equality.
   * @param val The value to put if no current entry exists for this key.
   * @return the value in the cache after this call.
   */
  V putIfAbsent(final K key, final V val);

  /**
   * The given value will be placed into the cache unless a value for this key already exists or
   * there are no under capacity null spaces available within its hash bucket. This method avoids
   * any synchronization.  In race conditions, occurring after entry to this call, another value may
   * win for this key and will be returned.
   *
   * @param key used for table hash and value equality.
   * @param val The value to put if null space and an entry does not currently exist this key.
   * @return the value in the cache after this call.
   */
  V putIfSpaceAbsent(final K key, final V val);

  /**
   * The given value will be placed into the cache unless there are no under capacity null spaces
   * available within its hash bucket. This method avoids any synchronization.  In race conditions,
   * occurring after entry to this call, another value may win for this key and will be returned.
   *
   * @param key used for table hash and stored key/value equality.
   * @param val The value to put if an entry exists or there is null space.  In race conditions
   *            occurring after entry to this call, another value may win.
   * @return the value in the cache after this call.
   */
  V putIfSpaceReplace(final K key, final V val);

  /**
   * @param key used for table hash and stored key/value equality.
   * @return the pre-existing value for this key.
   */
  V getIfPresent(final K key);

  /**
   * Uses {@link java.lang.invoke.VarHandle#getAcquire VarHandle#getAcquire} to access existing
   * elements.
   *
   * @param key used for table hash and stored key/value equality.
   * @return the pre-existing value for this key.
   */
  V getIfPresentAcquire(final K key);

  /**
   * Removes any entry for the corresponding key.
   *
   * @param key used for table hash and stored key/value equality.
   * @return true if an entry was found.
   */
  boolean remove(final K key);

  /**
   * Sets all hash table bucket slots to null.
   */
  void clear();

  /**
   * Sets all hash table buckets to null;
   */
  void nullBuckets();
}
