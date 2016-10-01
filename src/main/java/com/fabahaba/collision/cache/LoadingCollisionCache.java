package com.fabahaba.collision.cache;

import java.util.function.Function;

/**
 * @param <K> the type of keys used to map to values
 * @param <L> the type of loaded values before being mapped to type V
 * @param <V> the type of mapped values
 * @author James P. Edwards
 */
public interface LoadingCollisionCache<K, L, V> extends CollisionCache<K, V> {

  /**
   * If a value already exists for the key it is returned, otherwise it is loaded and filled into a
   * null space or swapped with the least frequently used for its hash bucket.
   *
   * @param key    used for table hash and value equality.
   * @param loader creates values in the event of a cache miss.
   * @return a value for the corresponding key.
   */
  V get(final K key, final Function<K, L> loader);
}
