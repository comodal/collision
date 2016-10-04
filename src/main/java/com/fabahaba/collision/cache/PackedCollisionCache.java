package com.fabahaba.collision.cache;

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
final class PackedCollisionCache<K, L, V> extends BaseCollisionCache<K, L, V> {

  PackedCollisionCache(final Class<V> valueType, final int maxCollisionsShift,
      final byte[] counters, final int initCount, final int pow2LogFactor, final V[][] hashTable,
      final ToIntFunction<K> hashCoder, final BiPredicate<K, Object> isValForKey,
      final Function<K, L> loader, final BiFunction<K, L, V> mapper) {
    super(valueType, maxCollisionsShift, counters, initCount, pow2LogFactor, hashTable, hashCoder,
        isValForKey, loader, mapper);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public <I> V get(final K key, final Function<K, I> loader, final BiFunction<K, I, V> mapper) {
    final int hash = hashCoder.applyAsInt(key) & mask;
    final V[] collisions = getCreateCollisions(hash);
    final int counterOffset = hash << maxCollisionsShift;
    int index = 0;
    do {
      final V collision = collisions[index];
      if (collision == null) {
        final I loaded = loader.apply(key);
        if (loaded == null) {
          return null;
        }
        final V val = mapper.apply(key, loaded);
        do {
          final Object witness = OA.compareAndExchangeRelease(collisions, index, null, val);
          if (witness == null) {
            BA.setRelease(counters, counterOffset + index, initCount);
            return val;
          }
          if (isValForKey.test(key, witness)) {
            atomicIncrement(counterOffset + index);
            return (V) witness;
          }
        } while (++index < collisions.length);
        return checkDecayAndSwapLFU(counterOffset, collisions, key, val);
      }
      if (isValForKey.test(key, collision)) {
        atomicIncrement(counterOffset + index);
        return collision;
      }
    } while (++index < collisions.length);
    final I loaded = loader.apply(key);
    return loaded == null ? null
        : checkDecayAndSwapLFU(counterOffset, collisions, key, loaded, mapper);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  protected V checkDecayAndSwapLFU(final int counterOffset, final V[] collisions, final K key,
      final Function<K, V> loadAndMap) {
    int index = 0;
    V val;
    Object collision;
    synchronized (collisions) {
      NO_SWAP:
      for (;;) { // Double-check locked volatile before swapping LFU to help prevent duplicates.
        collision = OA.getVolatile(collisions, index);
        if (collision == null) {
          val = loadAndMap.apply(key);
          if (val == null) {
            return null;
          }
          do {
            collision = OA.compareAndExchangeRelease(collisions, index, null, val);
            if (collision == null) {
              break NO_SWAP;
            }
            if (isValForKey.test(key, collision)) {
              val = (V) collision;
              break NO_SWAP;
            }
          } while (++index < collisions.length);
          decayAndSwapLFU(counterOffset, collisions, val);
          return val;
        }
        if (isValForKey.test(key, collision)) {
          val = (V) collision;
          break;
        }
        if (++index == collisions.length) {
          val = loadAndMap.apply(key);
          if (val == null) {
            return null;
          }
          decayAndSwapLFU(counterOffset, collisions, val);
          return val;
        }
      }
    }
    if (collision == null) {
      BA.setRelease(counters, counterOffset + index, initCount);
      return val;
    }
    atomicIncrement(counterOffset + index);
    return val;
  }

  @SuppressWarnings("unchecked")
  private <I> V checkDecayAndSwapLFU(final int counterOffset, final V[] collisions,
      final K key, final I loaded, final BiFunction<K, I, V> mapper) {
    int index = 0;
    synchronized (collisions) {
      do { // Double-checked locked volatile before swapping LFU to prevent duplicates.
        final Object collision = OA.getVolatile(collisions, index);
        if (isValForKey.test(key, collision)) {
          atomicIncrement(counterOffset + index);
          return (V) collision;
        }
      } while (++index < collisions.length);
      final V val = mapper.apply(key, loaded);
      if (val == null) {
        return null;
      }
      decayAndSwapLFU(counterOffset, collisions, val);
      return val;
    }
  }

  @SuppressWarnings("unchecked")
  private V checkDecayAndSwapLFU(final int counterOffset, final V[] collisions,
      final K key, final V val) {
    int index = 0;
    synchronized (collisions) {
      do { // Double-checked locked volatile before swapping LFU to prevent duplicates.
        final Object collision = OA.getVolatile(collisions, index);
        if (isValForKey.test(key, collision)) {
          atomicIncrement(counterOffset + index);
          return (V) collision;
        }
      } while (++index < collisions.length);
      decayAndSwapLFU(counterOffset, collisions, val);
    }
    return val;
  }

  /**
   * Divides all counters for values within a hash bucket (collisions), swaps the value for the
   * least frequently used, and sets its counter to an initial value.
   *
   * @param counterOffset beginning counter array index corresponding to collision values.
   * @param collisions    values sitting in a hash bucket.
   * @param val           The value to put in place of the least frequently used value.
   */
  private void decayAndSwapLFU(final int counterOffset, final V[] collisions,
      final Object val) {
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = 0xff;
    final int max = counterOffset + collisions.length;
    do {
      int count = ((int) BA.getVolatile(counters, counterIndex)) & 0xff;
      if (count == 0) {
        OA.setRelease(collisions, counterIndex - counterOffset, val);
        BA.setRelease(counters, counterIndex, initCount);
        while (++counterIndex < max) {
          count = ((int) BA.getVolatile(counters, counterIndex)) & 0xff;
          if (count > 0) {
            BA.setRelease(counters, counterIndex, (byte) (count >>> 1));
          }
        }
        return;
      }
      // Counter misses may occur between these two calls.
      BA.setRelease(counters, counterIndex, (byte) (count >>> 1));
      if (count < minCount) {
        minCount = count;
        minCounterIndex = counterIndex;
      }
    } while (++counterIndex < max);
    OA.setRelease(collisions, minCounterIndex - counterOffset, val);
    BA.setRelease(counters, minCounterIndex, initCount);
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public V putReplace(final K key, final V val) {
    if (val == null) {
      throw new NullPointerException("Cannot cache a null value.");
    }
    final int hash = hashCoder.applyAsInt(key) & mask;
    final V[] collisions = getCreateCollisions(hash);
    int index = 0;
    do {
      Object collision = OA.getVolatile(collisions, index);
      if (collision == null) {
        collision = OA.compareAndExchangeRelease(collisions, index, null, val);
        if (collision == null) {
          BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
          return val;
        }
        // If another thread raced to PUT, let it win.
        if (isValForKey.test(key, collision)) {
          return (V) collision;
        }
        continue;
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
    index = 0;
    synchronized (collisions) {
      do { // Double-checked locked volatile before swapping LFU to prevent duplicates.
        final Object collision = OA.getVolatile(collisions, index);
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
      decayAndSwapLFU(hash << maxCollisionsShift, collisions, val);
    }
    return val;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public V putIfAbsent(final K key, final V val) {
    if (val == null) {
      throw new NullPointerException("Cannot cache a null value.");
    }
    final int hash = hashCoder.applyAsInt(key) & mask;
    final V[] collisions = getCreateCollisions(hash);
    int index = 0;
    do {
      final V collision = collisions[index];
      if (collision == null) {
        final Object witness = OA.compareAndExchangeRelease(collisions, index, null, val);
        if (witness == null) {
          BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
          return val;
        }
        if (isValForKey.test(key, witness)) {
          return (V) witness;
        }
        continue;
      }
      if (isValForKey.test(key, collision)) {
        return collision;
      }
    } while (++index < collisions.length);
    index = 0;
    synchronized (collisions) {
      do { // Double-checked locked volatile before swapping LFU to prevent duplicates.
        final Object collision = OA.getVolatile(collisions, index);
        if (isValForKey.test(key, collision)) {
          return (V) collision;
        }
      } while (++index < collisions.length);
      decayAndSwapLFU(hash << maxCollisionsShift, collisions, val);
    }
    return val;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public V putIfSpaceAbsent(final K key, final V val) {
    if (val == null) {
      throw new NullPointerException("Cannot cache a null value.");
    }
    final int hash = hashCoder.applyAsInt(key) & mask;
    final V[] collisions = getCreateCollisions(hash);
    int index = 0;
    do {
      final V collision = collisions[index];
      if (collision == null) {
        final Object witness = OA.compareAndExchangeRelease(collisions, index, null, val);
        if (witness == null) {
          BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
          return val;
        }
        if (isValForKey.test(key, witness)) {
          return (V) witness;
        }
        continue;
      }
      if (isValForKey.test(key, collision)) {
        return collision;
      }
    } while (++index < collisions.length);
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public V putIfSpaceReplace(final K key, final V val) {
    if (val == null) {
      throw new NullPointerException("Cannot cache a null value.");
    }
    final int hash = hashCoder.applyAsInt(key) & mask;
    final V[] collisions = getCreateCollisions(hash);
    int index = 0;
    do {
      Object collision = OA.getVolatile(collisions, index);
      if (collision == null) {
        collision = OA.compareAndExchangeRelease(collisions, index, null, val);
        if (collision == null) {
          BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
          return val;
        }
        // If another thread raced to PUT, let it win.
        if (isValForKey.test(key, collision)) {
          return (V) collision;
        }
        continue;
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
    return "PackedCollisionCache{" + super.toString() + '}';
  }
}
