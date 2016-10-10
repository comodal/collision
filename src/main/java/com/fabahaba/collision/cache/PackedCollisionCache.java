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
      final ToIntFunction<K> hashCoder, final BiPredicate<K, V> isValForKey,
      final Function<K, L> loader, final BiFunction<K, L, V> mapper) {
    super(valueType, maxCollisionsShift, counters, initCount, pow2LogFactor, hashTable, hashCoder,
        isValForKey, loader, mapper);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public <I> V getAggressive(final K key, final Function<K, I> loader,
      final BiFunction<K, I, V> mapper) {
    final int hash = hashCoder.applyAsInt(key) & mask;
    final V[] collisions = getCreateCollisions(hash);
    final int counterOffset = hash << maxCollisionsShift;
    int index = 0;
    do {
      V collision = collisions[index];
      if (collision == null) {
        final I loaded = loader.apply(key);
        if (loaded == null) {
          return null;
        }
        final V val = mapper.apply(key, loaded);
        do {
          collision = (V) OA.compareAndExchangeRelease(collisions, index, null, val);
          if (collision == null) {
            BA.setRelease(counters, counterOffset + index, initCount);
            return val;
          }
          if (isValForKey.test(key, collision)) {
            atomicIncrement(counterOffset + index);
            return collision;
          }
        } while (++index < collisions.length);
        return checkDecayAndProbSwap(counterOffset, collisions, key, val);
      }
      if (isValForKey.test(key, collision)) {
        atomicIncrement(counterOffset + index);
        return collision;
      }
    } while (++index < collisions.length);
    final I loaded = loader.apply(key);
    return loaded == null ? null
        : checkDecayAndProbSwap(counterOffset, collisions, key, loaded, mapper);
  }

  @SuppressWarnings("unchecked")
  private V checkDecayAndProbSwap(final int counterOffset, final V[] collisions, final K key,
      final V val) {
    int index = 0;
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = 0xff;
    synchronized (collisions) {
      for (;;) {
        V collision = (V) OA.getAcquire(collisions, index);
        if (collision == null) { // Assume over capacity.
          if (index == 0) { // Strict capacity checked in parent call.
            collision = (V) OA.compareAndExchangeRelease(collisions, index, null, val);
            if (collision == null) {
              BA.setRelease(counters, counterIndex, initCount);
              return val;
            }
            if (isValForKey.test(key, collision)) {
              atomicIncrement(counterIndex);
              return collision;
            }
            return val; // Don't cache, lost tie breaker.
          }
          OA.setRelease(collisions, minCounterIndex - counterOffset, val);
          BA.setRelease(counters, minCounterIndex, initCount);
          decay(counterOffset, counterIndex, minCounterIndex);
          return val;
        }

        if (isValForKey.test(key, collision)) {
          atomicIncrement(counterIndex);
          return collision;
        }

        int count = ((int) BA.getAcquire(counters, counterIndex)) & 0xff;
        if (count < minCount) {
          minCount = count;
          minCounterIndex = counterIndex;
        }
        ++counterIndex;
        if (++index == collisions.length) {
          OA.setRelease(collisions, minCounterIndex - counterOffset, val);
          BA.setRelease(counters, minCounterIndex, initCount);
          decay(counterOffset, counterIndex, minCounterIndex);
          return val;
        }
      }
    }
  }

  /**
   * This method assumes a full bucket or (XOR) over capacity and checks for both (AND).
   */
  @SuppressWarnings("unchecked")
  private <I> V checkDecayAndProbSwap(final int counterOffset, final V[] collisions, final K key,
      final I loaded, final BiFunction<K, I, V> mapper) {
    int index = 0;
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = 0xff;
    synchronized (collisions) {
      for (;;) {
        V collision = (V) OA.getAcquire(collisions, index);
        if (collision == null) { // Assume over capacity.
          final V val = mapper.apply(key, loaded);
          if (index == 0) { // Strict capacity checked in parent call.
            collision = (V) OA.compareAndExchangeRelease(collisions, index, null, val);
            if (collision == null) {
              BA.setRelease(counters, counterIndex, initCount);
              return val;
            }
            if (isValForKey.test(key, collision)) {
              atomicIncrement(counterIndex);
              return collision;
            }
            return val; // Don't cache, lost tie breaker.
          }
          OA.setRelease(collisions, minCounterIndex - counterOffset, val);
          BA.setRelease(counters, minCounterIndex, initCount);
          decay(counterOffset, counterIndex, minCounterIndex);
          return val;
        }

        if (isValForKey.test(key, collision)) {
          atomicIncrement(counterIndex);
          return collision;
        }

        int count = ((int) BA.getAcquire(counters, counterIndex)) & 0xff;
        if (count < minCount) {
          minCount = count;
          minCounterIndex = counterIndex;
        }
        ++counterIndex;
        if (++index == collisions.length) {
          final V val = mapper.apply(key, loaded);
          OA.setRelease(collisions, minCounterIndex - counterOffset, val);
          BA.setRelease(counters, minCounterIndex, initCount);
          decay(counterOffset, counterIndex, minCounterIndex);
          return val;
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  V checkDecayAndSwap(final int counterOffset, final V[] collisions, final K key,
      final Function<K, V> loadAndMap) {
    int index = 0;
    synchronized (collisions) {
      for (;;) { // Double-check locked volatile before swapping LFU to help prevent duplicates.
        V collision = (V) OA.getAcquire(collisions, index);
        if (collision == null) {
          final V val = loadAndMap.apply(key);
          if (val == null) {
            return null;
          }
          do {
            collision = (V) OA.compareAndExchangeRelease(collisions, index, null, val);
            if (collision == null) {
              BA.setRelease(counters, counterOffset + index, initCount);
              return val;
            }
            if (isValForKey.test(key, collision)) {
              atomicIncrement(counterOffset + index);
              return collision;
            }
          } while (++index == collisions.length);
          decayAndSwap(counterOffset, counterOffset + collisions.length, collisions, val);
          return val;
        }
        if (isValForKey.test(key, collision)) {
          atomicIncrement(counterOffset + index);
          return collision;
        }
        if (++index == collisions.length) {
          final V val = loadAndMap.apply(key);
          if (val == null) {
            return null;
          }
          decayAndSwap(counterOffset, counterOffset + collisions.length, collisions, val);
          return val;
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  V checkDecayAndProbSwap(final int counterOffset, final V[] collisions, final K key,
      final Function<K, V> loadAndMap) {
    int index = 0;
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = 0xff;
    synchronized (collisions) {
      for (;;) {
        V collision = (V) OA.getAcquire(collisions, index);
        if (collision == null) {
          final V val = loadAndMap.apply(key);
          if (val == null) {
            return null;
          }
          do {
            collision = (V) OA.compareAndExchangeRelease(collisions, index, null, val);
            if (collision == null) {
              BA.setRelease(counters, counterOffset + index, initCount);
              return val;
            }
            if (isValForKey.test(key, collision)) {
              atomicIncrement(counterOffset + index);
              return collision;
            }
          } while (++index == collisions.length);
          OA.setRelease(collisions, minCounterIndex - counterOffset, val);
          BA.setRelease(counters, minCounterIndex, initCount);
          decay(counterOffset, counterOffset + collisions.length, minCounterIndex);
          return val;
        }

        if (isValForKey.test(key, collision)) {
          atomicIncrement(counterIndex);
          return collision;
        }

        int count = ((int) BA.getAcquire(counters, counterIndex)) & 0xff;
        if (count < minCount) {
          minCount = count;
          minCounterIndex = counterIndex;
        }
        ++counterIndex;
        if (++index == collisions.length) {
          final V val = loadAndMap.apply(key);
          if (val == null) {
            return null;
          }
          OA.setRelease(collisions, minCounterIndex - counterOffset, val);
          BA.setRelease(counters, minCounterIndex, initCount);
          decay(counterOffset, counterIndex, minCounterIndex);
          return val;
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public V putReplace(final K key, final V val) {
    if (val == null) {
      throw new NullPointerException("Cannot cache a null val.");
    }
    final int hash = hashCoder.applyAsInt(key) & mask;
    final V[] collisions = getCreateCollisions(hash);
    int index = 0;
    do {
      V collision = (V) OA.getAcquire(collisions, index);
      if (collision == null) {
        do {
          collision = (V) OA.compareAndExchangeRelease(collisions, index, null, val);
          if (collision == null) {
            BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
            return val;
          }
          if (isValForKey.test(key, collision)) {
            return collision; // If another thread raced to PUT, let it win.
          }
        } while (++index < collisions.length);
        break;
      }
      if (collision == val) {
        return val;
      }
      if (isValForKey.test(key, collision)) {
        final V witness = (V) OA.compareAndExchangeRelease(collisions, index, collision, val);
        if (witness == collision) {
          return val;
        }
        if (isValForKey.test(key, witness)) {
          return witness; // If another thread raced to PUT, let it win.
        }
      }
    } while (++index < collisions.length);

    index = 0;
    final int counterOffset = hash << maxCollisionsShift;
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = 0xff;
    synchronized (collisions) {
      for (;;) {
        final V collision = (V) OA.getAcquire(collisions, index);
        if (collision == val) {
          return val;
        }
        if (isValForKey.test(key, collision)) {
          final V witness = (V) OA.compareAndExchangeRelease(collisions, index, collision, val);
          if (witness == collision) {
            return val;
          }
          if (isValForKey.test(key, witness)) {
            return witness; // If another thread raced to PUT, let it win.
          }
        }

        int count = ((int) BA.getAcquire(counters, counterIndex)) & 0xff;
        if (count < minCount) {
          minCount = count;
          minCounterIndex = counterIndex;
        }
        ++counterIndex;
        if (++index == collisions.length) {
          OA.setRelease(collisions, minCounterIndex - counterOffset, val);
          BA.setRelease(counters, minCounterIndex, initCount);
          decay(counterOffset, counterIndex, minCounterIndex);
          return val;
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public V putIfAbsent(final K key, final V val) {
    if (val == null) {
      throw new NullPointerException("Cannot cache a null val.");
    }
    final int hash = hashCoder.applyAsInt(key) & mask;
    final V[] collisions = getCreateCollisions(hash);
    int index = 0;
    do {
      V collision = collisions[index];
      if (collision == null) {
        do {
          collision = (V) OA.compareAndExchangeRelease(collisions, index, null, val);
          if (collision == null) {
            BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
            return val;
          }
          if (isValForKey.test(key, collision)) {
            return collision;
          }
        } while (++index < collisions.length);
        break;
      }
      if (isValForKey.test(key, collision)) {
        return collision;
      }
    } while (++index < collisions.length);

    index = 0;
    final int counterOffset = hash << maxCollisionsShift;
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = 0xff;
    synchronized (collisions) {
      for (;;) {
        final V collision = (V) OA.getAcquire(collisions, index);
        if (isValForKey.test(key, collision)) {
          return collision;
        }
        int count = ((int) BA.getAcquire(counters, counterIndex)) & 0xff;
        if (count < minCount) {
          minCount = count;
          minCounterIndex = counterIndex;
        }
        ++counterIndex;
        if (++index == collisions.length) {
          OA.setRelease(collisions, minCounterIndex - counterOffset, val);
          BA.setRelease(counters, minCounterIndex, initCount);
          decay(counterOffset, counterIndex, minCounterIndex);
          return val;
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public V putIfSpaceAbsent(final K key, final V val) {
    if (val == null) {
      throw new NullPointerException("Cannot cache a null val.");
    }
    final int hash = hashCoder.applyAsInt(key) & mask;
    final V[] collisions = getCreateCollisions(hash);
    int index = 0;
    do {
      V collision = collisions[index];
      if (collision == null) {
        do {
          collision = (V) OA.compareAndExchangeRelease(collisions, index, null, val);
          if (collision == null) {
            BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
            return val;
          }
          if (isValForKey.test(key, collision)) {
            return collision;
          }
        } while (++index < collisions.length);
        return null;
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
  @Override
  @SuppressWarnings("unchecked")
  public V putIfSpaceReplace(final K key, final V val) {
    if (val == null) {
      throw new NullPointerException("Cannot cache a null val.");
    }
    final int hash = hashCoder.applyAsInt(key) & mask;
    final V[] collisions = getCreateCollisions(hash);
    int index = 0;
    do {
      V collision = (V) OA.getAcquire(collisions, index);
      if (collision == null) {
        do {
          collision = (V) OA.compareAndExchangeRelease(collisions, index, null, val);
          if (collision == null) {
            BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
            return val;
          }
          if (isValForKey.test(key, collision)) {
            return collision; // If another thread raced to PUT, let it win.
          }
        } while (++index < collisions.length);
        return null;
      }
      if (collision == val) {
        return val;
      }
      if (isValForKey.test(key, collision)) {
        final V witness = (V) OA.compareAndExchangeRelease(collisions, index, collision, val);
        if (witness == collision) {
          return val;
        }
        if (key.equals(witness)) {
          return witness; // If another thread raced to PUT, let it win.
        }
      }
    } while (++index < collisions.length);
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean remove(final K key) {
    final int hash = hashCoder.applyAsInt(key) & mask;
    final V[] collisions = getCreateCollisions(hash);
    synchronized (collisions) {
      int index = 0;
      do {
        V collision = (V) OA.getAcquire(collisions, index);
        if (collision == null) {
          return false;
        }
        if (isValForKey.test(key, collision)) {
          final int counterOffset = hash << maxCollisionsShift;
          int counterIndex = counterOffset + index;
          for (int nextIndex = index + 1;;++index, ++nextIndex) {
            // Element at collisionIndex is a zero count known non-null that cannot be
            // concurrently swapped, or a collision that has already been moved to the left.
            OA.setRelease(collisions, index, null);
            if (nextIndex == collisions.length) {
              return true;
            }
            final Object next = OA.getAcquire(collisions, nextIndex);
            if (next == null) {
              return true;
            }
            // - Try to slide entries and their counters to the front.
            // - If a new collision concurrently sneaks in, break out.
            if (OA.compareAndExchangeRelease(collisions, index, null, next) != null) {
              return true;
            }
            // Counter misses may occur during this transition.
            final int count = ((int) BA.getAcquire(counters, ++counterIndex)) & 0xff;
            BA.setRelease(counters, counterIndex - 1, (byte) (count >> 1));
          }
        }
      } while (++index < collisions.length);
    }
    return false;
  }

  @Override
  public String toString() {
    return "PackedCollisionCache{" + super.toString() + '}';
  }
}
