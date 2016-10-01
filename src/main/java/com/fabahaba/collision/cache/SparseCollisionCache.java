package com.fabahaba.collision.cache;

import java.util.concurrent.atomic.AtomicInteger;
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
final class SparseCollisionCache<K, L, V> extends BaseCollisionCache<K, L, V> {

  private final int capacity;
  private final AtomicInteger size;

  SparseCollisionCache(final int capacity, final Class<V> valueType, final int maxCollisionsShift,
      final byte[] counters, final int initCount, final int pow2LogFactor, final V[][] hashTable,
      final ToIntFunction<K> hashCoder, final BiPredicate<K, Object> isValForKey,
      final Function<K, L> loader, final BiFunction<K, L, V> finalizer) {
    super(valueType, maxCollisionsShift, counters, initCount, pow2LogFactor, hashTable, hashCoder,
        isValForKey, loader, finalizer);
    this.capacity = capacity;
    this.size = new AtomicInteger();
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
        // Always allow entry at index 0, regardless of capacity.
        if (index > 0 && size.get() > capacity) {
          return checkDecayAndSwapLFU(counterOffset, collisions, key, loaded, mapper);
        }
        final V val = mapper.apply(key, loaded);
        do {
          final Object witness = OA.compareAndExchangeRelease(collisions, index, null, val);
          if (witness == null) {
            BA.setRelease(counters, counterOffset + index, initCount);
            size.getAndIncrement();
            return val;
          }
          if (isValForKey.test(key, witness)) {
            atomicIncrement(counterOffset + index);
            return (V) witness;
          }
        } while (++index < collisions.length && size.get() <= capacity);
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

  @SuppressWarnings("unchecked")
  private <I> V checkDecayAndSwapLFU(final int counterOffset, final Object[] collisions,
      final K key, final I loaded, final BiFunction<K, I, V> mapper) {
    int index = 0;
    synchronized (collisions) {
      do { // Double-check locked volatile before swapping LFU to help prevent duplicates.
        Object collision = OA.getVolatile(collisions, index);
        if (collision == null) {
          if (index > 0) { // Always allow entry at index 0, regardless of capacity.
            break;
          }
          final V val = mapper.apply(key, loaded);
          if (val == null) {
            return null;
          }
          collision = OA.compareAndExchangeRelease(collisions, index, null, val);
          if (collision == null) {
            BA.setRelease(counters, counterOffset + index, initCount);
            size.getAndIncrement();
            return val;
          }
          if (isValForKey.test(key, collision)) {
            atomicIncrement(counterOffset + index);
            return (V) collision;
          }
          // Don't cache, lost tie breaker.
          return val;
        }
        if (isValForKey.test(key, collision)) {
          atomicIncrement(counterOffset + index);
          return (V) collision;
        }
      } while (++index < collisions.length);
      final V val = mapper.apply(key, loaded);
      if (val == null) {
        return null;
      }
      decayAndSwapLFU(counterOffset, counterOffset + index, collisions, val);
      return val;
    }
  }

  @SuppressWarnings("unchecked")
  private V checkDecayAndSwapLFU(final int counterOffset, final Object[] collisions,
      final K key, final V val) {
    int index = 0;
    synchronized (collisions) {
      do { // Double-check locked volatile before swapping LFU to help prevent duplicates.
        Object collision = OA.getVolatile(collisions, index);
        if (collision == null) {
          if (index > 0) { // Always allow entry at index 0, regardless of capacity.
            break;
          }
          collision = OA.compareAndExchangeRelease(collisions, index, null, val);
          if (collision == null) {
            BA.setRelease(counters, counterOffset + index, initCount);
            size.getAndIncrement();
            return val;
          }
          if (isValForKey.test(key, collision)) {
            atomicIncrement(counterOffset + index);
            return (V) collision;
          }
          // Don't cache, lost tie breaker.
          return val;
        }
        if (isValForKey.test(key, collision)) {
          atomicIncrement(counterOffset + index);
          return (V) collision;
        }
      } while (++index < collisions.length);
      decayAndSwapLFU(counterOffset, counterOffset + index, collisions, val);
    }
    return val;
  }

  /**
   * Divides all counters for values within a hash bucket (collisions), swaps the value for the
   * least frequently used, and sets its counter to an initial value.  Also evicts the tail entry if
   * its count is zero.
   *
   * @param counterOffset   beginning counter array index corresponding to collision values.
   * @param maxCounterIndex Max counter index for known non null collision values.
   * @param collisions      values sitting in a hash bucket.
   * @param val             The value to put in place of the least frequently used value.
   */
  private void decayAndSwapLFU(final int counterOffset, final int maxCounterIndex,
      final Object[] collisions, final Object val) {
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = 0xff;
    do {
      int count = ((int) BA.getVolatile(counters, counterIndex)) & 0xff;
      if (count == 0) {
        OA.setRelease(collisions, counterIndex - counterOffset, val);
        BA.setRelease(counters, counterIndex, initCount);
        while (++counterIndex < maxCounterIndex) {
          count = ((int) BA.getVolatile(counters, counterIndex)) & 0xff;
          if (count > 0) {
            BA.setRelease(counters, counterIndex, (byte) (count >>> 1));
            continue;
          }
          size.getAndDecrement();
          for (int collisionIndex = counterIndex - counterOffset,
               nextCollisionIndex = collisionIndex + 1;;++collisionIndex, ++nextCollisionIndex) {
            // Element at collisionIndex is a zero count known non-null that cannot be
            // concurrently swapped, or a collision that has already been moved to the left.
            OA.setRelease(collisions, collisionIndex, null);
            if (nextCollisionIndex == collisions.length) {
              return;
            }
            final Object next = OA.getVolatile(collisions, nextCollisionIndex);
            // - Try to slide new data and its counter to the front.
            // - If a new collision concurrently sneaks in, break out.
            // - Counter misses may occur during this transition.
            if (next != null && OA.compareAndSet(collisions, collisionIndex, null, next)) {
              if (counterIndex >= maxCounterIndex && next.equals(val)) {
                // Handles the unlikely corner case of capacity becoming available and the same
                // value slipping into one of the null spaces concurrently.  Set its count to zero
                // so that it wll be swapped on the next collision.
                BA.setRelease(counters, counterIndex++, (byte) 0);
              } else {
                count = ((int) BA.getVolatile(counters, counterIndex + 1)) & 0xff;
                BA.setRelease(counters, counterIndex++, (byte) (count >>> 1));
              }
              continue;
            }
            return;
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
    } while (++counterIndex < maxCounterIndex);
    OA.setRelease(collisions, minCounterIndex - counterOffset, val);
    BA.setRelease(counters, minCounterIndex, initCount);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
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
        // Always allow entry at index 0, regardless of capacity.
        if (index > 0 && size.get() > capacity) {
          break;
        }
        collision = OA.compareAndExchangeRelease(collisions, index, null, val);
        if (collision == null) {
          BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
          size.getAndIncrement();
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
      do { // Double-check locked volatile before swapping LFU to prevent duplicates.
        Object collision = OA.getVolatile(collisions, index);
        if (collision == null) {
          if (index > 0) { // Always allow entry at index 0, regardless of capacity.
            break;
          }
          collision = OA.compareAndExchangeRelease(collisions, index, null, val);
          if (collision == null) {
            BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
            size.getAndIncrement();
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
      final int counterOffset = hash << maxCollisionsShift;
      decayAndSwapLFU(counterOffset, counterOffset + index, collisions, val);
    }
    return val;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
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
        // Always allow entry at index 0, regardless of capacity.
        if (index > 0 && size.get() > capacity) {
          break;
        }
        final Object witness = OA.compareAndExchangeRelease(collisions, index, null, val);
        if (witness == null) {
          BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
          size.getAndIncrement();
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
      do { // Double-check locked volatile before swapping LFU to help prevent duplicates.
        Object collision = OA.getVolatile(collisions, index);
        if (collision == null) {
          if (index > 0) { // Always allow entry at index 0, regardless of capacity.
            break;
          }
          collision = OA.compareAndExchangeRelease(collisions, index, null, val);
          if (collision == null) {
            BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
            size.getAndIncrement();
            return val;
          }
        }
        if (isValForKey.test(key, collision)) {
          return (V) collision;
        }
      } while (++index < collisions.length);
      final int counterOffset = hash << maxCollisionsShift;
      decayAndSwapLFU(counterOffset, counterOffset + index, collisions, val);
    }
    return val;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
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
        if (size.get() > capacity) {
          return null;
        }
        final Object witness = OA.compareAndExchangeRelease(collisions, index, null, val);
        if (witness == null) {
          BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
          size.getAndIncrement();
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
  @Override
  @SuppressWarnings("unchecked")
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
        if (size.get() > capacity) {
          return null;
        }
        collision = OA.compareAndExchangeRelease(collisions, index, null, val);
        if (collision == null) {
          BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
          size.getAndIncrement();
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
    return "SparseCollisionCache{capacity=" + capacity
        + ", size=" + size.get()
        + ", " + super.toString() + '}';
  }
}
