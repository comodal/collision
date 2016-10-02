package com.fabahaba.collision.cache;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToIntFunction;

final class SparseEntryCollisionCache<K, L, V> extends BaseEntryCollisionCache<K, L, V> {

  private final int capacity;
  private final boolean strict;
  private final AtomicInteger size;

  SparseEntryCollisionCache(
      final int capacity,
      final boolean strictCapacity,
      final int maxCollisionsShift,
      final byte[] counters,
      final int initCount,
      final int pow2LogFactor,
      final Map.Entry<K, V>[][] hashTable,
      final ToIntFunction<K> hashCoder,
      final Function<K, L> loader,
      final BiFunction<K, L, V> mapper) {
    super(maxCollisionsShift, counters, initCount, pow2LogFactor, hashTable, hashCoder,
        loader, mapper);
    this.capacity = capacity;
    this.strict = strictCapacity;
    this.size = new AtomicInteger();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public <I> V get(final K key, final Function<K, I> loader, final BiFunction<K, I, V> mapper) {
    final int hash = hashCoder.applyAsInt(key) & mask;
    final Map.Entry<K, V>[] collisions = getCreateCollisions(hash);
    final int counterOffset = hash << maxCollisionsShift;
    int index = 0;
    do {
      final Map.Entry<K, V> collision = collisions[index];
      if (collision == null) {
        final I loaded = loader.apply(key);
        if (loaded == null) {
          return null;
        }
        if (index == 0) { // Nothing to swap with and over capacity.
          if (strict && size.get() > capacity) {
            return mapper.apply(key, loaded); // TODO Async or parallel scan for 0 counts to expire?
          } // If not strict, allows allow first entry into first collision index.
        } else if (size.get() > capacity) {
          return checkDecayAndSwapLFU(counterOffset, collisions, key, loaded, mapper);
        }
        final Map.Entry<K, V> entry = Map.entry(key, mapper.apply(key, loaded));
        do {
          final Map.Entry<K, V> witness = (Map.Entry<K, V>) OA
              .compareAndExchangeRelease(collisions, index, null, entry);
          if (witness == null) {
            BA.setRelease(counters, counterOffset + index, initCount);
            size.getAndIncrement();
            return entry.getValue();
          }
          if (key.equals(witness.getKey())) {
            atomicIncrement(counterOffset + index);
            return witness.getValue();
          }
        } while (++index < collisions.length && size.get() <= capacity);
        return checkDecayAndSwapLFU(counterOffset, collisions, entry);
      }
      if (key.equals(collision.getKey())) {
        atomicIncrement(counterOffset + index);
        return collision.getValue();
      }
    } while (++index < collisions.length);
    final I loaded = loader.apply(key);
    return loaded == null ? null
        : checkDecayAndSwapLFU(counterOffset, collisions, key, loaded, mapper);
  }

  @SuppressWarnings("unchecked")
  private <I> V checkDecayAndSwapLFU(final int counterOffset, final Map.Entry<K, V>[] collisions,
      final K key, final I loaded, final BiFunction<K, I, V> mapper) {
    int index = 0;
    synchronized (collisions) {
      do { // Double-check locked volatile before swapping LFU to help prevent duplicates.
        Map.Entry<K, V> collision = (Map.Entry<K, V>) OA.getVolatile(collisions, index);
        if (collision == null) {
          if (index > 0) { // Always allow entry at index 0, regardless of capacity.
            break;
          }
          final Map.Entry<K, V> entry = Map.entry(key, mapper.apply(key, loaded));
          collision = (Map.Entry<K, V>) OA
              .compareAndExchangeRelease(collisions, index, null, entry);
          if (collision == null) {
            BA.setRelease(counters, counterOffset + index, initCount);
            size.getAndIncrement();
            return entry.getValue();
          }
          if (key.equals(collision.getKey())) {
            atomicIncrement(counterOffset + index);
            return collision.getValue();
          }
          // Don't cache, lost tie breaker.
          return entry.getValue();
        }
        if (key.equals(collision.getKey())) {
          atomicIncrement(counterOffset + index);
          return collision.getValue();
        }
      } while (++index < collisions.length);
      final Map.Entry<K, V> entry = Map.entry(key, mapper.apply(key, loaded));
      decayAndSwapLFU(counterOffset, counterOffset + index, collisions, entry);
      return entry.getValue();
    }
  }

  @SuppressWarnings("unchecked")
  private V checkDecayAndSwapLFU(final int counterOffset, final Map.Entry<K, V>[] collisions,
      final Map.Entry<K, V> entry) {
    int index = 0;
    synchronized (collisions) {
      do { // Double-check locked volatile before swapping LFU to help prevent duplicates.
        Map.Entry<K, V> collision = (Map.Entry<K, V>) OA.getVolatile(collisions, index);
        if (collision == null) {
          if (index > 0) { // Always allow entry at index 0, regardless of capacity.
            break;
          }
          collision = (Map.Entry<K, V>) OA
              .compareAndExchangeRelease(collisions, index, null, entry);
          if (collision == null) {
            BA.setRelease(counters, counterOffset + index, initCount);
            size.getAndIncrement();
            return entry.getValue();
          }
          if (entry.getKey().equals(collision.getKey())) {
            atomicIncrement(counterOffset + index);
            return collision.getValue();
          }
          // Don't cache, lost tie breaker.
          return entry.getValue();
        }
        if (entry.getKey().equals(collision.getKey())) {
          atomicIncrement(counterOffset + index);
          return collision.getValue();
        }
      } while (++index < collisions.length);
      decayAndSwapLFU(counterOffset, counterOffset + index, collisions, entry);
    }
    return entry.getValue();
  }

  /**
   * Divides all counters for values within a hash bucket (collisions), swaps the value for the
   * least frequently used, and sets its counter to an initial value.  Also evicts the tail entry if
   * its count is zero.
   *
   * @param counterOffset   beginning counter array index corresponding to collision values.
   * @param maxCounterIndex Max counter index for known non null collision values.
   * @param collisions      values sitting in a hash bucket.
   * @param entry           The value to put in place of the least frequently used value.
   */
  @SuppressWarnings("unchecked")
  private void decayAndSwapLFU(final int counterOffset, final int maxCounterIndex,
      final Map.Entry<K, V>[] collisions, final Map.Entry<K, V> entry) {
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = 0xff;
    do {
      int count = ((int) BA.getVolatile(counters, counterIndex)) & 0xff;
      if (count == 0) {
        OA.setRelease(collisions, counterIndex - counterOffset, entry);
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
            final Map.Entry<K, V> next = (Map.Entry<K, V>) OA
                .getVolatile(collisions, nextCollisionIndex);
            // - Try to slide new data and its counter to the front.
            // - If a new collision concurrently sneaks in, break out.
            // - Counter misses may occur during this transition.
            if (next != null && OA.compareAndSet(collisions, collisionIndex, null, next)) {
              if (counterIndex >= maxCounterIndex && next.getKey().equals(entry.getKey())) {
                // Handles the unlikely corner case of capacity becoming available and
                // the same value slipping into one of the null spaces concurrently.  Set its
                // count to zero so that it wll be swapped on the next collision.
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
    OA.setRelease(collisions, minCounterIndex - counterOffset, entry);
    BA.setRelease(counters, minCounterIndex, initCount);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public V putReplace(final K key, final V val) {
    final int hash = hashCoder.applyAsInt(key) & mask;
    final Map.Entry<K, V>[] collisions = getCreateCollisions(hash);
    int index = 0;
    Map.Entry<K, V> newEntry = null;
    do {
      Map.Entry<K, V> collision = (Map.Entry<K, V>) OA.getVolatile(collisions, index);
      if (collision == null) {
        if (index == 0) { // Nothing to swap with and over capacity.
          if (strict && size.get() > capacity) {
            return val; // TODO Async or parallel scan for 0 counts to expire?
          } // If not strict, allows allow first entry into first collision index.
        } else if (size.get() > capacity) {
          break;
        }
        if (newEntry == null) {
          newEntry = Map.entry(key, val);
        }
        collision = (Map.Entry<K, V>) OA
            .compareAndExchangeRelease(collisions, index, null, newEntry);
        if (collision == null) {
          BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
          size.getAndIncrement();
          return val;
        }
        // If another thread raced to PUT, let it win.
        if (key.equals(collision.getKey())) {
          return collision.getValue();
        }
        continue;
      }
      if (collision.getValue() == val) {
        return val;
      }
      if (key.equals(collision.getKey())) {
        if (newEntry == null) {
          newEntry = Map.entry(key, val);
        }
        final Map.Entry<K, V> witness = (Map.Entry<K, V>) OA
            .compareAndExchangeRelease(collisions, index, collision, newEntry);
        if (witness == collision) {
          return val;
        }
        // If another thread raced to PUT, let it win.
        if (key.equals(witness.getKey())) {
          return witness.getValue();
        }
      }
    } while (++index < collisions.length);
    index = 0;
    synchronized (collisions) {
      do { // Double-check locked volatile before swapping LFU to prevent duplicates.
        Map.Entry<K, V> collision = (Map.Entry<K, V>) OA.getVolatile(collisions, index);
        if (collision == null) {
          if (index > 0) { // Always allow entry at index 0, regardless of capacity.
            break;
          }
          if (newEntry == null) {
            newEntry = Map.entry(key, val);
          }
          collision = (Map.Entry<K, V>) OA
              .compareAndExchangeRelease(collisions, index, null, newEntry);
          if (collision == null) {
            BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
            size.getAndIncrement();
            return val;
          }
          // If another thread raced to PUT, let it win.
          if (key.equals(collision.getKey())) {
            return collision.getValue();
          }
          continue;
        }
        if (collision.getValue() == val) {
          return val;
        }
        if (key.equals(collision.getKey())) {
          if (newEntry == null) {
            newEntry = Map.entry(key, val);
          }
          final Map.Entry<K, V> witness = (Map.Entry<K, V>) OA
              .compareAndExchangeRelease(collisions, index, collision, newEntry);
          if (witness == collision) {
            return val;
          }
          // If another thread raced to PUT, let it win.
          if (key.equals(witness.getKey())) {
            return witness.getValue();
          }
        }
      } while (++index < collisions.length);
      if (newEntry == null) {
        newEntry = Map.entry(key, val);
      }
      final int counterOffset = hash << maxCollisionsShift;
      decayAndSwapLFU(counterOffset, counterOffset + index, collisions, newEntry);
    }
    return val;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public V putIfAbsent(final K key, final V val) {
    final int hash = hashCoder.applyAsInt(key) & mask;
    final Map.Entry<K, V>[] collisions = getCreateCollisions(hash);
    int index = 0;
    Map.Entry<K, V> newEntry = null;
    do {
      final Map.Entry<K, V> collision = collisions[index];
      if (collision == null) {
        if (index == 0) { // Nothing to swap with and over capacity.
          if (strict && size.get() > capacity) {
            return val; // TODO Async or parallel scan for 0 counts to expire?
          } // If not strict, allows allow first entry into first collision index.
        } else if (size.get() > capacity) {
          break;
        }
        if (newEntry == null) {
          newEntry = Map.entry(key, val);
        }
        final Map.Entry<K, V> witness = (Map.Entry<K, V>) OA
            .compareAndExchangeRelease(collisions, index, null, newEntry);
        if (witness == null) {
          BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
          size.getAndIncrement();
          return val;
        }
        if (key.equals(witness.getKey())) {
          return witness.getValue();
        }
        continue;
      }
      if (key.equals(collision.getKey())) {
        return collision.getValue();
      }
    } while (++index < collisions.length);
    index = 0;
    synchronized (collisions) {
      do { // Double-check locked volatile before swapping LFU to help prevent duplicates.
        Map.Entry<K, V> collision = (Map.Entry<K, V>) OA.getVolatile(collisions, index);
        if (collision == null) {
          if (index > 0) { // Always allow entry at index 0, regardless of capacity.
            break;
          }
          if (newEntry == null) {
            newEntry = Map.entry(key, val);
          }
          collision = (Map.Entry<K, V>) OA
              .compareAndExchangeRelease(collisions, index, null, newEntry);
          if (collision == null) {
            BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
            size.getAndIncrement();
            return val;
          }
        }
        if (key.equals(collision.getKey())) {
          return collision.getValue();
        }
      } while (++index < collisions.length);
      if (newEntry == null) {
        newEntry = Map.entry(key, val);
      }
      final int counterOffset = hash << maxCollisionsShift;
      decayAndSwapLFU(counterOffset, counterOffset + index, collisions, newEntry);
    }
    return val;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public V putIfSpaceAbsent(final K key, final V val) {
    final int hash = hashCoder.applyAsInt(key) & mask;
    final Map.Entry<K, V>[] collisions = getCreateCollisions(hash);
    int index = 0;
    Map.Entry<K, V> newEntry = null;
    do {
      final Map.Entry<K, V> collision = collisions[index];
      if (collision == null) {
        if (size.get() > capacity) {
          return null;
        }
        if (newEntry == null) {
          newEntry = Map.entry(key, val);
        }
        final Map.Entry<K, V> witness = (Map.Entry<K, V>) OA
            .compareAndExchangeRelease(collisions, index, null, newEntry);
        if (witness == null) {
          BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
          size.getAndIncrement();
          return val;
        }
        if (key.equals(witness.getKey())) {
          return witness.getValue();
        }
        continue;
      }
      if (key.equals(collision.getKey())) {
        return collision.getValue();
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
    final int hash = hashCoder.applyAsInt(key) & mask;
    final Map.Entry<K, V>[] collisions = getCreateCollisions(hash);
    int index = 0;
    Map.Entry<K, V> newEntry = null;
    do {
      Map.Entry<K, V> collision = (Map.Entry<K, V>) OA.getVolatile(collisions, index);
      if (collision == null) {
        if (size.get() > capacity) {
          return null;
        }
        if (newEntry == null) {
          newEntry = Map.entry(key, val);
        }
        collision = (Map.Entry<K, V>) OA
            .compareAndExchangeRelease(collisions, index, null, newEntry);
        if (collision == null) {
          BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
          size.getAndIncrement();
          return val;
        }
        // If another thread raced to PUT, let it win.
        if (key.equals(collision.getKey())) {
          return collision.getValue();
        }
        continue;
      }
      if (collision.getValue() == val) {
        return val;
      }
      if (key.equals(collision.getKey())) {
        if (newEntry == null) {
          newEntry = Map.entry(key, val);
        }
        final Map.Entry<K, V> witness = (Map.Entry<K, V>) OA
            .compareAndExchangeRelease(collisions, index, collision, newEntry);
        if (witness == collision) {
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
    return "SparseEntryCollisionCache{capacity=" + capacity
        + ", strictCapacity=" + strict
        + ", size=" + size.get()
        + ", " + super.toString() + '}';
  }
}
