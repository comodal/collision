package com.fabahaba.collision.cache;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToIntFunction;

/**
 * @param <K> the type of keys used to map to values
 * @param <L> the type of loaded values before being mapped to type V
 * @param <V> the type of mapped values
 * @author James P. Edwards
 */
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
      final KeyVal<K, V>[][] hashTable,
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
  public <I> V getAggressive(final K key, final Function<K, I> loader,
      final BiFunction<K, I, V> mapper) {
    final int hash = hashCoder.applyAsInt(key) & mask;
    final KeyVal<K, V>[] collisions = getCreateCollisions(hash);
    final int counterOffset = hash << maxCollisionsShift;
    int index = 0;
    do {
      KeyVal<K, V> collision = collisions[index];
      if (collision == null) {
        final I loaded = loader.apply(key);
        if (loaded == null) {
          return null;
        }
        if (index == 0) {
          // If not strict, allow first entry into first collision index.
          if (strict && size.get() > capacity) {  // Nothing to swap with and over capacity.
            return mapper.apply(key, loaded);
          }
        } else if (size.get() > capacity) {
          return checkDecayAndProbSwap(counterOffset, collisions, key, loaded, mapper);
        }
        final KeyVal<K, V> entry = new KeyVal(key, mapper.apply(key, loaded));
        do {
          collision = (KeyVal<K, V>) OA.compareAndExchangeRelease(collisions, index, null, entry);
          if (collision == null) {
            BA.setRelease(counters, counterOffset + index, initCount);
            size.getAndIncrement();
            return entry.val;
          }
          if (key.equals(collision.key)) {
            atomicIncrement(counterOffset + index);
            return collision.val;
          }
        } while (++index < collisions.length && size.get() <= capacity);
        return checkDecayAndProbSwap(counterOffset, collisions, entry);
      }
      if (key.equals(collision.key)) {
        atomicIncrement(counterOffset + index);
        return collision.val;
      }
    } while (++index < collisions.length);
    final I loaded = loader.apply(key);
    return loaded == null ? null
        : checkDecayAndProbSwap(counterOffset, collisions, key, loaded, mapper);
  }

  /**
   * This method assumes either or both a full bucket and to be over capacity.
   */
  @SuppressWarnings("unchecked")
  private <I> V checkDecayAndProbSwap(final int counterOffset, final KeyVal<K, V>[] collisions,
      final K key, final I loaded, final BiFunction<K, I, V> mapper) {
    int index = 0;
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = 0xff;
    synchronized (collisions) {
      for (;;) {
        KeyVal<K, V> collision = (KeyVal<K, V>) OA.getAcquire(collisions, index);
        if (collision == null) { // Assume over capacity.
          final V val = mapper.apply(key, loaded);
          final KeyVal<K, V> entry = new KeyVal(key, val);
          if (index == 0) { // Strict capacity checked in parent call.
            collision = (KeyVal<K, V>) OA.compareAndExchangeRelease(collisions, index, null, entry);
            if (collision == null) {
              BA.setRelease(counters, counterIndex, initCount);
              size.getAndIncrement();
              return val;
            }
            if (key.equals(collision.key)) {
              atomicIncrement(counterIndex);
              return collision.val;
            }
            return val; // Don't cache, lost tie breaker.
          }
          OA.setRelease(collisions, minCounterIndex - counterOffset, entry);
          BA.setRelease(counters, minCounterIndex, initCount);
          decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
          return val;
        }

        if (key.equals(collision.key)) {
          atomicIncrement(counterIndex);
          return collision.val;
        }

        int count = ((int) BA.getAcquire(counters, counterIndex)) & 0xff;
        if (count < minCount) {
          minCount = count;
          minCounterIndex = counterIndex;
        }
        ++counterIndex;
        if (++index == collisions.length) {
          final V val = mapper.apply(key, loaded);
          OA.setRelease(collisions, minCounterIndex - counterOffset, new KeyVal(key, val));
          BA.setRelease(counters, minCounterIndex, initCount);
          if (size.get() > capacity) {
            decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
            return val;
          }
          decay(counterOffset, counterIndex, minCounterIndex);
          return val;
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private V checkDecayAndProbSwap(final int counterOffset, final KeyVal<K, V>[] collisions,
      final KeyVal<K, V> entry) {
    int index = 0;
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = 0xff;
    synchronized (collisions) {
      for (;;) {
        KeyVal<K, V> collision = (KeyVal<K, V>) OA.getAcquire(collisions, index);
        if (collision == null) { // Assume over capacity.
          if (index == 0) { // Strict capacity checked in parent call.
            collision = (KeyVal<K, V>) OA.compareAndExchangeRelease(collisions, index, null, entry);
            if (collision == null) {
              BA.setRelease(counters, counterIndex, initCount);
              size.getAndIncrement();
              return entry.val;
            }
            if (entry.key.equals(collision.key)) {
              atomicIncrement(counterIndex);
              return collision.val;
            }
            return entry.val; // Don't cache, lost tie breaker.
          }
          OA.setRelease(collisions, minCounterIndex - counterOffset, entry);
          BA.setRelease(counters, minCounterIndex, initCount);
          decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
          return entry.val;
        }

        if (entry.key.equals(collision.key)) {
          atomicIncrement(counterIndex);
          return collision.val;
        }

        int count = ((int) BA.getAcquire(counters, counterIndex)) & 0xff;
        if (count < minCount) {
          minCount = count;
          minCounterIndex = counterIndex;
        }
        ++counterIndex;
        if (++index == collisions.length) {
          OA.setRelease(collisions, minCounterIndex - counterOffset, entry);
          BA.setRelease(counters, minCounterIndex, initCount);
          if (size.get() > capacity) {
            decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
            return entry.val;
          }
          decay(counterOffset, counterIndex, minCounterIndex);
          return entry.val;
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  V checkDecayAndSwap(final int counterOffset, final KeyVal<K, V>[] collisions, final K key,
      final Function<K, V> loadAndMap) {
    if (size.get() > capacity) {
      return checkDecayAndProbSwap(counterOffset, collisions, key, loadAndMap);
    }
    int index = 0;
    synchronized (collisions) {
      for (;;) {
        KeyVal<K, V> collision = (KeyVal<K, V>) OA.getAcquire(collisions, index);
        if (collision == null) {
          final V val = loadAndMap.apply(key);
          if (val == null) {
            return null;
          }
          if (index == 0) {
            // If not strict, allow first entry into first collision index.
            if (strict && size.get() > capacity) {
              // Nothing to swap with and over capacity.
              return val;
            }
          } else if (size.get() > capacity) {
            decaySwapAndDrop(counterOffset, counterOffset + index, collisions,
                new KeyVal(key, val));
            return val;
          }
          final KeyVal<K, V> entry = new KeyVal(key, val);
          do {
            collision = (KeyVal<K, V>) OA.compareAndExchangeRelease(collisions, index, null, entry);
            if (collision == null) {
              BA.setRelease(counters, counterOffset + index, initCount);
              size.getAndIncrement();
              return val;
            }
            if (key.equals(collision.key)) {
              atomicIncrement(counterOffset + index);
              return collision.val;
            }
          } while (++index == collisions.length);
          decayAndSwap(counterOffset, counterOffset + collisions.length, collisions, entry);
          return val;
        }
        if (key.equals(collision.key)) {
          atomicIncrement(counterOffset + index);
          return collision.val;
        }
        if (++index == collisions.length) {
          final V val = loadAndMap.apply(key);
          if (val == null) {
            return null;
          }
          if (size.get() > capacity) {
            decaySwapAndDrop(counterOffset, counterOffset + collisions.length, collisions,
                new KeyVal(key, val));
            return val;
          }
          decayAndSwap(counterOffset, counterOffset + collisions.length, collisions,
              new KeyVal(key, val));
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
  V checkDecayAndProbSwap(final int counterOffset, final KeyVal<K, V>[] collisions, final K key,
      final Function<K, V> loadAndMap) {
    int index = 0;
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = 0xff;
    synchronized (collisions) {
      for (;;) {
        KeyVal<K, V> collision = (KeyVal<K, V>) OA.getAcquire(collisions, index);
        if (collision == null) {
          final V val = loadAndMap.apply(key);
          if (val == null) {
            return null;
          }
          if (index == 0) {
            // If not strict, allow first entry into first collision index.
            if (strict && size.get() > capacity) { // Nothing to swap with and over capacity.
              return val;
            }
          } else if (size.get() > capacity) {
            OA.setRelease(collisions, minCounterIndex - counterOffset, new KeyVal(key, val));
            BA.setRelease(counters, minCounterIndex, initCount);
            decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
            return val;
          }
          final KeyVal<K, V> entry = new KeyVal(key, val);
          do {
            collision = (KeyVal<K, V>) OA.compareAndExchangeRelease(collisions, index, null, entry);
            if (collision == null) {
              BA.setRelease(counters, counterOffset + index, initCount);
              size.getAndIncrement();
              return val;
            }
            if (key.equals(collision.key)) {
              atomicIncrement(counterOffset + index);
              return collision.val;
            }
          } while (++index == collisions.length);
          OA.setRelease(collisions, minCounterIndex - counterOffset, entry);
          BA.setRelease(counters, minCounterIndex, initCount);
          decay(counterOffset, counterOffset + collisions.length, minCounterIndex);
          return val;
        }

        if (key.equals(collision.key)) {
          atomicIncrement(counterIndex);
          return collision.val;
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
          OA.setRelease(collisions, minCounterIndex - counterOffset, new KeyVal(key, val));
          BA.setRelease(counters, minCounterIndex, initCount);
          if (size.get() > capacity) {
            decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
            return val;
          }
          decay(counterOffset, counterIndex, minCounterIndex);
          return val;
        }
      }
    }
  }

  private void decayAndDrop(final int counterOffset, final int maxCounterIndex, final int skipIndex,
      final KeyVal[] collisions) {
    int counterIndex = counterOffset;
    do {
      if (counterIndex == skipIndex) {
        continue;
      }
      int count = ((int) BA.getAcquire(counters, counterIndex)) & 0xff;
      if (count == 0) {
        if (counterIndex < skipIndex) {
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
          final Object next = OA.getAcquire(collisions, nextCollisionIndex);
          if (next == null) {
            return;
          }
          // - Try to slide new data and its counter to the front.
          // - If a new collision concurrently sneaks in, break out.
          if (OA.compareAndExchangeRelease(collisions, collisionIndex, null, next) != null) {
            return;
          }
          // Counter misses may occur during this transition.
          count = ((int) BA.getAcquire(counters, ++counterIndex)) & 0xff;
          BA.setRelease(counters, counterIndex - 1, (byte) (count >> 1));
        }
      }
      // Counter misses may occur between these two calls.
      BA.setRelease(counters, counterIndex, (byte) (count >> 1));
    } while (++counterIndex < maxCounterIndex);
  }

  private void decaySwapAndDrop(final int counterOffset, final int maxCounterIndex,
      final KeyVal<K, V>[] collisions, final KeyVal<K, V> entry) {
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = 0xff;
    do {
      int count = ((int) BA.getAcquire(counters, counterIndex)) & 0xff;
      if (count == 0) {
        OA.setRelease(collisions, counterIndex - counterOffset, entry);
        BA.setRelease(counters, counterIndex, initCount);
        while (++counterIndex < maxCounterIndex) {
          count = ((int) BA.getAcquire(counters, counterIndex)) & 0xff;
          if (count > 0) {
            BA.setRelease(counters, counterIndex, (byte) (count >> 1));
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
            final Object next = OA.getAcquire(collisions, nextCollisionIndex);
            if (next == null) {
              return;
            }
            // - Try to slide new data and its counter to the front.
            // - If a new collision concurrently sneaks in, break out.
            if (OA.compareAndExchangeRelease(collisions, collisionIndex, null, next) != null) {
              return;
            }
            // - Counter misses may occur during this transition.
            count = ((int) BA.getAcquire(counters, counterIndex + 1)) & 0xff;
            BA.setRelease(counters, counterIndex++, (byte) (count >> 1));
          }
        }
        return;
      }
      // Counter misses may occur between these two calls.
      BA.setRelease(counters, counterIndex, (byte) (count >> 1));
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
    final KeyVal<K, V>[] collisions = getCreateCollisions(hash);
    int index = 0;
    KeyVal<K, V> entry = null;
    do {
      KeyVal<K, V> collision = (KeyVal<K, V>) OA.getAcquire(collisions, index);
      if (collision == null) {
        if (index == 0) {
          // If not strict, allow first entry into first collision index.
          if (strict && size.get() > capacity) { // Nothing to swap with and over capacity.
            return val;
          }
        } else if (size.get() > capacity) {
          break;
        }
        if (entry == null) {
          entry = new KeyVal(key, val);
        }
        do {
          collision = (KeyVal<K, V>) OA.compareAndExchangeRelease(collisions, index, null, entry);
          if (collision == null) {
            BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
            size.getAndIncrement();
            return val;
          }
          if (key.equals(collision.key)) {
            return collision.val; // If another thread raced to PUT, let it win.
          }
        } while (++index < collisions.length && size.get() <= capacity);
        break;
      }
      if (collision.val == val) {
        return val;
      }
      if (key.equals(collision.key)) {
        if (entry == null) {
          entry = new KeyVal(key, val);
        }
        final KeyVal<K, V> witness = (KeyVal<K, V>) OA
            .compareAndExchangeRelease(collisions, index, collision, entry);
        if (witness == collision) {
          return val;
        }
        if (key.equals(witness.key)) {
          return witness.val; // If another thread raced to PUT, let it win.
        }
      }
    } while (++index < collisions.length);

    index = 0;
    final int counterOffset = hash << maxCollisionsShift;
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = 0xff;
    synchronized (collisions) {
      for (;;++counterIndex) {
        KeyVal<K, V> collision = (KeyVal<K, V>) OA.getAcquire(collisions, index);
        if (collision == null) {  // Assume over capacity.
          if (entry == null) {
            entry = new KeyVal(key, val);
          }
          if (index == 0) {  // Strict capacity checked above.
            collision = (KeyVal<K, V>) OA.compareAndExchangeRelease(collisions, index, null, entry);
            if (collision == null) {
              BA.setRelease(counters, counterIndex, initCount);
              size.getAndIncrement();
              return val;
            }
            if (key.equals(collision.key)) {
              return collision.val; // If another thread raced to PUT, let it win.
            }
            return val; // Don't cache, lost tie breaker.
          }
          OA.setRelease(collisions, minCounterIndex - counterOffset, entry);
          BA.setRelease(counters, minCounterIndex, initCount);
          decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
          return val;
        }

        if (collision.val == val) {
          return val;
        }

        if (key.equals(collision.key)) {
          if (entry == null) {
            entry = new KeyVal(key, val);
          }
          final KeyVal<K, V> witness = (KeyVal<K, V>) OA
              .compareAndExchangeRelease(collisions, index, collision, entry);
          if (witness == collision) {
            return val;
          }
          if (key.equals(witness.key)) {
            return witness.val; // If another thread raced to PUT, let it win.
          }
        }

        int count = ((int) BA.getAcquire(counters, counterIndex)) & 0xff;
        if (count < minCount) {
          minCount = count;
          minCounterIndex = counterIndex;
        }

        if (++index == collisions.length) {
          if (entry == null) {
            entry = new KeyVal(key, val);
          }
          OA.setRelease(collisions, minCounterIndex - counterOffset, entry);
          BA.setRelease(counters, minCounterIndex, initCount);
          if (size.get() > capacity) {
            decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
            return val;
          }
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
    final int hash = hashCoder.applyAsInt(key) & mask;
    final KeyVal<K, V>[] collisions = getCreateCollisions(hash);
    int index = 0;
    KeyVal<K, V> entry = null;
    do {
      KeyVal<K, V> collision = collisions[index];
      if (collision == null) {
        if (index == 0) {
          // If not strict, allow first entry into first collision index.
          if (strict && size.get() > capacity) { // Nothing to swap with and over capacity.
            return val;
          }
        } else if (size.get() > capacity) {
          break;
        }
        entry = new KeyVal(key, val);
        do {
          collision = (KeyVal<K, V>) OA.compareAndExchangeRelease(collisions, index, null, entry);
          if (collision == null) {
            BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
            size.getAndIncrement();
            return val;
          }
          if (key.equals(collision.key)) {
            return collision.val;
          }
        } while (++index < collisions.length && size.get() <= capacity);
        break;
      }
      if (key.equals(collision.key)) {
        return collision.val;
      }
    } while (++index < collisions.length);

    index = 0;
    final int counterOffset = hash << maxCollisionsShift;
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = 0xff;
    synchronized (collisions) {
      for (;;) {
        KeyVal<K, V> collision = (KeyVal<K, V>) OA.getAcquire(collisions, index);
        if (collision == null) {  // Assume over capacity.
          if (entry == null) {
            entry = new KeyVal(key, val);
          }
          if (index == 0) {  // Strict capacity checked above.
            collision = (KeyVal<K, V>) OA.compareAndExchangeRelease(collisions, index, null, entry);
            if (collision == null) {
              BA.setRelease(counters, counterIndex, initCount);
              size.getAndIncrement();
              return val;
            }
            if (key.equals(collision.key)) {
              return collision.val;
            }
            return val; // Don't cache, lost tie breaker.
          }
          OA.setRelease(collisions, minCounterIndex - counterOffset, entry);
          BA.setRelease(counters, minCounterIndex, initCount);
          decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
          return val;
        }

        if (key.equals(collision.key)) {
          return collision.val;
        }

        int count = ((int) BA.getAcquire(counters, counterIndex)) & 0xff;
        if (count < minCount) {
          minCount = count;
          minCounterIndex = counterIndex;
        }
        ++counterIndex;
        if (++index == collisions.length) {
          if (entry == null) {
            entry = new KeyVal(key, val);
          }
          OA.setRelease(collisions, minCounterIndex - counterOffset, entry);
          BA.setRelease(counters, minCounterIndex, initCount);
          if (size.get() > capacity) {
            decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
            return val;
          }
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
    final int hash = hashCoder.applyAsInt(key) & mask;
    final KeyVal<K, V>[] collisions = getCreateCollisions(hash);
    int index = 0;
    do {
      KeyVal<K, V> collision = collisions[index];
      if (collision == null) {
        if (size.get() > capacity) {
          return null;
        }
        final KeyVal<K, V> entry = new KeyVal(key, val);
        do {
          collision = (KeyVal<K, V>) OA.compareAndExchangeRelease(collisions, index, null, entry);
          if (collision == null) {
            BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
            size.getAndIncrement();
            return val;
          }
          if (key.equals(collision.key)) {
            return collision.val;
          }
        } while (++index < collisions.length && size.get() <= capacity);
        return null;
      }
      if (key.equals(collision.key)) {
        return collision.val;
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
    final KeyVal<K, V>[] collisions = getCreateCollisions(hash);
    int index = 0;
    KeyVal<K, V> entry = null;
    do {
      KeyVal<K, V> collision = (KeyVal<K, V>) OA.getAcquire(collisions, index);
      if (collision == null) {
        if (size.get() > capacity) {
          return null;
        }
        if (entry == null) {
          entry = new KeyVal(key, val);
        }
        do {
          collision = (KeyVal<K, V>) OA.compareAndExchangeRelease(collisions, index, null, entry);
          if (collision == null) {
            BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
            size.getAndIncrement();
            return val;
          }
          if (key.equals(collision.key)) {
            return collision.val; // If another thread raced to PUT, let it win.
          }
        } while (++index < collisions.length && size.get() <= capacity);
        return null;
      }
      if (collision.val == val) {
        return val;
      }
      if (key.equals(collision.key)) {
        if (entry == null) {
          entry = new KeyVal(key, val);
        }
        final KeyVal<K, V> witness = (KeyVal<K, V>) OA
            .compareAndExchangeRelease(collisions, index, collision, entry);
        if (witness == collision) {
          return val;
        }
        if (key.equals(witness.key)) {
          return witness.val; // If another thread raced to PUT, let it win.
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
    final KeyVal<K, V>[] collisions = getCreateCollisions(hash);
    synchronized (collisions) {
      int index = 0;
      do {
        KeyVal<K, V> collision = (KeyVal<K, V>) OA.getAcquire(collisions, index);
        if (collision == null) {
          return false;
        }
        if (key.equals(collision.key)) {
          size.getAndDecrement();
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
    return "SparseEntryCollisionCache{capacity=" + capacity
        + ", strictCapacity=" + strict
        + ", size=" + size.get()
        + ", " + super.toString() + '}';
  }
}
