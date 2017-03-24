package systems.comodal.collision.cache;

import static systems.comodal.collision.cache.AtomicLogCounters.MAX_COUNT;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

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
      final KeyVal<K, V>[][] hashTable,
      final IntFunction<KeyVal<K, V>[]> getBucket,
      final AtomicLogCounters counters,
      final ToIntFunction<K> hashCoder,
      final Function<K, L> loader,
      final BiFunction<K, L, V> mapper) {
    super(maxCollisionsShift, hashTable, getBucket, counters, hashCoder, loader, mapper);
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
    final KeyVal<K, V>[] collisions = getBucket.apply(hash);
    final int counterOffset = hash << maxCollisionsShift;
    int index = 0;
    do {
      KeyVal<K, V> collision = (KeyVal<K, V>) COLLISIONS.getOpaque(collisions, index);
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
        final KeyVal<K, V> entry = new KeyVal<>(key, mapper.apply(key, loaded));
        do {
          collision = (KeyVal<K, V>) COLLISIONS.compareAndExchange(collisions, index, null, entry);
          if (collision == null) {
            counters.initializeOpaque(counterOffset + index);
            size.getAndIncrement();
            return entry.val;
          }
          if (key.equals(collision.key)) {
            counters.increment(counterOffset + index);
            return collision.val;
          }
        } while (++index < collisions.length && size.get() <= capacity);
        return checkDecayAndProbSwap(counterOffset, collisions, entry);
      }
      if (key.equals(collision.key)) {
        counters.increment(counterOffset + index);
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
    int minCount = MAX_COUNT;
    synchronized (collisions) {
      for (; ; ) {
        KeyVal<K, V> collision = (KeyVal<K, V>) COLLISIONS.getOpaque(collisions, index);
        if (collision == null) { // Assume over capacity.
          final V val = mapper.apply(key, loaded);
          final KeyVal<K, V> entry = new KeyVal<>(key, val);
          if (index == 0) { // Strict capacity checked in parent call.
            collision = (KeyVal<K, V>) COLLISIONS
                .compareAndExchange(collisions, index, null, entry);
            if (collision == null) {
              counters.initializeOpaque(counterIndex);
              size.getAndIncrement();
              return val;
            }
            if (key.equals(collision.key)) {
              counters.increment(counterIndex);
              return collision.val;
            }
            return val; // Don't cache, lost tie breaker.
          }
          COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, entry);
          counters.initializeOpaque(minCounterIndex);
          decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
          return val;
        }

        if (key.equals(collision.key)) {
          counters.increment(counterIndex);
          return collision.val;
        }

        int count = counters.getOpaque(counterIndex);
        if (count < minCount) {
          minCount = count;
          minCounterIndex = counterIndex;
        }
        ++counterIndex;
        if (++index == collisions.length) {
          final V val = mapper.apply(key, loaded);
          COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, new KeyVal<>(key, val));
          counters.initializeOpaque(minCounterIndex);
          if (size.get() > capacity) {
            decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
            return val;
          }
          counters.decay(counterOffset, counterIndex, minCounterIndex);
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
    int minCount = MAX_COUNT;
    synchronized (collisions) {
      for (; ; ) {
        KeyVal<K, V> collision = (KeyVal<K, V>) COLLISIONS.getOpaque(collisions, index);
        if (collision == null) { // Assume over capacity.
          if (index == 0) { // Strict capacity checked in parent call.
            collision = (KeyVal<K, V>) COLLISIONS
                .compareAndExchange(collisions, index, null, entry);
            if (collision == null) {
              counters.initializeOpaque(counterIndex);
              size.getAndIncrement();
              return entry.val;
            }
            if (entry.key.equals(collision.key)) {
              counters.increment(counterIndex);
              return collision.val;
            }
            return entry.val; // Don't cache, lost tie breaker.
          }
          COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, entry);
          counters.initializeOpaque(minCounterIndex);
          decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
          return entry.val;
        }

        if (entry.key.equals(collision.key)) {
          counters.increment(counterIndex);
          return collision.val;
        }

        int count = counters.getOpaque(counterIndex);
        if (count < minCount) {
          minCount = count;
          minCounterIndex = counterIndex;
        }
        ++counterIndex;
        if (++index == collisions.length) {
          COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, entry);
          counters.initializeOpaque(minCounterIndex);
          if (size.get() > capacity) {
            decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
            return entry.val;
          }
          counters.decay(counterOffset, counterIndex, minCounterIndex);
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
      for (; ; ) {
        KeyVal<K, V> collision = (KeyVal<K, V>) COLLISIONS.getOpaque(collisions, index);
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
                new KeyVal<>(key, val));
            return val;
          }
          final KeyVal<K, V> entry = new KeyVal<>(key, val);
          do {
            collision = (KeyVal<K, V>) COLLISIONS
                .compareAndExchange(collisions, index, null, entry);
            if (collision == null) {
              counters.initializeOpaque(counterOffset + index);
              size.getAndIncrement();
              return val;
            }
            if (key.equals(collision.key)) {
              counters.increment(counterOffset + index);
              return collision.val;
            }
          } while (++index == collisions.length);
          decayAndSwap(counterOffset, counterOffset + collisions.length, collisions, entry);
          return val;
        }
        if (key.equals(collision.key)) {
          counters.increment(counterOffset + index);
          return collision.val;
        }
        if (++index == collisions.length) {
          final V val = loadAndMap.apply(key);
          if (val == null) {
            return null;
          }
          if (size.get() > capacity) {
            decaySwapAndDrop(counterOffset, counterOffset + collisions.length, collisions,
                new KeyVal<>(key, val));
            return val;
          }
          decayAndSwap(counterOffset, counterOffset + collisions.length, collisions,
              new KeyVal<>(key, val));
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
    int minCount = MAX_COUNT;
    synchronized (collisions) {
      for (; ; ) {
        KeyVal<K, V> collision = (KeyVal<K, V>) COLLISIONS.getOpaque(collisions, index);
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
            COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, new KeyVal<>(key, val));
            counters.initializeOpaque(minCounterIndex);
            decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
            return val;
          }
          final KeyVal<K, V> entry = new KeyVal<>(key, val);
          do {
            collision = (KeyVal<K, V>) COLLISIONS
                .compareAndExchange(collisions, index, null, entry);
            if (collision == null) {
              counters.initializeOpaque(counterOffset + index);
              size.getAndIncrement();
              return val;
            }
            if (key.equals(collision.key)) {
              counters.increment(counterOffset + index);
              return collision.val;
            }
          } while (++index == collisions.length);
          COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, entry);
          counters.initializeOpaque(minCounterIndex);
          counters.decay(counterOffset, counterOffset + collisions.length, minCounterIndex);
          return val;
        }

        if (key.equals(collision.key)) {
          counters.increment(counterIndex);
          return collision.val;
        }

        int count = counters.getOpaque(counterIndex);
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
          COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, new KeyVal<>(key, val));
          counters.initializeOpaque(minCounterIndex);
          if (size.get() > capacity) {
            decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
            return val;
          }
          counters.decay(counterOffset, counterIndex, minCounterIndex);
          return val;
        }
      }
    }
  }

  private void decayAndDrop(final int counterOffset, final int maxCounterIndex,
      final int skipIndex, final KeyVal<K, V>[] collisions) {
    int counterIndex = counterOffset;
    do {
      if (counterIndex == skipIndex) {
        continue;
      }
      int count = counters.getOpaque(counterIndex);
      if (count == 0) {
        if (counterIndex < skipIndex) {
          continue;
        }
        if (size.getAndDecrement() <= capacity) {
          size.getAndIncrement();
          continue;
        }
        for (int collisionIndex = counterIndex - counterOffset,
            nextCollisionIndex = collisionIndex + 1; ; ++collisionIndex, ++nextCollisionIndex) {
          if (nextCollisionIndex == collisions.length) {
            COLLISIONS.setOpaque(collisions, collisionIndex, null);
            return;
          }
          Object next = COLLISIONS.getOpaque(collisions, nextCollisionIndex);
          if (next == null) {
            COLLISIONS.setOpaque(collisions, collisionIndex, null);
            next = COLLISIONS.getOpaque(collisions, nextCollisionIndex);
            if (next == null
                || COLLISIONS.compareAndExchange(collisions, collisionIndex, null, next) != null) {
              return;
            }
          } else {
            COLLISIONS.setOpaque(collisions, collisionIndex, next);
          }
          // Counter misses may occur during this transition.
          count = counters.getOpaque(++counterIndex);
          counters.setOpaque(counterIndex - 1, count >> 1);
        }
      }
      // Counter misses may occur between these two calls.
      counters.setOpaque(counterIndex, count >> 1);
    } while (++counterIndex < maxCounterIndex);
  }

  private void decaySwapAndDrop(final int counterOffset, final int maxCounterIndex,
      final KeyVal<K, V>[] collisions, final KeyVal<K, V> entry) {
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = MAX_COUNT;
    do {
      int count = counters.getOpaque(counterIndex);
      if (count == 0) {
        COLLISIONS.setOpaque(collisions, counterIndex - counterOffset, entry);
        counters.initializeOpaque(counterIndex);
        while (++counterIndex < maxCounterIndex) {
          count = counters.getOpaque(counterIndex);
          if (count > 0) {
            counters.setOpaque(counterIndex, count >> 1);
            continue;
          }
          if (size.getAndDecrement() <= capacity) {
            size.getAndIncrement();
            continue;
          }
          for (int collisionIndex = counterIndex - counterOffset,
              nextCollisionIndex = collisionIndex + 1; ; ++collisionIndex, ++nextCollisionIndex) {
            if (nextCollisionIndex == collisions.length) {
              COLLISIONS.setOpaque(collisions, collisionIndex, null);
              return;
            }
            Object next = COLLISIONS.getOpaque(collisions, nextCollisionIndex);
            if (next == null) {
              COLLISIONS.setOpaque(collisions, collisionIndex, null);
              next = COLLISIONS.getOpaque(collisions, nextCollisionIndex);
              if (next == null
                  || COLLISIONS.compareAndExchange(collisions, collisionIndex, null, next)
                  != null) {
                return;
              }
            } else {
              COLLISIONS.setOpaque(collisions, collisionIndex, next);
            }
            // Counter misses may occur during this transition.
            count = counters.getOpaque(++counterIndex);
            counters.setOpaque(counterIndex - 1, count >> 1);
          }
        }
        return;
      }
      // Counter misses may occur between these two calls.
      counters.setOpaque(counterIndex, count >> 1);
      if (count < minCount) {
        minCount = count;
        minCounterIndex = counterIndex;
      }
    } while (++counterIndex < maxCounterIndex);
    COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, entry);
    counters.initializeOpaque(minCounterIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public V putReplace(final K key, final V val) {
    final int hash = hashCoder.applyAsInt(key) & mask;
    final KeyVal<K, V>[] collisions = getBucket.apply(hash);
    int index = 0;
    KeyVal<K, V> entry = null;
    do {
      KeyVal<K, V> collision = (KeyVal<K, V>) COLLISIONS.getOpaque(collisions, index);
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
          entry = new KeyVal<>(key, val);
        }
        do {
          collision = (KeyVal<K, V>) COLLISIONS.compareAndExchange(collisions, index, null, entry);
          if (collision == null) {
            counters.initializeOpaque((hash << maxCollisionsShift) + index);
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
          entry = new KeyVal<>(key, val);
        }
        final KeyVal<K, V> witness = (KeyVal<K, V>) COLLISIONS
            .compareAndExchange(collisions, index, collision, entry);
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
    int minCount = MAX_COUNT;
    synchronized (collisions) {
      for (; ; ++counterIndex) {
        KeyVal<K, V> collision = (KeyVal<K, V>) COLLISIONS.getOpaque(collisions, index);
        if (collision == null) {  // Assume over capacity.
          if (entry == null) {
            entry = new KeyVal<>(key, val);
          }
          if (index == 0) {  // Strict capacity checked above.
            collision = (KeyVal<K, V>) COLLISIONS
                .compareAndExchange(collisions, index, null, entry);
            if (collision == null) {
              counters.initializeOpaque(counterIndex);
              size.getAndIncrement();
              return val;
            }
            if (key.equals(collision.key)) {
              return collision.val; // If another thread raced to PUT, let it win.
            }
            return val; // Don't cache, lost tie breaker.
          }
          COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, entry);
          counters.initializeOpaque(minCounterIndex);
          decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
          return val;
        }

        if (collision.val == val) {
          return val;
        }

        if (key.equals(collision.key)) {
          if (entry == null) {
            entry = new KeyVal<>(key, val);
          }
          final KeyVal<K, V> witness = (KeyVal<K, V>) COLLISIONS
              .compareAndExchange(collisions, index, collision, entry);
          if (witness == collision) {
            return val;
          }
          if (key.equals(witness.key)) {
            return witness.val; // If another thread raced to PUT, let it win.
          }
        }

        int count = counters.getOpaque(counterIndex);
        if (count < minCount) {
          minCount = count;
          minCounterIndex = counterIndex;
        }

        if (++index == collisions.length) {
          if (entry == null) {
            entry = new KeyVal<>(key, val);
          }
          COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, entry);
          counters.initializeOpaque(minCounterIndex);
          if (size.get() > capacity) {
            decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
            return val;
          }
          counters.decay(counterOffset, counterIndex, minCounterIndex);
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
    final KeyVal<K, V>[] collisions = getBucket.apply(hash);
    int index = 0;
    KeyVal<K, V> entry = null;
    do {
      KeyVal<K, V> collision = (KeyVal<K, V>) COLLISIONS.getOpaque(collisions, index);
      if (collision == null) {
        if (index == 0) {
          // If not strict, allow first entry into first collision index.
          if (strict && size.get() > capacity) { // Nothing to swap with and over capacity.
            return val;
          }
        } else if (size.get() > capacity) {
          break;
        }
        entry = new KeyVal<>(key, val);
        do {
          collision = (KeyVal<K, V>) COLLISIONS
              .compareAndExchange(collisions, index, null, entry);
          if (collision == null) {
            counters.initializeOpaque((hash << maxCollisionsShift) + index);
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
    int minCount = MAX_COUNT;
    synchronized (collisions) {
      for (; ; ) {
        KeyVal<K, V> collision = (KeyVal<K, V>) COLLISIONS.getOpaque(collisions, index);
        if (collision == null) {  // Assume over capacity.
          if (entry == null) {
            entry = new KeyVal<>(key, val);
          }
          if (index == 0) {  // Strict capacity checked above.
            collision = (KeyVal<K, V>) COLLISIONS
                .compareAndExchange(collisions, index, null, entry);
            if (collision == null) {
              counters.initializeOpaque(counterIndex);
              size.getAndIncrement();
              return val;
            }
            if (key.equals(collision.key)) {
              return collision.val;
            }
            return val; // Don't cache, lost tie breaker.
          }
          COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, entry);
          counters.initializeOpaque(minCounterIndex);
          decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
          return val;
        }

        if (key.equals(collision.key)) {
          return collision.val;
        }

        int count = counters.getOpaque(counterIndex);
        if (count < minCount) {
          minCount = count;
          minCounterIndex = counterIndex;
        }
        ++counterIndex;
        if (++index == collisions.length) {
          if (entry == null) {
            entry = new KeyVal<>(key, val);
          }
          COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, entry);
          counters.initializeOpaque(minCounterIndex);
          if (size.get() > capacity) {
            decayAndDrop(counterOffset, counterIndex, minCounterIndex, collisions);
            return val;
          }
          counters.decay(counterOffset, counterIndex, minCounterIndex);
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
    final KeyVal<K, V>[] collisions = getBucket.apply(hash);
    int index = 0;
    do {
      KeyVal<K, V> collision = (KeyVal<K, V>) COLLISIONS.getOpaque(collisions, index);
      if (collision == null) {
        if (size.get() > capacity) {
          return null;
        }
        final KeyVal<K, V> entry = new KeyVal<>(key, val);
        do {
          collision = (KeyVal<K, V>) COLLISIONS
              .compareAndExchange(collisions, index, null, entry);
          if (collision == null) {
            counters.initializeOpaque((hash << maxCollisionsShift) + index);
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
    final KeyVal<K, V>[] collisions = getBucket.apply(hash);
    int index = 0;
    KeyVal<K, V> entry = null;
    do {
      KeyVal<K, V> collision = (KeyVal<K, V>) COLLISIONS.getOpaque(collisions, index);
      if (collision == null) {
        if (size.get() > capacity) {
          return null;
        }
        if (entry == null) {
          entry = new KeyVal<>(key, val);
        }
        do {
          collision = (KeyVal<K, V>) COLLISIONS
              .compareAndExchange(collisions, index, null, entry);
          if (collision == null) {
            counters.initializeOpaque((hash << maxCollisionsShift) + index);
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
          entry = new KeyVal<>(key, val);
        }
        final KeyVal<K, V> witness = (KeyVal<K, V>) COLLISIONS
            .compareAndExchange(collisions, index, collision, entry);
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
  @SuppressWarnings("unchecked")
  public boolean remove(final K key) {
    final int hash = hashCoder.applyAsInt(key) & mask;
    final KeyVal<K, V>[] collisions = getBucket.apply(hash);
    synchronized (collisions) {
      int index = 0;
      do {
        KeyVal<K, V> collision = (KeyVal<K, V>) COLLISIONS.getOpaque(collisions, index);
        if (collision == null) {
          return false;
        }
        if (key.equals(collision.key)) {
          size.getAndDecrement();
          final int counterOffset = hash << maxCollisionsShift;
          int counterIndex = counterOffset + index;
          for (int nextIndex = index + 1; ; ++index, ++nextIndex) {
            if (nextIndex == collisions.length) {
              COLLISIONS.setOpaque(collisions, index, null);
              return true;
            }
            Object next = COLLISIONS.getOpaque(collisions, nextIndex);
            if (next == null) {
              COLLISIONS.setOpaque(collisions, index, null);
              next = COLLISIONS.getOpaque(collisions, nextIndex);
              if (next == null
                  || COLLISIONS.compareAndExchange(collisions, index, null, next) != null) {
                return true;
              }
            } else {
              COLLISIONS.setOpaque(collisions, index, next);
            }
            // Counter misses may occur during this transition.
            final int count = counters.getOpaque(++counterIndex);
            counters.setOpaque(counterIndex - 1, count >> 1);
          }
        }
      } while (++index < collisions.length);
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public void clear() {
    synchronized (hashTable) {
      IntStream.range(0, hashTable.length).parallel().forEach(i -> {
        final KeyVal<K, V>[] collisions = (KeyVal<K, V>[]) COLLISIONS.getOpaque(hashTable, i);
        if (collisions == null) {
          return;
        }
        int index = 0;
        do {
          final Object collision = COLLISIONS.getAndSet(collisions, index, null);
          if (collision != null) {
            size.getAndDecrement();
          }
        } while (++index < collisions.length);
      });
    }
  }

  @Override
  public String toString() {
    return "SparseEntryCollisionCache{capacity=" + capacity
        + ", strictCapacity=" + strict
        + ", size=" + size.get()
        + ", " + super.toString() + '}';
  }
}
