package com.fabahaba.collision.cache;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToIntFunction;

final class PackedEntryCollisionCache<K, L, V> extends BaseEntryCollisionCache<K, L, V> {

  PackedEntryCollisionCache(final int maxCollisionsShift,
      final byte[] counters, final int initCount, final int pow2LogFactor,
      final Map.Entry<K, V>[][] hashTable, final ToIntFunction<K> hashCoder,
      final Function<K, L> loader, final BiFunction<K, L, V> mapper) {
    super(maxCollisionsShift, counters, initCount, pow2LogFactor, hashTable, hashCoder,
        loader, mapper);
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
        final Map.Entry<K, V> entry = Map.entry(key, mapper.apply(key, loaded));
        do {
          final Map.Entry<K, V> witness = (Map.Entry<K, V>) OA
              .compareAndExchangeRelease(collisions, index, null, entry);
          if (witness == null) {
            BA.setRelease(counters, counterOffset + index, initCount);
            return entry.getValue();
          }
          if (key.equals(witness.getKey())) {
            atomicIncrement(counterOffset + index);
            return witness.getValue();
          }
        } while (++index < collisions.length);
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
  private <I> V checkDecayAndSwapLFU(final int counterOffset, final Object[] collisions,
      final K key, final I loaded, final BiFunction<K, I, V> mapper) {
    int index = 0;
    synchronized (collisions) {
      do { // Double-checked locked volatile before swapping LFU to prevent duplicates.
        final Map.Entry<K, V> collision = (Map.Entry<K, V>) OA.getVolatile(collisions, index);
        if (key.equals(collision.getKey())) {
          atomicIncrement(counterOffset + index);
          return collision.getValue();
        }
      } while (++index < collisions.length);
      final V val = mapper.apply(key, loaded);
      if (val == null) {
        return null;
      }
      decayAndSwapLFU(counterOffset, collisions, Map.entry(key, val));
      return val;
    }
  }

  @SuppressWarnings("unchecked")
  private V checkDecayAndSwapLFU(final int counterOffset, final Object[] collisions,
      final Map.Entry<K, V> entry) {
    int index = 0;
    synchronized (collisions) {
      do { // Double-checked locked volatile before swapping LFU to prevent duplicates.
        final Map.Entry<K, V> collision = (Map.Entry<K, V>) OA.getVolatile(collisions, index);
        if (entry.getKey().equals(collision.getKey())) {
          atomicIncrement(counterOffset + index);
          return collision.getValue();
        }
      } while (++index < collisions.length);
      decayAndSwapLFU(counterOffset, collisions, entry);
    }
    return entry.getValue();
  }

  /**
   * Divides all counters for values within a hash bucket (collisions), swaps the value for the
   * least frequently used, and sets its counter to an initial value.
   *
   * @param counterOffset beginning counter array index corresponding to collision values.
   * @param collisions    values sitting in a hash bucket.
   * @param entry         The value to put in place of the least frequently used value.
   */
  private void decayAndSwapLFU(final int counterOffset, final Object[] collisions,
      final Map.Entry<K, V> entry) {
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = 0xff;
    final int max = counterOffset + collisions.length;
    do {
      int count = ((int) BA.getVolatile(counters, counterIndex)) & 0xff;
      if (count == 0) {
        OA.setRelease(collisions, counterIndex - counterOffset, entry);
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
    OA.setRelease(collisions, minCounterIndex - counterOffset, entry);
    BA.setRelease(counters, minCounterIndex, initCount);
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public V putReplace(final K key, final V val) {
    final int hash = hashCoder.applyAsInt(key) & mask;
    final Map.Entry<K, V>[] collisions = getCreateCollisions(hash);
    int index = 0;
    do {
      Map.Entry<K, V> collision = (Map.Entry<K, V>) OA.getVolatile(collisions, index);
      if (collision == null) {
        collision = (Map.Entry<K, V>) OA
            .compareAndExchangeRelease(collisions, index, null, Map.entry(key, val));
        if (collision == null) {
          BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
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
        final Map.Entry<K, V> witness = (Map.Entry<K, V>) OA
            .compareAndExchangeRelease(collisions, index, collision, Map.entry(key, val));
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
      do { // Double-checked locked volatile before swapping LFU to prevent duplicates.
        final Map.Entry<K, V> collision = (Map.Entry<K, V>) OA.getVolatile(collisions, index);
        if (collision.getValue() == val) {
          return val;
        }
        if (key.equals(collision.getKey())) {
          final Map.Entry<K, V> witness = (Map.Entry<K, V>) OA
              .compareAndExchangeRelease(collisions, index, collision, Map.entry(key, val));
          if (witness == collision) {
            return val;
          }
          // If another thread raced to PUT, let it win.
          if (key.equals(witness.getKey())) {
            return witness.getValue();
          }
        }
      } while (++index < collisions.length);
      decayAndSwapLFU(hash << maxCollisionsShift, collisions, Map.entry(key, val));
    }
    return val;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public V putIfAbsent(final K key, final V val) {
    final int hash = hashCoder.applyAsInt(key) & mask;
    final Map.Entry<K, V>[] collisions = getCreateCollisions(hash);
    int index = 0;
    do {
      final Map.Entry<K, V> collision = collisions[index];
      if (collision == null) {
        final Map.Entry<K, V> witness = (Map.Entry<K, V>) OA
            .compareAndExchangeRelease(collisions, index, null, Map.entry(key, val));
        if (witness == null) {
          BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
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
      do { // Double-checked locked volatile before swapping LFU to prevent duplicates.
        final Map.Entry<K, V> collision = (Map.Entry<K, V>) OA.getVolatile(collisions, index);
        if (key.equals(collision.getKey())) {
          return collision.getValue();
        }
      } while (++index < collisions.length);
      decayAndSwapLFU(hash << maxCollisionsShift, collisions, Map.entry(key, val));
    }
    return val;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public V putIfSpaceAbsent(final K key, final V val) {
    final int hash = hashCoder.applyAsInt(key) & mask;
    final Map.Entry<K, V>[] collisions = getCreateCollisions(hash);
    int index = 0;
    do {
      final Map.Entry<K, V> collision = collisions[index];
      if (collision == null) {
        final Map.Entry<K, V> witness = (Map.Entry<K, V>) OA
            .compareAndExchangeRelease(collisions, index, null, Map.entry(key, val));
        if (witness == null) {
          BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
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
  @SuppressWarnings("unchecked")
  @Override
  public V putIfSpaceReplace(final K key, final V val) {
    final int hash = hashCoder.applyAsInt(key) & mask;
    final Map.Entry<K, V>[] collisions = getCreateCollisions(hash);
    int index = 0;
    do {
      Map.Entry<K, V> collision = (Map.Entry<K, V>) OA.getVolatile(collisions, index);
      if (collision == null) {
        collision = (Map.Entry<K, V>) OA
            .compareAndExchangeRelease(collisions, index, null, Map.entry(key, val));
        if (collision == null) {
          BA.setRelease(counters, (hash << maxCollisionsShift) + index, initCount);
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
        final Map.Entry<K, V> witness = (Map.Entry<K, V>) OA
            .compareAndExchangeRelease(collisions, index, collision, Map.entry(key, val));
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
    return "PackedEntryCollisionCache{" + super.toString() + '}';
  }
}
