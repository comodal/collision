package systems.comodal.collision.cache;

import static systems.comodal.collision.cache.AtomicLogCounters.MAX_COUNT;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
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
abstract class BaseCollisionCache<K, L, V> implements LoadingCollisionCache<K, L, V> {

  static final VarHandle COLLISIONS = MethodHandles.arrayElementVarHandle(Object[].class);

  final int maxCollisionsShift;
  final V[][] hashTable;
  final int mask;
  final IntFunction<V[]> getBucket;
  final AtomicLogCounters counters;
  final ToIntFunction<K> hashCoder;
  final BiPredicate<K, V> isValForKey;
  private final Class<V> valueType;
  private final Function<K, L> loader;
  private final BiFunction<K, L, V> mapper;
  private final Function<K, V> loadAndMap;

  BaseCollisionCache(final Class<V> valueType,
      final int maxCollisionsShift,
      final V[][] hashTable,
      final IntFunction<V[]> getBucket,
      final AtomicLogCounters counters,
      final ToIntFunction<K> hashCoder,
      final BiPredicate<K, V> isValForKey,
      final Function<K, L> loader,
      final BiFunction<K, L, V> mapper) {
    this.valueType = valueType;
    this.maxCollisionsShift = maxCollisionsShift;
    this.hashTable = hashTable;
    this.mask = hashTable.length - 1;
    this.getBucket = getBucket;
    this.counters = counters;
    this.hashCoder = hashCoder;
    this.isValForKey = isValForKey;
    this.loader = loader;
    this.mapper = mapper;
    this.loadAndMap = key -> {
      final L loaded = loader.apply(key);
      return loaded == null ? null : mapper.apply(key, loaded);
    };
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final V getAggressive(final K key) {
    return getAggressive(key, loader, mapper);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final V getAggressive(final K key, final Function<K, L> loader) {
    return getAggressive(key, loader, mapper);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final V get(final K key) {
    return get(key, loadAndMap);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public final V get(final K key, final Function<K, V> loadAndMap) {
    final int hash = hashCoder.applyAsInt(key) & mask;
    final V[] collisions = getBucket.apply(hash);
    final int counterOffset = hash << maxCollisionsShift;
    for (int index = 0; ; ) {
      final V collision = (V) COLLISIONS.getOpaque(collisions, index);
      if (collision == null) {
        return checkDecayAndSwap(counterOffset, collisions, key, loadAndMap);
      }
      if (isValForKey.test(key, collision)) {
        counters.increment(counterOffset + index);
        return collision;
      }
      if (++index == collisions.length) {
        return checkDecayAndProbSwap(counterOffset, collisions, key, loadAndMap);
      }
    }
  }

  /**
   * Checks for an existing entry synchronized behind the current collision hash bucket using
   * opaque memory access semantics.  If an entry does not exist, a value is loaded and the
   * behavior will be in line with the method {@link #decayAndSwap decayAndSwap}
   *
   * @param counterOffset beginning counter array index corresponding to collision values.
   * @param collisions values sitting in a hash bucket.
   * @param key used for table hash and entry equality.
   * @param loadAndMap loads a new value to cache if missing.
   * @return a value for the corresponding key.
   */
  abstract V checkDecayAndSwap(final int counterOffset, final V[] collisions,
      final K key, final Function<K, V> loadAndMap);

  /**
   * Checks for an existing entry synchronized behind the current collision hash bucket using
   * opaque memory access semantics.  The minimum count for each entry is proactively tracked for
   * swapping.  If an entry does not exist, a value is loaded and the behavior will be in line with
   * the method {@link #decayAndSwap decayAndSwap}
   *
   * @param counterOffset beginning counter array index corresponding to collision values.
   * @param collisions values sitting in a hash bucket.
   * @param key used for table hash and entry equality.
   * @param loadAndMap loads a new value to cache if missing.
   * @return a value for the corresponding key.
   */
  abstract V checkDecayAndProbSwap(final int counterOffset, final V[] collisions,
      final K key, final Function<K, V> loadAndMap);

  /**
   * Divides all counters for values within a hash bucket (collisions), swaps the val for the
   * least frequently used, and sets its counter to an initial val.  Also evicts the tail entry if
   * its count is zero.
   *
   * @param counterOffset beginning counter array index corresponding to collision values.
   * @param maxCounterIndex Max counter index for known non null collision values.
   * @param collisions values sitting in a hash bucket.
   * @param val The value to put in place of the least frequently used value.
   */
  final void decayAndSwap(final int counterOffset, final int maxCounterIndex, final V[] collisions,
      final V val) {
    int counterIndex = counterOffset;
    int minCounterIndex = counterOffset;
    int minCount = MAX_COUNT;
    do {
      int count = counters.getOpaque(counterIndex);
      if (count == 0) {
        COLLISIONS.setOpaque(collisions, counterIndex - counterOffset, val);
        counters.initializeOpaque(counterIndex);
        while (++counterIndex < maxCounterIndex) {
          count = counters.getOpaque(counterIndex);
          if (count == 0) {
            continue;
          }
          counters.setOpaque(counterIndex, count >> 1);
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
    COLLISIONS.setOpaque(collisions, minCounterIndex - counterOffset, val);
    counters.initializeOpaque(counterIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public final V getIfPresent(final K key) {
    final int hash = hashCoder.applyAsInt(key) & mask;
    final V[] collisions = getBucket.apply(hash);
    int index = 0;
    do {
      final V val = (V) COLLISIONS.getOpaque(collisions, index);
      if (val == null) {
        return null;
      }
      if (isValForKey.test(key, val)) {
        counters.increment((hash << maxCollisionsShift) + index);
        return val;
      }
    } while (++index < collisions.length);
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public final V replace(final K key, final V val) {
    if (val == null) {
      throw new NullPointerException("Cannot cache a null val.");
    }
    final V[] collisions = getBucket.apply(hashCoder.applyAsInt(key) & mask);
    int index = 0;
    do {
      final V collision = (V) COLLISIONS.getOpaque(collisions, index);
      if (collision == null) {
        return null;
      }
      if (collision == val) {
        return val;
      }
      if (isValForKey.test(key, collision)) {
        final V witness = (V) COLLISIONS.compareAndExchange(collisions, index, collision, val);
        if (witness == collision) {
          return val;
        }
        if (isValForKey.test(key, witness)) {
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
  public void clear() {
    IntStream.range(0, hashTable.length)
        .parallel()
        .forEach(i -> {
          final V[] collisions = hashTable[i];
          if (collisions == null) {
            return;
          }
          int index = 0;
          do {
            COLLISIONS.setOpaque(collisions, index++, null);
          } while (index < collisions.length);
        });
  }

  @Override
  public String toString() {
    return "CollisionCache{valueType=" + valueType
        + ", maxCollisions=" + (1 << maxCollisionsShift)
        + ", hashTableLength=" + hashTable.length
        + ", counters=" + counters + '}';
  }
}
