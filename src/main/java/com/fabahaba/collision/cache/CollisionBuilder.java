package com.fabahaba.collision.cache;

import java.lang.reflect.Array;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.ToIntFunction;

public final class CollisionBuilder<V> {

  static final int DEFAULT_SPARSE_BUCKET_SIZE = 4;
  static final int DEFAULT_PACKED_BUCKET_SIZE = 8;

  private final int capacity;
  private boolean strictCapacity = false;
  private Class<V> valueType;
  private int bucketSize = 0;
  private int initCount = 5;
  private int maxCounterVal = 1_048_576;
  private boolean lazyInitBuckets = false;
  private boolean storeKeys = true;

  CollisionBuilder(final int capacity) {
    this.capacity = capacity;
  }

  /**
   * Multiplied by the desired capacity to determine the hash table length.
   * Increase to reduce collisions.
   * If increasing consider lazyInitBuckets to prevent unnecessary array creation.
   */
  static final double DEFAULT_SPARSE_FACTOR = 3.0;

  public <K> CollisionCache<K, V> buildSparse() {
    return buildSparse(DEFAULT_SPARSE_FACTOR);
  }

  /**
   * @param sparseFactor Used to expand the size of the backing hash table to reduce collisions.
   *                     Defaults to 3.0 and has a minimum of 1.0.
   * @return A newly built {@link CollisionCache CollisionCache}.
   */
  public <K> CollisionCache<K, V> buildSparse(final double sparseFactor) {
    return buildSparse(
        sparseFactor,
        new KeyedCollisionBuilder.DefaultHashCoder<>(),
        new KeyedCollisionBuilder.DefaultIsValForKey<>(),
        key -> null,
        null);
  }

  <K, L> LoadingCollisionCache<K, L, V> buildSparse(
      final double sparseFactor,
      final ToIntFunction<K> hashCoder,
      final BiPredicate<K, V> isValForKey,
      final Function<K, L> loader,
      final BiFunction<K, L, V> mapper) {
    final int bucketSize = this.bucketSize > 0 ? this.bucketSize : DEFAULT_SPARSE_BUCKET_SIZE;
    final int maxCollisions = Integer.highestOneBit(bucketSize - 1) << 1;
    final int maxCollisionsShift = Integer.numberOfTrailingZeros(maxCollisions);
    final byte[] counters = new byte[Integer
        .highestOneBit((int) (capacity * Math.max(1.0, sparseFactor)) - 1) << 1];
    final int pow2LogFactor = AtomicLogCounters.calcLogFactorShift(maxCounterVal);
    final int hashTableLength = counters.length >> maxCollisionsShift;
    if (isStoreKeys()) {
      final KeyVal<K, V>[][] hashTable = createEntryHashTable(hashTableLength, maxCollisions);
      return new SparseEntryCollisionCache<>(
          capacity,
          strictCapacity,
          maxCollisionsShift,
          counters,
          initCount,
          pow2LogFactor,
          hashTable,
          hashCoder, loader, mapper);
    }
    final V[][] hashTable = createHashTable(hashTableLength, maxCollisions);
    return new SparseCollisionCache<>(
        capacity,
        strictCapacity,
        valueType,
        maxCollisionsShift,
        counters,
        initCount,
        pow2LogFactor,
        hashTable,
        hashCoder, isValForKey, loader, mapper);
  }

  public <K> CollisionCache<K, V> buildPacked() {
    return buildPacked(
        new KeyedCollisionBuilder.DefaultHashCoder<>(),
        new KeyedCollisionBuilder.DefaultIsValForKey<>(),
        key -> null,
        null);
  }

  <K, L> LoadingCollisionCache<K, L, V> buildPacked(
      final ToIntFunction<K> hashCoder,
      final BiPredicate<K, V> isValForKey,
      final Function<K, L> loader,
      final BiFunction<K, L, V> mapper) {
    final int bucketSize = this.bucketSize > 0 ? this.bucketSize : DEFAULT_PACKED_BUCKET_SIZE;
    final int maxCollisions = Integer.highestOneBit(bucketSize - 1) << 1;
    final int maxCollisionsShift = Integer.numberOfTrailingZeros(maxCollisions);
    final byte[] counters = new byte[Integer.highestOneBit(capacity - 1) << 1];
    final int pow2LogFactor = AtomicLogCounters.calcLogFactorShift(maxCounterVal);
    final int hashTableLength = counters.length >> maxCollisionsShift;
    if (isStoreKeys()) {
      final KeyVal<K, V>[][] hashTable = createEntryHashTable(hashTableLength, maxCollisions);
      return new PackedEntryCollisionCache<>(
          maxCollisionsShift,
          counters,
          initCount,
          pow2LogFactor,
          hashTable,
          hashCoder, loader, mapper);
    }
    final V[][] hashTable = createHashTable(hashTableLength, maxCollisions);
    return new PackedCollisionCache<>(
        valueType,
        maxCollisionsShift,
        counters,
        initCount,
        pow2LogFactor,
        hashTable,
        hashCoder, isValForKey, loader, mapper);
  }

  @SuppressWarnings("unchecked")
  private <K, V> KeyVal<K, V>[][] createEntryHashTable(
      final int hashTableLength,
      final int maxCollisions) {
    if (lazyInitBuckets) {
      final Class<?> valueArrayType = Array
          .newInstance(KeyVal.class, 0).getClass();
      return (KeyVal<K, V>[][]) Array.newInstance(valueArrayType, hashTableLength);
    }
    return (KeyVal<K, V>[][]) Array
        .newInstance(KeyVal.class, hashTableLength, maxCollisions);
  }

  @SuppressWarnings("unchecked")
  private V[][] createHashTable(final int hashTableLength, final int maxCollisions) {
    if (valueType == null) {
      throw new IllegalStateException("valueType needed.");
    }

    if (lazyInitBuckets) {
      final Class<?> valueArrayType = Array.newInstance(valueType, 0).getClass();
      return (V[][]) Array.newInstance(valueArrayType, hashTableLength);
    }
    return (V[][]) Array.newInstance(valueType, hashTableLength, maxCollisions);
  }

  /**
   * The computed hash code is used to index the backing hash table of the cache.  Hash tables are
   * always a length of some power of two.  The hash code will be masked against
   * (hashTable.length - 1) to prevent index out of bounds exceptions.
   *
   * @param hashCoder computes an integer hash code for a given key.
   * @return {@link KeyedCollisionBuilder KeyedCollisionBuilder} to continue building process.
   */
  public <K> KeyedCollisionBuilder<K, V> setHashCoder(final ToIntFunction<K> hashCoder) {
    return new KeyedCollisionBuilder<>(this, hashCoder);
  }

  /**
   * Keys will not be stored if this predicate is provided.  This is the primary motivation of
   * Collision.  The idea is allow for more cache capacity by not storing keys.
   *
   * @param isValForKey tests if a given value corresponds to the given key.
   * @return {@link KeyedCollisionBuilder KeyedCollisionBuilder} to continue building process.
   */
  public <K> KeyedCollisionBuilder<K, V> setIsValForKey(final BiPredicate<K, V> isValForKey) {
    return new KeyedCollisionBuilder<>(this, isValForKey);
  }

  /**
   * Set the loader used to initialize values if missing from the cache.  The loader may return null
   * values, the cache will simply return null as well.  The cache will provide methods to use the
   * loader either atomically or not.
   *
   * @param loader returns values for a given key.
   * @return {@link LoadingCollisionBuilder LoadingCollisionBuilder} to continue building process.
   */
  public <K> LoadingCollisionBuilder<K, V, V> setLoader(final Function<K, V> loader) {
    return setLoader(loader, (key, val) -> val);
  }

  /**
   * Set the loader and mapper used to initialize values if missing from the cache.  The loader may
   * return null values, the cache will simply return null as well.  The cache will provide methods
   * to use the loader either atomically or not.  The mapper is separated out to delay any final
   * processing/parsing until it is absolutely needed.  The mapper will never be passed a null value
   * and must not return a null value; cache performance could severely degrade.
   *
   * @param loader returns values for a given key.
   * @param mapper map loaded types to value types.
   * @param <L>    The intermediate type between loading and mapping.
   * @return {@link LoadingCollisionBuilder LoadingCollisionBuilder} to continue building process.
   */
  public <K, L> LoadingCollisionBuilder<K, L, V> setLoader(final Function<K, L> loader,
      final BiFunction<K, L, V> mapper) {
    return new LoadingCollisionBuilder<>(new KeyedCollisionBuilder<>(this), loader, mapper);
  }

  public int getCapacity() {
    return capacity;
  }

  public boolean isStrictCapacity() {
    return strictCapacity;
  }

  public CollisionBuilder<V> setStrictCapacity(final boolean strictCapacity) {
    this.strictCapacity = strictCapacity;
    return this;
  }

  public Class<V> getValueType() {
    return valueType;
  }

  public CollisionBuilder<V> setValueType(final Class<V> valueType) {
    this.valueType = valueType;
    return this;
  }

  public int getBucketSize() {
    return bucketSize;
  }

  public CollisionBuilder<V> setBucketSize(final int bucketSize) {
    this.bucketSize = bucketSize;
    return this;
  }

  public int getInitCount() {
    return initCount;
  }

  public CollisionBuilder<V> setInitCount(final int initCount) {
    if (initCount > 32) {
      throw new IllegalStateException("Setting a large initial counter count is pointless.");
    }
    if (initCount < 0) {
      throw new IllegalStateException("Initial counter count must be >= 0.");
    }
    this.initCount = initCount;
    return this;
  }

  public int getMaxCounterVal() {
    return maxCounterVal;
  }

  public CollisionBuilder<V> setMaxCounterVal(final int maxCounterVal) {
    if (maxCounterVal < 256) {
      throw new IllegalStateException("The maximum counter count should be large to increase the "
          + "likelihood of choosing the least frequently used entry for eviction.");
    }
    this.maxCounterVal = maxCounterVal;
    return this;
  }

  public boolean isLazyInitBuckets() {
    return lazyInitBuckets;
  }

  public CollisionBuilder<V> setLazyInitBuckets(final boolean lazyInitBuckets) {
    this.lazyInitBuckets = lazyInitBuckets;
    return this;
  }

  public boolean isStoreKeys() {
    return storeKeys;
  }

  public CollisionBuilder<V> setStoreKeys(final boolean storeKeys) {
    this.storeKeys = storeKeys;
    return this;
  }
}
