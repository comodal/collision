package com.fabahaba.collision.cache;

import java.lang.reflect.Array;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.ToIntFunction;

public final class CollisionBuilder<V> {

  private final int capacity;
  private Class<V> valueType;
  private int bucketSize = 0;
  private int initCount = 5;
  private int maxCounterVal = 1_048_576;
  private boolean lazyInitBuckets = false;
  private boolean storeKeys = true;

  CollisionBuilder(final int capacity) {
    this.capacity = capacity;
  }

  static final double DEFAULT_SPARSE_FACTOR = 2.0;

  public <K> CollisionCache<K, V> buildSparse() {
    return buildSparse(DEFAULT_SPARSE_FACTOR);
  }

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
      final BiPredicate<K, Object> isValForKey,
      final Function<K, L> loader,
      final BiFunction<K, L, V> mapper) {
    final int bucketSize = this.bucketSize > 0 ? this.bucketSize : 4;
    final int maxCollisions = Integer.highestOneBit(bucketSize - 1) << 1;
    final int maxCollisionsShift = Integer.numberOfTrailingZeros(maxCollisions);
    final byte[] counters = new byte[Integer
        .highestOneBit((int) (capacity * Math.max(1.0, sparseFactor)) - 1) << 1];
    final int pow2LogFactor = LogCounterCache.calcLogFactorShift(maxCounterVal);
    final int hashTableLength = counters.length >> maxCollisionsShift;
    if (isStoreKeys()) {
      final Map.Entry<K, V>[][] hashTable = createEntryHashTable(hashTableLength, maxCollisions);
      return new SparseEntryCollisionCache<>(
          capacity,
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
      final BiPredicate<K, Object> isValForKey,
      final Function<K, L> loader,
      final BiFunction<K, L, V> mapper) {
    final int bucketSize = this.bucketSize > 0 ? this.bucketSize : 8;
    final int maxCollisions = Integer.highestOneBit(bucketSize - 1) << 1;
    final int maxCollisionsShift = Integer.numberOfTrailingZeros(maxCollisions);
    final byte[] counters = new byte[Integer.highestOneBit(capacity - 1) << 1];
    final int pow2LogFactor = LogCounterCache.calcLogFactorShift(maxCounterVal);
    final int hashTableLength = counters.length >> maxCollisionsShift;
    if (isStoreKeys()) {
      final Map.Entry<K, V>[][] hashTable = createEntryHashTable(hashTableLength, maxCollisions);
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
  private <K, V> Map.Entry<K, V>[][] createEntryHashTable(
      final int hashTableLength,
      final int maxCollisions) {
    if (lazyInitBuckets) {
      final Class<?> valueArrayType = Array
          .newInstance(Map.Entry.class, 0).getClass();
      return (Map.Entry<K, V>[][]) Array.newInstance(valueArrayType, hashTableLength);
    }
    return (Map.Entry<K, V>[][]) Array
        .newInstance(Map.Entry.class, hashTableLength, maxCollisions);
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

  public <K> KeyedCollisionBuilder<K, V> setHashCoder(final ToIntFunction<K> hashCoder) {
    return new KeyedCollisionBuilder<>(this, hashCoder);
  }

  public <K> KeyedCollisionBuilder<K, V> setIsValForKey(final BiPredicate<K, Object> isValForKey) {
    return new KeyedCollisionBuilder<>(this, isValForKey);
  }

  public <K> LoadingCollisionBuilder<K, V, V> setLoader(final Function<K, V> loader) {
    return setLoader(loader, (key, val) -> val);
  }

  public <K, L> LoadingCollisionBuilder<K, L, V> setLoader(final Function<K, L> loader,
      final BiFunction<K, L, V> finalizer) {
    return new LoadingCollisionBuilder<>(new KeyedCollisionBuilder<>(this), loader, finalizer);
  }

  public int getCapacity() {
    return capacity;
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
    this.initCount = initCount;
    return this;
  }

  public int getMaxCounterVal() {
    return maxCounterVal;
  }

  public CollisionBuilder<V> setMaxCounterVal(final int maxCounterVal) {
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
