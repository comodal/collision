package com.fabahaba.collision.cache;

import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import static com.fabahaba.collision.cache.CollisionBuilder.DEFAULT_SPARSE_FACTOR;

public final class KeyedCollisionBuilder<K, V> {

  private final CollisionBuilder<V> delegate;
  private ToIntFunction<K> hashCoder;
  private BiPredicate<K, Object> isValForKey;

  KeyedCollisionBuilder(final CollisionBuilder<V> delegate, final ToIntFunction<K> hashCoder) {
    this(delegate, hashCoder, null);
  }

  KeyedCollisionBuilder(final CollisionBuilder<V> delegate,
      final BiPredicate<K, Object> isValForKey) {
    this(delegate, null, isValForKey);
  }

  KeyedCollisionBuilder(final CollisionBuilder<V> delegate) {
    this(delegate, null, null);
  }

  KeyedCollisionBuilder(final CollisionBuilder<V> delegate, final ToIntFunction<K> hashCoder,
      final BiPredicate<K, Object> isValForKey) {
    this.delegate = delegate;
    this.hashCoder = hashCoder;
    this.isValForKey = isValForKey;
  }

  static final class DefaultIsValForKey<K> implements BiPredicate<K, Object> {

    @Override
    public boolean test(final K key, final Object val) {
      return val.equals(key);
    }
  }

  static final class DefaultHashCoder<K> implements ToIntFunction<K> {

    /**
     * Taken from {@link java.util.concurrent.ConcurrentHashMap#spread
     * java.util.concurrent.ConcurrentHashMap}
     *
     * @see java.util.concurrent.ConcurrentHashMap#spread(int)
     */
    private static int spread(final int hash) {
      return hash ^ (hash >>> 16);
    }

    @Override
    public int applyAsInt(final K key) {
      return spread(key.hashCode());
    }
  }

  public CollisionCache<K, V> buildSparse() {
    return buildSparse(DEFAULT_SPARSE_FACTOR);
  }

  public CollisionCache<K, V> buildSparse(final double sparseFactor) {
    return buildSparse(sparseFactor, key -> null, null);
  }

  <L> LoadingCollisionCache<K, L, V> buildSparse(
      final double sparseFactor,
      final Function<K, L> loader,
      final BiFunction<K, L, V> mapper) {
    return delegate.buildSparse(sparseFactor, getHashCoder(), getIsValForKey(), loader, mapper);
  }

  public CollisionCache<K, V> buildPacked() {
    return buildPacked(key -> null, null);
  }

  <L> LoadingCollisionCache<K, L, V> buildPacked(
      final Function<K, L> loader,
      final BiFunction<K, L, V> mapper) {
    return delegate.buildPacked(getHashCoder(), getIsValForKey(), loader, mapper);
  }

  public int getCapacity() {
    return delegate.getCapacity();
  }

  public LoadingCollisionBuilder<K, V, V> setLoader(final Function<K, V> loader) {
    return setLoader(loader, (key, val) -> val);
  }

  public <L> LoadingCollisionBuilder<K, L, V> setLoader(final Function<K, L> loader,
      final BiFunction<K, L, V> finalizer) {
    return new LoadingCollisionBuilder<>(this, loader, finalizer);
  }

  public ToIntFunction<K> getHashCoder() {
    return hashCoder == null ? new DefaultHashCoder<>() : hashCoder;
  }

  public KeyedCollisionBuilder<K, V> setHashCoder(final ToIntFunction<K> hashCoder) {
    this.hashCoder = hashCoder;
    return this;
  }

  public BiPredicate<K, Object> getIsValForKey() {
    return isValForKey == null ? new DefaultIsValForKey<>() : isValForKey;
  }

  public KeyedCollisionBuilder<K, V> setIsValForKey(final BiPredicate<K, Object> isValForKey) {
    delegate.setStoreKeys(false);
    this.isValForKey = isValForKey;
    return this;
  }

  public boolean isStrictCapacity() {
    return delegate.isStrictCapacity();
  }

  public KeyedCollisionBuilder<K, V> setStrictCapacity(final boolean strictCapacity) {
    delegate.setStrictCapacity(strictCapacity);
    return this;
  }

  public Class<V> getValueType() {
    return delegate.getValueType();
  }

  public KeyedCollisionBuilder<K, V> setValueType(final Class<V> valueType) {
    delegate.setValueType(valueType);
    return this;
  }

  public int getBucketSize() {
    return delegate.getBucketSize();
  }

  public KeyedCollisionBuilder<K, V> setBucketSize(final int bucketSize) {
    delegate.setBucketSize(bucketSize);
    return this;
  }

  public int getInitCount() {
    return delegate.getInitCount();
  }

  public KeyedCollisionBuilder<K, V> setInitCount(final int initCount) {
    delegate.setInitCount(initCount);
    return this;
  }

  public int getMaxCounterVal() {
    return delegate.getMaxCounterVal();
  }

  public KeyedCollisionBuilder<K, V> setMaxCounterVal(final int maxCounterVal) {
    delegate.setMaxCounterVal(maxCounterVal);
    return this;
  }

  public boolean isLazyInitBuckets() {
    return delegate.isLazyInitBuckets();
  }

  public KeyedCollisionBuilder<K, V> setLazyInitBuckets(final boolean lazyInitBuckets) {
    delegate.setLazyInitBuckets(lazyInitBuckets);
    return this;
  }

  public boolean isStoreKeys() {
    return delegate.isStoreKeys();
  }

  public KeyedCollisionBuilder<K, V> setStoreKeys(final boolean storeKeys) {
    delegate.setStoreKeys(storeKeys);
    return this;
  }
}
