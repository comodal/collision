package com.fabahaba.collision.cache;

import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import static com.fabahaba.collision.cache.CollisionBuilder.DEFAULT_SPARSE_FACTOR;

public final class LoadingCollisionBuilder<K, L, V> {

  private final KeyedCollisionBuilder<K, V> delegate;
  private final Function<K, L> loader;
  private final BiFunction<K, L, V> mapper;

  LoadingCollisionBuilder(final KeyedCollisionBuilder<K, V> delegate, final Function<K, L> loader,
      final BiFunction<K, L, V> mapper) {
    this.delegate = delegate;
    this.loader = loader;
    this.mapper = mapper;
  }

  public LoadingCollisionCache<K, L, V> buildSparse() {
    return buildSparse(DEFAULT_SPARSE_FACTOR);
  }

  public LoadingCollisionCache<K, L, V> buildSparse(final double sparseFactor) {
    return delegate.buildSparse(sparseFactor, loader, mapper);
  }

  public LoadingCollisionCache<K, L, V> buildPacked() {
    return delegate.buildPacked(loader, mapper);
  }

  public int getCapacity() {
    return delegate.getCapacity();
  }

  public ToIntFunction<K> getHashCoder() {
    return delegate.getHashCoder();
  }

  public LoadingCollisionBuilder<K, L, V> setHashCoder(final ToIntFunction<K> hashCoder) {
    delegate.setHashCoder(hashCoder);
    return this;
  }

  public Class<V> getValueType() {
    return delegate.getValueType();
  }

  public LoadingCollisionBuilder<K, L, V> setValueType(final Class<V> valueType) {
    delegate.setValueType(valueType);
    return this;
  }

  public int getBucketSize() {
    return delegate.getBucketSize();
  }

  public LoadingCollisionBuilder<K, L, V> setBucketSize(final int bucketSize) {
    delegate.setBucketSize(bucketSize);
    return this;
  }

  public int getInitCount() {
    return delegate.getInitCount();
  }

  public LoadingCollisionBuilder<K, L, V> setInitCount(final int initCount) {
    delegate.setInitCount(initCount);
    return this;
  }

  public int getMaxCounterVal() {
    return delegate.getMaxCounterVal();
  }

  public LoadingCollisionBuilder<K, L, V> setMaxCounterVal(final int maxCounterVal) {
    delegate.setMaxCounterVal(maxCounterVal);
    return this;
  }

  public boolean isLazyInitBuckets() {
    return delegate.isLazyInitBuckets();
  }

  public LoadingCollisionBuilder<K, L, V> setLazyInitBuckets(final boolean lazyInitBuckets) {
    delegate.setLazyInitBuckets(lazyInitBuckets);
    return this;
  }

  public BiPredicate<K, Object> getIsValForKey() {
    return delegate.getIsValForKey();
  }

  public LoadingCollisionBuilder<K, L, V> setIsValForKey(final BiPredicate<K, Object> isValForKey) {
    delegate.setIsValForKey(isValForKey);
    return this;
  }

  public boolean isStoreKeys() {
    return delegate.isStoreKeys();
  }

  public LoadingCollisionBuilder<K, L, V> setStoreKeys(final boolean storeKeys) {
    delegate.setStoreKeys(storeKeys);
    return this;
  }
}
