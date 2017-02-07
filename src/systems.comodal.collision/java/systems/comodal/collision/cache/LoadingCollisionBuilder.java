package systems.comodal.collision.cache;

import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.ToIntFunction;

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
    return buildSparse(CollisionBuilder.DEFAULT_SPARSE_FACTOR);
  }

  /**
   * @param sparseFactor Used to expand the size of the backing hash table to reduce collisions.
   * Defaults to 3.0 and has a minimum of 1.0.
   * @return A newly built {@link LoadingCollisionCache LoadingCollisionCache}.
   */
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

  /**
   * The computed hash code is used to index the backing hash table of the cache.  Hash tables are
   * always a length of some power of two.  The hash code will be masked against
   * (hashTable.length - 1) to prevent index out of bounds exceptions.
   *
   * @param hashCoder computes an integer hash code for a given key.
   * @return {@link LoadingCollisionBuilder KeyedCollisionBuilder} to continue building process.
   */
  public LoadingCollisionBuilder<K, L, V> setHashCoder(final ToIntFunction<K> hashCoder) {
    delegate.setHashCoder(hashCoder);
    return this;
  }

  public BiPredicate<K, V> getIsValForKey() {
    return delegate.getIsValForKey();
  }

  /**
   * Keys will not be stored if this predicate is provided.  This is the primary motivation of
   * Collision.  The idea is allow for more cache capacity by not storing keys.
   *
   * @param isValForKey tests if a given value corresponds to the given key.
   * @return {@link LoadingCollisionBuilder LoadingCollisionBuilder} to continue building process.
   */
  public LoadingCollisionBuilder<K, L, V> setIsValForKey(final BiPredicate<K, V> isValForKey) {
    delegate.setIsValForKey(isValForKey);
    return this;
  }

  public boolean isStrictCapacity() {
    return delegate.isStrictCapacity();
  }

  public LoadingCollisionBuilder<K, L, V> setStrictCapacity(final boolean strictCapacity) {
    delegate.setStrictCapacity(strictCapacity);
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

  public boolean isStoreKeys() {
    return delegate.isStoreKeys();
  }

  public LoadingCollisionBuilder<K, L, V> setStoreKeys(final boolean storeKeys) {
    delegate.setStoreKeys(storeKeys);
    return this;
  }
}
