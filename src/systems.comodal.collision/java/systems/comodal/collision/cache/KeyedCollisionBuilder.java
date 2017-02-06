package systems.comodal.collision.cache;

import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.ToIntFunction;

public final class KeyedCollisionBuilder<K, V> {

  private final CollisionBuilder<V> delegate;
  private ToIntFunction<K> hashCoder;
  private BiPredicate<K, V> isValForKey;

  KeyedCollisionBuilder(final CollisionBuilder<V> delegate, final ToIntFunction<K> hashCoder) {
    this(delegate, hashCoder, null);
  }

  KeyedCollisionBuilder(final CollisionBuilder<V> delegate,
      final BiPredicate<K, V> isValForKey) {
    this(delegate, null, isValForKey);
  }

  KeyedCollisionBuilder(final CollisionBuilder<V> delegate) {
    this(delegate, null, null);
  }

  KeyedCollisionBuilder(final CollisionBuilder<V> delegate, final ToIntFunction<K> hashCoder,
      final BiPredicate<K, V> isValForKey) {
    this.delegate = delegate;
    this.hashCoder = hashCoder;
    this.isValForKey = isValForKey;
    if (isValForKey != null) {
      delegate.setStoreKeys(false);
    }
  }

  static final class DefaultIsValForKey<K, V> implements BiPredicate<K, V> {

    @Override
    public boolean test(final K key, final V val) {
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
    return buildSparse(CollisionBuilder.DEFAULT_SPARSE_FACTOR);
  }

  /**
   * @param sparseFactor Used to expand the size of the backing hash table to reduce collisions.
   *                     Defaults to 3.0 and has a minimum of 1.0.
   * @return A newly built {@link CollisionCache CollisionCache}.
   */
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

  /**
   * Set the loader used to initialize values if missing from the cache.  The loader may return null
   * values, the cache will simply return null as well.  The cache will provide methods to use the
   * loader either atomically or not.
   *
   * @param loader returns values for a given key.
   * @return {@link LoadingCollisionBuilder LoadingCollisionBuilder} to continue building process.
   */
  public LoadingCollisionBuilder<K, V, V> setLoader(final Function<K, V> loader) {
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
  public <L> LoadingCollisionBuilder<K, L, V> setLoader(final Function<K, L> loader,
      final BiFunction<K, L, V> mapper) {
    return new LoadingCollisionBuilder<>(this, loader, mapper);
  }

  public ToIntFunction<K> getHashCoder() {
    return hashCoder == null ? new DefaultHashCoder<>() : hashCoder;
  }

  /**
   * The computed hash code is used to index the backing hash table of the cache.  Hash tables are
   * always a length of some power of two.  The hash code will be masked against
   * (hashTable.length - 1) to prevent index out of bounds exceptions.
   *
   * @param hashCoder computes an integer hash code for a given key.
   * @return {@link KeyedCollisionBuilder KeyedCollisionBuilder} to continue building process.
   */
  public KeyedCollisionBuilder<K, V> setHashCoder(final ToIntFunction<K> hashCoder) {
    this.hashCoder = hashCoder;
    return this;
  }

  public BiPredicate<K, V> getIsValForKey() {
    return isValForKey == null ? new DefaultIsValForKey<>() : isValForKey;
  }

  /**
   * Keys will not be stored if this predicate is provided.  This is the primary motivation of
   * Collision.  The idea is allow for more cache capacity by not storing keys.
   *
   * @param isValForKey tests if a given value corresponds to the given key.
   * @return {@link KeyedCollisionBuilder KeyedCollisionBuilder} to continue building process.
   */
  public KeyedCollisionBuilder<K, V> setIsValForKey(final BiPredicate<K, V> isValForKey) {
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
