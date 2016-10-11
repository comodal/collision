package com.fabahaba.collision.cache;

import org.junit.Before;

public final class SparseCollisionCacheTest extends BaseCollisionCacheTest {

  @Before
  public void before() {
    this.maxCollisions = 4;
    this.cache = CollisionCache
        .withCapacity(32, Integer.class)
        .setBucketSize(maxCollisions)
        .setStoreKeys(false)
        .<Integer, Integer>setLoader(num -> num, (key, num) -> num)
        .buildSparse();
  }
}
