package com.fabahaba.collision.cache;

import org.junit.Before;

public final class PackedCollisionCacheTest extends BaseCollisionCacheTest {

  @Before
  public void before() {
    this.maxCollisions = 8;
    this.cache = CollisionCache
        .withCapacity(32, Integer.class)
        .setBucketSize(maxCollisions)
        .setStoreKeys(false)
        .<Integer, Integer>setLoader(num -> num, (key, num) -> num)
        .buildPacked();
  }
}
