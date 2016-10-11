package com.fabahaba.collision.cache;

import org.junit.Before;

public final class PackedCacheTest extends BaseCacheTest {

  @Before
  public void before() {
    this.maxCollisions = 8;
    this.cache = CollisionCache
        .withCapacity(32, TestNumber.class)
        .setBucketSize(maxCollisions)
        .setStoreKeys(false)
        .<TestNumber, TestNumber>setLoader(num -> num, (key, num) -> num)
        .buildPacked();
  }
}
