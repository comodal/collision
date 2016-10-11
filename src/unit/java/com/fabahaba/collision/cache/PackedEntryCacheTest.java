package com.fabahaba.collision.cache;

import org.junit.Before;

public final class PackedEntryCacheTest extends BaseEntryCacheTest {

  @Before
  public void before() {
    this.cache = CollisionCache
        .<String>withCapacity(64)
        .<Integer, String>setLoader(
            key -> hashInteger(key),
            (key, hash) -> hash)
        .buildPacked();
  }
}
