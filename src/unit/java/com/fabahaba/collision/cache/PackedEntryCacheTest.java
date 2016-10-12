package com.fabahaba.collision.cache;

import org.junit.Before;

public final class PackedEntryCacheTest extends BaseEntryCacheTest {

  @Before
  public void before() {
    this.cache = CollisionCache
        .<String>withCapacity(64)
        .setLoader(BaseEntryCacheTest::hashInteger, (key, hash) -> toHexString(hash))
        .buildPacked();
  }
}
