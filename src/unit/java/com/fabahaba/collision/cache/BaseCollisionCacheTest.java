package com.fabahaba.collision.cache;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

abstract class BaseCollisionCacheTest {

  private static final int NUM_KEYS_TO_TEST = 10;

  int maxCollisions;
  CollisionCache<Integer, Integer> cache;

  @After
  public void after() {
    cache = null;
  }

  @Test
  public void testPutGetExisting() {
    final Integer max = Integer.MAX_VALUE;
    assertNull(cache.getIfPresent(max));
    assertSame(max, cache.putIfAbsent(max, max));
    assertSame(max, cache.putIfAbsent(max, max));
    assertSame(max, cache.getIfPresentAcquire(max));
    assertSame(max, cache.getIfPresent(max));
    assertSame(max, cache.get(max));
    assertSame(max, cache.getAggressive(max));
  }

  @Test
  public void testIfSpace() {
    final Integer key = 9;
    int val = 0;
    for (;val < maxCollisions;++val) {
      assertEquals(val, cache.putIfSpaceAbsent(key, val).intValue());
    }
    final Integer boxed = val;
    assertNull(cache.putIfSpaceAbsent(key, boxed));
    final Integer newBoxed = boxed - 1;
    assertSame(newBoxed, cache.replace(key, newBoxed));
    assertEquals(key, cache.putIfAbsent(key, key));
    assertSame(key, cache.putIfAbsent(key, 42));
    assertSame(key, cache.putIfSpaceAbsent(key, 27));
    assertEquals(27, cache.putIfSpaceReplace(key, 27).intValue());
    assertEquals(42, cache.putReplace(key, 42).intValue());
  }

  @Test
  public void testLoadAggressive() {
    for (int key = 0;key < NUM_KEYS_TO_TEST;key++) {
      assertNull(cache.getIfPresent(key));
      final Integer loaded = cache.getAggressive(key);
      final Integer expected = key;
      assertEquals(expected, loaded);
      assertEquals(expected, cache.getIfPresent(key));
      assertSame(loaded, cache.getIfPresent(key));
      assertSame(loaded, cache.get(key));
    }
  }

  @Test
  public void testLoad() {
    for (int key = 0;key < NUM_KEYS_TO_TEST;key++) {
      assertNull(cache.getIfPresentAcquire(key));
      final Integer loaded = cache.get(key);
      final Integer expected = key;
      assertEquals(expected, loaded);
      assertEquals(expected, cache.getIfPresentAcquire(key));
      assertSame(loaded, cache.getIfPresentAcquire(key));
      assertSame(loaded, cache.get(key));
    }
  }

  @Test
  public void testGetIfPresentAcquire() {
    for (int key = 0;key < NUM_KEYS_TO_TEST;key++) {
      final Integer expected = key;
      assertNull(cache.getIfPresentAcquire(key));
      assertSame(expected, cache.putIfAbsent(key, expected));
      assertSame(expected, cache.getIfPresentAcquire(key));
      cache.remove(key);
      assertNull(cache.getIfPresentAcquire(key));
    }
  }

  @Test
  public void testPutIfAbsent() {
    for (int key = 0;key < NUM_KEYS_TO_TEST;key++) {
      final Integer expected = key;
      assertSame(expected, cache.putIfAbsent(key, expected));
      assertSame(expected, cache.putIfAbsent(key, expected));
    }
  }

  @Test
  public void testNullBuckets() {
    for (int key = 0;key < NUM_KEYS_TO_TEST;key++) {
      final Integer expected = key;
      cache.putIfAbsent(key, expected);
      assertEquals(expected, cache.getIfPresent(key));
    }
    cache.nullBuckets();
    for (int key = 0;key < NUM_KEYS_TO_TEST;key++) {
      assertNull(cache.getIfPresent(key));
    }
  }

  @Test
  public void testClear() {
    for (int key = 0;key < NUM_KEYS_TO_TEST;key++) {
      final Integer expected = key;
      cache.putReplace(key, expected);
      assertEquals(expected, cache.getIfPresentAcquire(key));
    }
    cache.clear();
    for (int key = 0;key < NUM_KEYS_TO_TEST;key++) {
      assertNull(cache.getIfPresent(key));
    }
  }
}
