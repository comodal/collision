package com.fabahaba.collision.cache;

import org.junit.After;
import org.junit.Test;

import static com.fabahaba.collision.cache.BaseCacheTest.TestNumber.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

abstract class BaseCacheTest {

  private static final int NUM_KEYS_TO_TEST = 10;

  int maxCollisions;
  CollisionCache<TestNumber, TestNumber> cache;

  @After
  public void after() {
    cache = null;
  }

  @Test
  public void testPutGetExisting() {
    final TestNumber key = of(Integer.MAX_VALUE);
    assertNull(cache.getIfPresent(key));
    assertSame(key, cache.putIfAbsent(key, key));
    assertSame(key, cache.putIfAbsent(key, key));
    assertSame(key, cache.getIfPresentAcquire(key));
    assertSame(key, cache.getIfPresent(key));
    assertSame(key, cache.get(key));
    assertSame(key, cache.getAggressive(key));
  }

  @Test
  public void testIfSpace() {
    final TestNumber key = of(9);
    int val = 0;
    for (;val < maxCollisions;++val) {
      final TestNumber boxed = of(val);
      assertEquals(val, cache.putIfSpaceAbsent(key, boxed).val);
    }
    assertNull(cache.putIfSpaceAbsent(key, key));
    assertNull(cache.putIfSpaceReplace(key, key));
    assertSame(key, cache.putIfAbsent(key, key));
    assertEquals(key, cache.putIfSpaceReplace(key, of(key.val)));
    final TestNumber newKey = of(val);
    assertNotSame(key, newKey);
    assertSame(newKey, cache.replace(key, newKey));
    assertSame(key, cache.putIfAbsent(key, key));
    assertSame(key, cache.putIfAbsent(key, of(42)));
    assertSame(key, cache.putIfSpaceAbsent(key, of(27)));
    assertEquals(of(27), cache.putIfSpaceReplace(key, of(27)));
    assertEquals(of(42), cache.putReplace(key, of(42)));
  }

  @Test
  public void testReplace() {
    for (int key = 0;key < NUM_KEYS_TO_TEST;key++) {
      final TestNumber boxedKey = of(key);
      final TestNumber newBoxedKey = of(key);
      assertNotSame(boxedKey, newBoxedKey);
      assertEquals(boxedKey, newBoxedKey);

      assertSame(boxedKey, cache.putIfAbsent(boxedKey, boxedKey));
      assertSame(boxedKey, cache.get(boxedKey));
      assertSame(newBoxedKey, cache.putReplace(boxedKey, newBoxedKey));
      assertSame(newBoxedKey, cache.get(boxedKey));
      assertSame(newBoxedKey, cache.putReplace(boxedKey, newBoxedKey));
      assertSame(newBoxedKey, cache.getAggressive(boxedKey));
      assertSame(boxedKey, cache.replace(boxedKey, boxedKey));
      assertSame(boxedKey, cache.getIfPresent(boxedKey));
      assertSame(newBoxedKey, cache.replace(boxedKey, newBoxedKey));
      assertSame(newBoxedKey, cache.getIfPresentAcquire(boxedKey));

      assertTrue(cache.remove(boxedKey));
      assertSame(newBoxedKey, cache.putReplace(boxedKey, newBoxedKey));
      assertSame(newBoxedKey, cache.get(boxedKey));
      assertTrue(cache.remove(boxedKey));
      assertSame(boxedKey, cache.putReplace(boxedKey, boxedKey));
      assertSame(boxedKey, cache.getAggressive(boxedKey));
      assertTrue(cache.remove(boxedKey));
      assertNull(cache.replace(boxedKey, newBoxedKey));
      assertNull(cache.getIfPresent(boxedKey));
      assertNull(cache.replace(boxedKey, boxedKey));
      assertNull(cache.getIfPresentAcquire(boxedKey));
    }
  }

  @Test
  public void testLoadAggressive() {
    for (int key = 0;key < NUM_KEYS_TO_TEST;key++) {
      final TestNumber boxedKey = of(key);
      assertNull(cache.getIfPresent(boxedKey));
      final TestNumber loaded = cache.getAggressive(of(key));
      assertEquals(boxedKey, loaded);
      assertNotSame(boxedKey, loaded);
      assertEquals(boxedKey, cache.getIfPresent(boxedKey));
      assertSame(loaded, cache.getIfPresent(boxedKey));
      assertSame(loaded, cache.get(boxedKey));
    }
  }

  @Test
  public void testLoad() {
    for (int key = 0;key < NUM_KEYS_TO_TEST;key++) {
      final TestNumber boxedKey = of(key);
      assertNull(cache.getIfPresentAcquire(boxedKey));
      final TestNumber loaded = cache.get(of(key));
      assertEquals(boxedKey, loaded);
      assertNotSame(boxedKey, loaded);
      assertEquals(boxedKey, cache.getIfPresentAcquire(boxedKey));
      assertSame(loaded, cache.getIfPresentAcquire(boxedKey));
      assertSame(loaded, cache.get(boxedKey));
    }
  }

  @Test
  public void testGetIfPresentAcquire() {
    for (int key = 0;key < NUM_KEYS_TO_TEST;key++) {
      final TestNumber boxedKey = of(key);
      final TestNumber expected = of(key);
      assertNull(cache.getIfPresentAcquire(boxedKey));
      assertSame(expected, cache.putIfAbsent(boxedKey, expected));
      assertSame(expected, cache.getIfPresentAcquire(boxedKey));
      cache.remove(boxedKey);
      assertNull(cache.getIfPresentAcquire(boxedKey));
    }
  }

  @Test
  public void testPutIfAbsent() {
    for (int key = 0;key < NUM_KEYS_TO_TEST;key++) {
      final TestNumber boxedKey = of(key);
      final TestNumber expected = of(key);
      assertSame(expected, cache.putIfAbsent(boxedKey, expected));
      assertSame(expected, cache.putIfAbsent(boxedKey, expected));
    }
  }

  @Test
  public void testNullBuckets() {
    for (int key = 0;key < NUM_KEYS_TO_TEST;key++) {
      final TestNumber boxedKey = of(key);
      final TestNumber expected = of(key);
      cache.putIfAbsent(boxedKey, expected);
      assertSame(expected, cache.getIfPresent(boxedKey));
    }
    cache.nullBuckets();
    for (int key = 0;key < NUM_KEYS_TO_TEST;key++) {
      final TestNumber boxedKey = of(key);
      assertNull(cache.getIfPresent(boxedKey));
    }
  }

  @Test
  public void testClear() {
    for (int key = 0;key < NUM_KEYS_TO_TEST;key++) {
      final TestNumber boxedKey = of(key);
      final TestNumber expected = of(key);
      cache.putReplace(boxedKey, expected);
      assertSame(expected, cache.getIfPresentAcquire(boxedKey));
    }
    cache.clear();
    for (int key = 0;key < NUM_KEYS_TO_TEST;key++) {
      final TestNumber boxedKey = of(key);
      assertNull(cache.getIfPresent(boxedKey));
    }
  }

  static final class TestNumber {

    final long val;

    static TestNumber of(final long val) {
      return new TestNumber(val);
    }

    TestNumber(final long val) {
      this.val = val;
    }

    @Override
    public boolean equals(final Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }
      final TestNumber that = (TestNumber) other;
      return val == that.val;
    }

    @Override
    public int hashCode() {
      return (int) val;
    }
  }
}
