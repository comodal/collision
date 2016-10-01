package com.fabahaba.collission.cache;

import com.fabahaba.collision.cache.CollisionCache;

import org.junit.Test;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CollisionCacheTest {

  @Test
  public void testCache() {
    final int maxCollisions = 8;
    final CollisionCache<Integer, Integer> cache = CollisionCache
        .withCapacity(32, Integer.class)
        .setBucketSize(maxCollisions)
        .setStoreKeys(false)
        .<Integer, Integer>setLoader(num -> num, (key, num) -> num)
        .buildPacked();
    final Integer max = Integer.MAX_VALUE;
    assertNull(cache.getIfPresent(max));
    assertEquals(max, cache.putIfAbsent(max, max));
    assertEquals(max, cache.putIfAbsent(max, max));
    assertEquals(max, cache.getIfPresentVolatile(max));
    assertEquals(max, cache.getIfPresent(max));

    final Integer nine = 9;
    int val = 0;
    for (;val < maxCollisions;++val) {
      assertEquals(val, cache.putIfSpaceAbsent(nine, val).intValue());
    }
    final Integer boxed = val;
    assertNull(cache.putIfSpaceAbsent(nine, boxed));
    final Integer newBoxed = boxed - 1;
    assertTrue(newBoxed == cache.replace(nine, newBoxed));
    assertEquals(nine, cache.putIfAbsent(nine, nine));
    assertTrue(nine == cache.putIfAbsent(nine, 42));
    assertTrue(nine == cache.putIfSpaceAbsent(nine, 27));
    assertEquals(27, cache.putIfSpaceReplace(nine, 27).intValue());
    assertEquals(42, cache.putReplace(nine, 42).intValue());
  }

  @Test
  public void testSparseCache() {
    final int maxCollisions = 4;
    final CollisionCache<Integer, Integer> cache = CollisionCache
        .withCapacity(32, Integer.class)
        .setBucketSize(maxCollisions)
        .setStoreKeys(false)
        .<Integer, Integer>setLoader(num -> num, (key, num) -> num)
        .buildSparse();
    final Integer max = Integer.MAX_VALUE;
    assertNull(cache.getIfPresent(max));
    assertEquals(max, cache.putIfAbsent(max, max));
    assertEquals(max, cache.putIfAbsent(max, max));
    assertEquals(max, cache.getIfPresentVolatile(max));
    assertEquals(max, cache.getIfPresent(max));

    final Integer nine = 9;
    int val = 0;
    for (;val < maxCollisions;++val) {
      assertEquals(val, cache.putIfSpaceAbsent(nine, val).intValue());
    }
    final Integer boxed = val;
    assertNull(cache.putIfSpaceAbsent(nine, boxed));
    final Integer newBoxed = boxed - 1;
    assertTrue(newBoxed == cache.replace(nine, newBoxed));
    assertEquals(nine, cache.putIfAbsent(nine, nine));
    assertTrue(nine == cache.putIfAbsent(nine, 42));
    assertTrue(nine == cache.putIfSpaceAbsent(nine, 27));
    assertEquals(27, cache.putIfSpaceReplace(nine, 27).intValue());
    assertEquals(42, cache.putReplace(nine, 42).intValue());
  }

  @Test
  public void testPackedEntryCache() {
    final CollisionCache<Integer, String> cache = CollisionCache.<String>withCapacity(128)
        .<Integer, String>setLoader(
            key -> hashInteger(key),
            (key, hash) -> hash)
        .buildPacked();
    for (int i = 0;i < 10;i++) {
      final String expected = hashInteger(i);
      assertEquals(expected, cache.get(i));
      assertEquals(expected, cache.getIfPresent(i));
      assertEquals(expected, cache.getIfPresentVolatile(i));
      System.out.println(i + ": " + expected);
    }
  }

  @Test
  public void testSparseEntryCache() {
    final CollisionCache<Integer, String> cache = CollisionCache.<String>withCapacity(128)
        .<Integer, String>setLoader(
            key -> hashInteger(key),
            (key, hash) -> hash)
        .buildSparse();
    for (int i = 0;i < 10;i++) {
      final String expected = hashInteger(i);
      assertEquals(expected, cache.get(i));
      assertEquals(expected, cache.getIfPresent(i));
      assertEquals(expected, cache.getIfPresentVolatile(i));
      System.out.println(i + ": " + expected);
    }
  }

  static final ThreadLocal<MessageDigest> sha3MessageDigest512 = ThreadLocal
      .withInitial(() -> {
        try {
          return MessageDigest.getInstance("SHA3-512");
        } catch (final NoSuchAlgorithmException e) {
          throw new AssertionError(e);
        }
      });

  private static String hashInteger(final Integer integer) {
    final byte[] intStringBytes = integer.toString().getBytes(StandardCharsets.US_ASCII);
    final byte[] hashBytes = sha3MessageDigest512.get().digest(intStringBytes);
    return new BigInteger(1, hashBytes).toString(16);
  }
}
