package systems.comodal.collision.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.junit.After;
import org.junit.Test;

abstract class BaseEntryCacheTest {

  private static final String DIGEST_ALGO = "SHA-256";
  private static final ThreadLocal<MessageDigest> MESSAGE_DIGEST = ThreadLocal
      .withInitial(() -> {
        try {
          return MessageDigest.getInstance(DIGEST_ALGO);
        } catch (final NoSuchAlgorithmException ex) {
          throw new AssertionError(ex);
        }
      });
  private static String[] expectedHashes = new String[32];

  static {
    for (int i = 0; i < expectedHashes.length; i++) {
      expectedHashes[i] = toHexString(hashInteger(i));
    }
  }

  LoadingCollisionCache<Integer, byte[], String> cache;

  static byte[] hashInteger(final Integer integer) {
    final byte[] intStringBytes = integer.toString().getBytes(StandardCharsets.US_ASCII);
    return MESSAGE_DIGEST.get().digest(intStringBytes);
  }

  static String toHexString(final byte[] hashBytes) {
    return new BigInteger(1, hashBytes).toString(16);
  }

  @After
  public void after() {
    cache = null;
  }

  @Test
  public void testPutGetExisting() {
    for (int key = 0; key < expectedHashes.length; key++) {
      final String expected = expectedHashes[key];
      assertNull(cache.getIfPresent(key));
      assertSame(expected, cache.putIfAbsent(key, expected));
      assertSame(expected, cache.putIfAbsent(key, expected));
      assertSame(expected, cache.getIfPresent(key));
      assertSame(expected, cache.getIfPresent(key));
      assertSame(expected, cache.get(key));
      assertSame(expected, cache.getAggressive(key));
    }
  }

  @Test
  public void testLoadAggressive() {
    for (int key = 0; key < expectedHashes.length; key++) {
      final String expected = expectedHashes[key];
      assertNull(cache.getIfPresent(key));
      final String loaded = cache.getAggressive(key);
      assertEquals(expected, loaded);
      assertEquals(expected, cache.getIfPresent(key));
      assertSame(loaded, cache.getIfPresent(key));
      assertSame(loaded, cache.get(key));
    }
  }

  @Test
  public void testLoad() {
    for (int key = 0; key < expectedHashes.length; key++) {
      final String expected = expectedHashes[key];
      assertNull(cache.getIfPresent(key));
      final String loaded = cache.get(key);
      assertEquals(expected, loaded);
      assertEquals(expected, cache.getIfPresent(key));
      assertSame(loaded, cache.getIfPresent(key));
      assertSame(loaded, cache.get(key));
    }
  }

  @Test
  public void testgetIfPresent() {
    for (int key = 0; key < expectedHashes.length; key++) {
      final String expected = expectedHashes[key];
      assertNull(cache.getIfPresent(key));
      assertSame(expected, cache.putIfAbsent(key, expected));
      assertSame(expected, cache.getIfPresent(key));
      cache.remove(key);
      assertNull(cache.getIfPresent(key));
    }
  }

  @Test
  public void testPutIfAbsent() {
    for (int key = 0; key < expectedHashes.length; key++) {
      final String expected = expectedHashes[key];
      assertSame(expected, cache.putIfAbsent(key, expected));
      assertSame(expected, cache.putIfAbsent(key, expected));
    }
  }

  @Test
  public void testReplace() {
    for (int key = 0; key < expectedHashes.length; key++) {
      final String expected = expectedHashes[key];
      final String newHash = toHexString(hashInteger(key));
      assertNotSame(expected, newHash);
      assertEquals(expected, newHash);

      assertSame(expected, cache.putIfAbsent(key, expected));
      assertSame(expected, cache.get(key));
      assertSame(newHash, cache.putReplace(key, newHash));
      assertSame(newHash, cache.get(key));
      assertSame(expected, cache.putReplace(key, expected));
      assertSame(expected, cache.getAggressive(key));
      assertSame(newHash, cache.replace(key, newHash));
      assertSame(newHash, cache.getIfPresent(key));
      assertSame(expected, cache.replace(key, expected));
      assertSame(expected, cache.getIfPresent(key));

      assertTrue(cache.remove(key));
      assertSame(newHash, cache.putReplace(key, newHash));
      assertSame(newHash, cache.get(key));
      assertTrue(cache.remove(key));
      assertSame(expected, cache.putReplace(key, expected));
      assertSame(expected, cache.getAggressive(key));
      assertTrue(cache.remove(key));
      assertNull(cache.replace(key, newHash));
      assertNull(cache.getIfPresent(key));
      assertNull(cache.replace(key, expected));
      assertNull(cache.getIfPresent(key));
    }
  }

  @Test
  public void testClear() {
    for (int key = 0; key < expectedHashes.length; key++) {
      final String expected = expectedHashes[key];
      cache.putReplace(key, expected);
      assertEquals(expected, cache.getIfPresent(key));
    }
    cache.clear();
    for (int key = 0; key < expectedHashes.length; key++) {
      assertNull(cache.getIfPresent(key));
    }
  }
}
