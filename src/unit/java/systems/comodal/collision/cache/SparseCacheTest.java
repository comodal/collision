package systems.comodal.collision.cache;

import org.junit.Before;

public final class SparseCacheTest extends BaseCacheTest {

  @Before
  public void before() {
    this.maxCollisions = 4;
    this.cache = CollisionCache
        .withCapacity(32, TestNumber.class)
        .setBucketSize(maxCollisions)
        .setStoreKeys(false)
        .<TestNumber, TestNumber>setLoader(num -> num, (key, num) -> num)
        .buildSparse();
  }
}
