package systems.comodal.collision.benchmarks;

final class ScrambledZipfGenerator {

  private static final double ZETAN = 26.46902820178302;
  private static final long ITEM_COUNT = 10_000_000_000L;
  private static final long FNV_offset_basis_64 = 0xCBF29CE484222325L;
  private static final long FNV_prime_64 = 1099511628211L;
  private final ZipfGenerator gen;
  private final long itemCount;

  ScrambledZipfGenerator(final long itemCount) {
    this.itemCount = itemCount;
    this.gen = new ZipfGenerator(ITEM_COUNT, ZipfGenerator.ZIPF_CONSTANT, ZETAN);
  }

  private static long fnvHash64(final long val) {
    // http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
    long hash = FNV_offset_basis_64;
    for (int i = 0; i < 64; i += 8) {
      hash = hash ^ (val >> i) & 0xff;
      hash = hash * FNV_prime_64;
    }
    return hash;
  }

  long nextValue() {
    return Math.abs(fnvHash64(gen.nextValue())) % itemCount;
  }
}
