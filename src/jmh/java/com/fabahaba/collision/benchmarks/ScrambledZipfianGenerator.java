package com.fabahaba.collision.benchmarks;

final class ScrambledZipfianGenerator {

  private static final double ZETAN = 26.46902820178302;
  private static final long ITEM_COUNT = 10000000000L;

  private final ZipfianGenerator gen;
  private final long itemCount;

  ScrambledZipfianGenerator(final long itemCount) {
    this(itemCount - 1, ZipfianGenerator.ZIPFIAN_CONSTANT);
  }

  private ScrambledZipfianGenerator(final long max, final double zipfianConstant) {
    this.itemCount = max + 1;
    this.gen = zipfianConstant == ZipfianGenerator.ZIPFIAN_CONSTANT
        ? new ZipfianGenerator(ITEM_COUNT, zipfianConstant, ZETAN)
        : new ZipfianGenerator(ITEM_COUNT, zipfianConstant);
  }

  long nextValue() {
    return Math.abs(fnvHash64(gen.nextValue())) % itemCount;
  }

  private static final long FNV_offset_basis_64 = 0xCBF29CE484222325L;
  private static final long FNV_prime_64 = 1099511628211L;

  private static long fnvHash64(final long val) {
    // http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
    long hash = FNV_offset_basis_64;
    for (int i = 0;i < 64;i += 8) {
      hash = hash ^ (val >> i) & 0xff;
      hash = hash * FNV_prime_64;
    }
    return hash;
  }
}
