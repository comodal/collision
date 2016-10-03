package com.fabahaba.collision.benchmarks;

final class ZipfianGenerator {

  static final double ZIPFIAN_CONSTANT = 0.99;

  private final long items;
  private final double alpha;
  private final double zetan;
  private final double eta;
  private final double theta;

  ZipfianGenerator(final long max, final double zipfianConstant) {
    this(max, zipfianConstant, zetastatic(max + 1, zipfianConstant));
  }

  ZipfianGenerator(final long max, final double zipfianConstant,
      final double zetan) {
    this.items = max + 1;
    this.theta = zipfianConstant;
    final double zetaToTheta = zetastatic(2, theta);
    this.alpha = 1.0 / (1.0 - theta);
    this.zetan = zetan;
    this.eta = (1 - Math.pow(2.0 / items, 1 - theta)) / (1 - zetaToTheta / zetan);
    nextValue();
  }

  private static double zetastatic(final long max, final double theta) {
    double sum = 0.0;
    for (long i = 0;i < max;) {
      sum += 1 / Math.pow(++i, theta);
    }
    return sum;
  }

  long nextValue() {
    final double u = Math.random();
    final double uz = u * zetan;
    if (uz < 1.0) {
      return 0;
    }
    if (uz < 1.0 + Math.pow(0.5, theta)) {
      return 1;
    }
    return (long) ((items) * Math.pow(eta * u - eta + 1, alpha));
  }
}
