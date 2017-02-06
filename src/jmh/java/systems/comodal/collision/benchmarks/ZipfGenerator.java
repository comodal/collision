package systems.comodal.collision.benchmarks;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;

final class ZipfGenerator {

  static final double ZIPF_CONSTANT = 0.99;

  private final long items;
  private final double alpha;
  private final double zetan;
  private final double eta;
  private final double theta;

  ZipfGenerator(final long max, final double zipfConstant) {
    this(max, zipfConstant, zeta(max + 1, zipfConstant));
  }

  ZipfGenerator(final long max, final double zipfConstant, final double zetan) {
    this.items = max + 1;
    this.theta = zipfConstant;
    final double zetaToTheta = zeta(2, theta);
    this.alpha = 1.0 / (1.0 - theta);
    this.zetan = zetan;
    this.eta = (1 - Math.pow(2.0 / items, 1 - theta)) / (1 - zetaToTheta / zetan);
    nextValue();
  }

  private static double zeta(final long max, final double theta) {
    return LongStream.range(1, max + 1).parallel().mapToDouble(i -> 1 / Math.pow(i, theta)).sum();
  }

  long nextValue() {
    final double u = ThreadLocalRandom.current().nextDouble();
    final double uz = u * zetan;
    if (uz < 1.0) {
      return 0;
    }
    if (uz < 1.0 + Math.pow(0.5, theta)) {
      return 1;
    }
    return (long) (items * Math.pow(eta * u - eta + 1, alpha));
  }
}
