package systems.comodal.collision.cache;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Provides atomic operations for 8-bit logarithmic counters backed by a byte array.
 *
 * @author James P. Edwards
 */
public final class AtomicLogCounters {

  static final int MAX_COUNT = 0xff;

  private static final VarHandle COUNTERS = MethodHandles.arrayElementVarHandle(byte[].class);

  private final byte[] counters;
  private final byte initialCount;
  private final double[] thresholds;

  private AtomicLogCounters(final byte[] counters, final int initialCount,
      final double[] thresholds) {
    this.counters = counters;
    this.initialCount = (byte) initialCount;
    this.thresholds = thresholds;
  }

  public static AtomicLogCounters create(final int numCounters, final int initialCount,
      final int maxCounterVal) {
    final int pow2LogFactor = calcLogFactorShift(maxCounterVal);
    final byte[] counters = new byte[numCounters];
    final double[] thresholds = new double[MAX_COUNT];
    thresholds[0] = 1.0;
    for (int i = 1; i < MAX_COUNT; i++) {
      thresholds[i] = 1.0 / ((long) i << pow2LogFactor);
    }
    return new AtomicLogCounters(counters, initialCount, thresholds);
  }

  /**
   * Used in conjunction with {@link #increment increment} as a multiplication
   * factor to decrease the probability of a counter increment as the counter increases.
   *
   * @param maxCount The relative max count.  Once a counter is incremented this many times its
   * value should be 255.
   * @return The power of two multiplication factor as the number of bits to shift.
   */
  private static int calcLogFactorShift(final int maxCount) {
    // Divide next highest power of 2 by 32,768... (256^2 / 2).
    // Then get the number of bits to shift for efficiency in future calculations.
    // The result of this factor will cause the count to be 255 after maxCount increments.
    return Integer.numberOfTrailingZeros(Integer.highestOneBit(maxCount - 1) >> 14);
  }

  public int getNumCounters() {
    return counters.length;
  }

  public void initializeOpaque(final int index) {
    COUNTERS.setOpaque(counters, index, initialCount);
  }

  public void setOpaque(final int index, final int initialCount) {
    COUNTERS.setOpaque(counters, index, (byte) initialCount);
  }


  public int getOpaque(final int index) {
    return ((int) COUNTERS.getOpaque(counters, index)) & MAX_COUNT;
  }

  /**
   * Probabilistically increments a relatively large counter, represented from
   * {@code initialCount} to 255.  The probability of an increment decreases at a rate of
   * {@code (1 / (counters[index] * maxRelativeCount / (256^2 / 2)))}.
   *
   * @param index counter array index to increment.
   */
  public void increment(final int index) {
    int witness = (int) COUNTERS.getOpaque(counters, index);
    int count = witness & MAX_COUNT;
    if (count == MAX_COUNT) {
      return;
    }
    int expected;
    while (count <= initialCount) {
      expected = witness;
      witness = (int) COUNTERS
          .compareAndExchange(counters, index, (byte) expected, (byte) (count + 1));
      if (expected == witness || (count = witness & MAX_COUNT) == MAX_COUNT) {
        return;
      }
    }
    if (thresholds[count] < ThreadLocalRandom.current().nextFloat()) {
      return;
    }
    for (; ; ) {
      expected = witness;
      witness = (int) COUNTERS
          .compareAndExchange(counters, index, (byte) expected, (byte) (count + 1));
      if (expected == witness || (count = witness & MAX_COUNT) == MAX_COUNT) {
        return;
      }
    }
  }

  /**
   * Divides all values by two within the ranges [from skip) and (skip, to).
   *
   * @param from inclusive counter index to start at.
   * @param to exclusive max index for the counters to decay.
   * @param skip Skips decay for this index because it corresponds to a new entry.
   */
  void decay(final int from, final int to, final int skip) {
    decay(from, skip);
    decay(skip + 1, to);
  }

  void decay(final int from, final int to) {
    for (int counterIndex = from; counterIndex < to; ++counterIndex) {
      final int count = ((int) COUNTERS.getOpaque(counters, counterIndex)) & MAX_COUNT;
      if (count == 0) {
        continue;
      }
      // Counter misses may occur between these two calls.
      COUNTERS.setOpaque(counters, counterIndex, (byte) (count >> 1));
    }
  }

  @Override
  public String toString() {
    return "AtomicLogCounters{numCounters=" + counters.length
        + ", initialCount=" + initialCount + '}';
  }
}
