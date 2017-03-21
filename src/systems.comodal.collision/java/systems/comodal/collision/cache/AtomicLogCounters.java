package systems.comodal.collision.cache;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Provides atomic operations for 8-bit logarithmic counters backed by a byte array.
 *
 * @author James P. Edwards
 */
class AtomicLogCounters {

  static final VarHandle COUNTERS = MethodHandles.arrayElementVarHandle(byte[].class);

  static final int MAX_COUNT = 0xff;

  final byte[] counters;
  final byte initCount;
  private final double[] thresholds;

  AtomicLogCounters(final int numCounters, final int initCount, final int maxCounterVal) {
    this(new byte[numCounters], initCount, calcLogFactorShift(maxCounterVal));
  }

  AtomicLogCounters(final byte[] counters, final int initCount, final int pow2LogFactor) {
    this.counters = counters;
    this.initCount = (byte) initCount;
    this.thresholds = new double[MAX_COUNT];
    thresholds[0] = 1.0;
    for (int i = 1; i < MAX_COUNT; i++) {
      thresholds[i] = 1.0 / ((long) i << pow2LogFactor);
    }
  }

  /**
   * Used in conjunction with {@link #atomicIncrement atomicIncrement} as a multiplication
   * factor to decrease the probability of a counter increment as the counter increases.
   *
   * @param maxCount The relative max count.  Once a counter is incremented this many times its
   * value should be 255.
   * @return The power of two multiplication factor as the number of bits to shift.
   */
  static int calcLogFactorShift(final int maxCount) {
    // Divide next highest power of 2 by 32,768... (256^2 / 2).
    // Then get the number of bits to shift for efficiency in future calculations.
    // The result of this factor will cause the count to be 255 after maxCount increments.
    return Integer.numberOfTrailingZeros(Integer.highestOneBit(maxCount - 1) >> 14);
  }

  final void initCount(final int index) {
    COUNTERS.setOpaque(counters, index, initCount);
  }

  final void initCount(final int index, final int initCount) {
    COUNTERS.setOpaque(counters, index, (byte) initCount);
  }


  final int getOpaqueCount(final int index) {
    return ((int) COUNTERS.getOpaque(counters, index)) & MAX_COUNT;
  }

  /**
   * Probabilistically increments a relatively large counter, represented from
   * {@code initCount} to 255.  The probability of an increment decreases at a rate of
   * {@code (1 / (counters[index] * maxRelativeCount / (256^2 / 2)))}.
   *
   * @param index counter array index to increment.
   */
  final void atomicIncrement(final int index) {
    int witness = (int) COUNTERS.getOpaque(counters, index);
    int count = witness & MAX_COUNT;
    if (count == MAX_COUNT) {
      return;
    }
    int expected;
    while (count <= initCount) {
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
  final void decay(final int from, final int to, final int skip) {
    decay(from, skip);
    decay(skip + 1, to);
  }

  final void decay(final int from, final int to) {
    for (int counterIndex = from; counterIndex < to; ++counterIndex) {
      final int count = ((int) COUNTERS.getOpaque(counters, counterIndex)) & MAX_COUNT;
      if (count == 0) {
        continue;
      }
      // Counter misses may occur between these two calls.
      COUNTERS.setOpaque(counters, counterIndex, (byte) (count >> 1));
    }
  }
}
