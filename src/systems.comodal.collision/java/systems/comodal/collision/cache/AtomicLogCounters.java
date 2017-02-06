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

  static final VarHandle BA = MethodHandles.arrayElementVarHandle(byte[].class);
  static final VarHandle OA = MethodHandles.arrayElementVarHandle(Object[].class);

  static final int MAX_COUNT = 0xff;

  final byte[] counters;
  final byte initCount;
  private final int pow2LogFactor;

  AtomicLogCounters(final int numCounters, final int initCount, final int maxCounterVal) {
    this(new byte[numCounters], initCount, calcLogFactorShift(maxCounterVal));
  }

  AtomicLogCounters(final byte[] counters, final int initCount, final int pow2LogFactor) {
    this.counters = counters;
    this.initCount = (byte) initCount;
    this.pow2LogFactor = pow2LogFactor;
  }

  final void initCount(final int index) {
    BA.setRelease(counters, index, initCount);
  }

  final void initCount(final int index, final int initCount) {
    BA.setRelease(counters, index, (byte) initCount);
  }

  final int getAcquireCount(final int index) {
    return ((int) BA.getAcquire(counters, index)) & MAX_COUNT;
  }

  /**
   * Probabilistically increments a relatively large counter, represented from
   * {@code initCount} to 255.  The probability of an increment decreases at a rate of
   * {@code (1 / (counters[index] * maxRelativeCount / (256^2 / 2)))}.
   *
   * @param index counter array index to increment.
   */
  final void atomicIncrement(final int index) {
    byte witness = (byte) BA.getAcquire(counters, index);
    int count = ((int) witness) & MAX_COUNT;
    if (count == MAX_COUNT) {
      return;
    }
    byte expected;
    while (count <= initCount) {
      expected = witness;
      witness = (byte) BA.compareAndExchangeRelease(counters, index, expected, (byte) (count + 1));
      if (expected == witness) {
        return;
      }
      count = ((int) witness) & MAX_COUNT;
      if (count == MAX_COUNT) {
        return;
      }
    }
    final int prob = ((int) (1.0 / ThreadLocalRandom.current().nextDouble())) >>> pow2LogFactor;
    while (prob >= count) {
      expected = witness;
      witness = (byte) BA.compareAndExchangeRelease(counters, index, expected, (byte) (count + 1));
      if (expected == witness) {
        return;
      }
      count = ((int) witness) & MAX_COUNT;
      if (count == MAX_COUNT) {
        return;
      }
    }
  }

  /**
   * Used in conjunction with {@link #atomicIncrement atomicIncrement} as a multiplication
   * factor to decrease the probability of a counter increment as the counter increases.
   *
   * @param maxCount The relative max count.  Once a counter is incremented this many times its
   *                 value should be 255.
   * @return The power of two multiplication factor as the number of bits to shift.
   */
  static int calcLogFactorShift(final int maxCount) {
    // Divide next highest power of 2 by 32,768... (256^2 / 2).
    // Then get the number of bits to shift for efficiency in future calculations.
    // The result of this factor will cause the count to be 255 after maxCount increments.
    return Integer.numberOfTrailingZeros(Integer.highestOneBit(maxCount - 1) >> 14);
  }

  /**
   * Divides all values by two within the ranges [from skip) and (skip, to).
   *
   * @param from inclusive counter index to start at.
   * @param to   exclusive max index for the counters to decay.
   * @param skip Skips decay for this index because it corresponds to a new entry.
   */
  final void decay(final int from, final int to, final int skip) {
    int counterIndex = from;
    do {
      if (counterIndex == skip) {
        continue;
      }
      final int count = ((int) BA.getAcquire(counters, counterIndex)) & MAX_COUNT;
      if (count == 0) {
        continue;
      }
      // Counter misses may occur between these two calls.
      BA.setRelease(counters, counterIndex, (byte) (count >> 1));
    } while (++counterIndex < to);
  }
}
