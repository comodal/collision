package com.fabahaba.collision.cache;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.ThreadLocalRandom;

abstract class LogCounterCache {

  static final VarHandle BA = MethodHandles.arrayElementVarHandle(byte[].class);
  static final VarHandle OA = MethodHandles.arrayElementVarHandle(Object[].class);

  final byte[] counters;
  final byte initCount;
  private final int pow2LogFactor;
  private final int initCountOffset;

  LogCounterCache(final byte[] counters, final int initCount, final int pow2LogFactor) {
    this.counters = counters;
    this.initCount = (byte) initCount;
    this.pow2LogFactor = pow2LogFactor;
    this.initCountOffset = initCount << pow2LogFactor;
  }

  /**
   * Probabilistically increments a relatively large counter, represented from
   * {@code initCount} to 255.  The probability of an increment decreases at a rate of
   * {@code (1 / (counters[index] * maxRelativeCount / (256^2 / 2)))}.
   *
   * @param index Array index of the counter to increment.
   */
  final void atomicIncrement(final int index) {
    byte witness = (byte) BA.getAcquire(counters, index);
    int count = ((int) witness) & 0xff;
    if (count == 0xff) {
      return;
    }
    byte expected;
    while (count <= initCount) {
      expected = witness;
      witness = (byte) BA.compareAndExchangeRelease(counters, index, expected, (byte) (count + 1));
      if (expected == witness) {
        return;
      }
      count = ((int) witness) & 0xff;
      if (count == 0xff) {
        return;
      }
    }
    final double prob = initCountOffset + (1.0 / ThreadLocalRandom.current().nextDouble());
    while (prob >= count << pow2LogFactor) {
      expected = witness;
      witness = (byte) BA.compareAndExchangeRelease(counters, index, expected, (byte) (count + 1));
      if (expected == witness) {
        return;
      }
      count = ((int) witness) & 0xff;
      if (count == 0xff) {
        return;
      }
    }
  }

  //  public static void main(String[] args) {
  //    final int initCount = 5;
  //    final int pow2LogFactor = LogCounterCache.calcLogFactorShift(1_048_576);
  //    final int initCountOffset = initCount << pow2LogFactor;
  //    for (int count = 6;count < 255;) {
  //      final double prob = 1.0 / ThreadLocalRandom.current().nextDouble();
  //      boolean result = prob < (count - initCount) << pow2LogFactor;
  //      final double testProb = prob + initCountOffset;
  //      boolean result2 = testProb < count << pow2LogFactor;
  //      if (result != result2) {
  //        System.err.println(result + " : " + result2);
  //      }
  //      if (!result) {
  //        ++count;
  //        //System.out.println(result);
  //      }
  //    }
  //  }

  /**
   * Used in conjunction with {@link #atomicIncrement atomicIncrement} as a multiplication
   * factor to decrease the probability of a counter increment as the counter increases.
   *
   * @param maxCount The relative max count.  Once a counter is incremented this many times its val
   *                 should be 255.
   * @return The power of two multiplication factor as the number of bits to shift.
   */
  static int calcLogFactorShift(final int maxCount) {
    // Divide next highest power of 2 by 32,768... (256^2 / 2).
    // Then getAggressive the number of bits to shift for efficiency.
    // The result of this factor will cause the count to be 255 after maxCount increments.
    return Integer.numberOfTrailingZeros(Integer.highestOneBit(maxCount - 1) >> 14);
  }

  /**
   * Divides all values by two within the ranges [from skip) and (skip, to).
   *
   * @param from inclusive counter index to start at.
   * @param to   exclusive max index for the counters to decay.
   * @param skip Skips decay for this entry because it is a brand new.
   */
  final void decay(final int from, final int to, final int skip) {
    int counterIndex = from;
    do {
      if (counterIndex == skip) {
        continue;
      }
      int count = ((int) BA.getAcquire(counters, counterIndex)) & 0xff;
      if (count == 0) {
        continue;
      }
      // Counter misses may occur between these two calls.
      BA.setRelease(counters, counterIndex, (byte) (count >> 1));
    } while (++counterIndex < to);
  }
}
