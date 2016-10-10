package com.fabahaba.collision.cache;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.ThreadLocalRandom;

abstract class LogCounterCache {

  static final VarHandle BA = MethodHandles.arrayElementVarHandle(byte[].class);
  static final VarHandle OA = MethodHandles.arrayElementVarHandle(Object[].class);
  private static final byte MAX_COUNT = (byte) 0xff;

  final byte[] counters;
  final byte initCount;
  private final int pow2LogFactor;

  LogCounterCache(final byte[] counters, final int initCount, final int pow2LogFactor) {
    this.counters = counters;
    this.initCount = (byte) initCount;
    this.pow2LogFactor = pow2LogFactor;
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
    if (witness == MAX_COUNT) {
      return;
    }
    int count = ((int) witness) & 0xff;
    byte expected;
    while (count <= initCount) {
      expected = witness;
      witness = (byte) BA.compareAndExchangeRelease(counters, index, expected, (byte) (count + 1));
      if (expected == witness || witness == MAX_COUNT) {
        return;
      }
      count = ((int) witness) & 0xff;
    }
    final int prob = (int) (1.0 / ThreadLocalRandom.current().nextDouble()) >>> pow2LogFactor;
    while (prob >= count) {
      expected = witness;
      witness = (byte) BA.compareAndExchangeRelease(counters, index, expected, (byte) (count + 1));
      if (expected == witness || witness == MAX_COUNT) {
        return;
      }
      count = ((int) witness) & 0xff;
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
   * @param skip Skips decay for this entry because it is a brand new.
   */
  final void decay(final int from, final int to, final int skip) {
    int counterIndex = from;
    do {
      if (counterIndex == skip) {
        continue;
      }
      final int count = ((int) BA.getAcquire(counters, counterIndex)) & 0xff;
      if (count == 0) {
        continue;
      }
      // Counter misses may occur between these two calls.
      BA.setRelease(counters, counterIndex, (byte) (count >> 1));
    } while (++counterIndex < to);
  }
}
